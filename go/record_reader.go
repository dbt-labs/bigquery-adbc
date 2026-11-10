// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bigquery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
)

// MetadataKeyBigqueryQueryID is the Arrow schema metadata key under which the
// driver publishes the BigQuery job ID that produced the result set.
const MetadataKeyBigqueryQueryID = "BIGQUERY:query_id"

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.RecordBatch
	curChIndex int
	rec        arrow.RecordBatch
	errMu      sync.Mutex
	err        error

	cancelFn context.CancelFunc
}

func checkContext(ctx context.Context, maybeErr error) error {
	if maybeErr != nil {
		return maybeErr
	} else if errors.Is(ctx.Err(), context.Canceled) {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusCancelled}
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusTimeout}
	}
	return ctx.Err()
}

func runQuery(ctx context.Context, logger *slog.Logger, client *bigquery.Client, query *bigquery.Query, executeUpdate bool, linkFailedJob bool, alloc memory.Allocator) (bigquery.ArrowIterator, *bigquery.JobStatus, int64, error) {
	// A Query can be reused for bound parameter rows. Give every execution its
	// own ID, then reuse that ID only to recover a duplicate insert response.
	query.JobID = "adbc-" + uuid.NewString()

	job, err := query.Run(ctx)
	var apiErr *googleapi.Error
	// 409 error code returns when trying to create a job, dataset, or table that already exists.
	// https://docs.cloud.google.com/bigquery/docs/error-messages
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusConflict {
		// TODO: Making an additional call to retrieve a Job that the API assures already exists is suboptimal.
		// the better fix might go to the https://github.com/googleapis/google-cloud-go/blob/b9c76ec52f46f4fb1f1252b4ca159ca3cd2f86fa/bigquery/query.go#L368
		// this method just invokes insertJob but it should still return the Job when running into the 409 error
		job, err = client.JobFromIDLocation(ctx, query.JobID, query.Location)
	}
	if err != nil {
		return nil, nil, -1, errToAdbcErr(adbc.StatusInternal, err, "run query")
	}

	// The project id, location, and job id are all URL-safe:
	// - Project id and job id can only contain URL-safe characters:
	//   https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
	// - Locations are also URL-safe, listed here:
	//   https://cloud.google.com/bigquery/docs/locations
	jobLink := fmt.Sprintf(
		"https://console.cloud.google.com/bigquery?project=%s&j=bq:%s:%s&page=queryresults",
		job.ProjectID(), job.Location(), job.ID(),
	)
	wrap := func(err error) error {
		if err == nil || !linkFailedJob {
			return err
		}
		var adbcErr adbc.Error
		if errors.As(err, &adbcErr) {
			adbcErr.Msg = fmt.Sprintf("%s (Query: %s)", adbcErr.Msg, jobLink)
			return adbcErr
		}
		return fmt.Errorf("%w (Query: %s)", err, jobLink)
	}

	// XXX: Google SDK badness.  We can't use Wait here because queries that
	// *fail* with a rateLimitExceeded (e.g. too many metadata operations)
	// will get the *polling* retried infinitely in Google's SDK (I believe
	// the SDK wants to retry "polling for job status" rate limit exceeded but
	// doesn't differentiate between them because googleapi.CheckResponse
	// appears to put the API error from the response object as an error of
	// the API call, from digging around using a debugger.  In other words, it
	// seems to be confusing "I got an error that my API request was rate
	// limited" and "I got an error that my job was rate limited" because
	// their internal APIs mix both errors into a single error path.)
	js, err := safeWaitForJob(ctx, logger, job)
	if err != nil {
		return nil, nil, -1, wrap(err)
	}

	if err := js.Err(); err != nil {
		return nil, js, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "complete job"))
	} else if !js.Done() {
		return nil, js, -1, wrap(adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[bq] Query job did not complete",
		})
	}

	mayReturnResults := false
	stats, statsOk := js.Statistics.Details.(*bigquery.QueryStatistics)
	if executeUpdate {
		if statsOk {
			return nil, js, stats.NumDMLAffectedRows, nil
		}
		return nil, js, -1, nil
	} else if query.DryRun {
		return nil, js, js.Statistics.TotalBytesProcessed, nil
	} else if statsOk {
		// note that SCRIPT doesn't always have results. we catch this below
		mayReturnResults = stats.StatementType == "SELECT" || stats.StatementType == "CALL" || stats.StatementType == "SCRIPT"
	}

	// XXX: the Google SDK badness also applies here; it makes a similar
	// mistake with the retry, so we wait for the job above.
	iter, err := job.Read(ctx)
	if err != nil {
		return nil, js, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "read query results"))
	}

	var arrowIterator bigquery.ArrowIterator
	// We need to detect if this actually returned data. Originally we
	// checked for the presence of a schema, but it turns out statements
	// like CREATE VIEW return a schema! Then we checked if there are
	// rows, but it turns out that bigquery-emulator returns
	// iter.TotalRows == 0 (this is valid as per the API: the field is not
	// _necessarily_ populated until after a call to Next). Finally we use
	// job statistics instead
	// If the caller opted into the row-based (Storage-API-disabled) reader
	// via ctx, skip Storage entirely — the Arrow iterator returned by
	// RowIterator would silently null out pseudo-columns like
	// _PARTITIONDATE / _PARTITIONTIME. Requires the caller to have run the
	// query through a client without EnableStorageReadClient (see
	// connectionImpl.getOrCreateStorageApiDisabledClient), which they'll
	// do by consulting statement.useStorageApiDisabledClient.
	useLegacyAPI := false
	if v, ok := ctx.Value(ContextKeyUseStorageApiDisabledClient).(bool); ok {
		useLegacyAPI = v
	}
	if mayReturnResults {
		if useLegacyAPI {
			arrowIterator = newRowBasedArrowIterator(iter, alloc)
		} else if arrowIterator, err = iter.ArrowIterator(); err != nil {
			if stats.StatementType == "SCRIPT" && err.Error() == "failed to resolve table for script job: no child jobs found" {
				// Script job with no results
				// N.B. BigQuery SDK doesn't give a structured error - it's a fmt.Errorf
				arrowIterator = emptyArrowIterator{iter.Schema}
			} else if apiErr, ok := errors.AsType[*apierror.APIError](err); ok && apiErr.GRPCStatus() != nil && apiErr.GRPCStatus().Code() == codes.PermissionDenied {
				// Preserve the previous error
				// message. readSessionUser may sound
				// unrelated but creating a "read session" is
				// the first step of using the Storage API.
				return nil, js, -1, wrap(adbc.Error{
					Code: adbc.StatusUnauthorized,
					Msg:  fmt.Sprintf("[bq] Could not read Arrow query results: (%s) %s (Arrow reader requires roles/bigquery.readSessionUser, see https://github.com/apache/arrow-adbc/issues/3282)", apiErr.GRPCStatus().Code(), apiErr.GRPCStatus().Message()),
				})
			} else {
				return nil, js, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "read Arrow query results"))
			}
		}
	} else {
		arrowIterator = emptyArrowIterator{iter.Schema}
	}
	totalRows := int64(iter.TotalRows)
	// Publish the job ID on the query so callers can surface it (e.g.
	// via schema metadata) without reaching for the *bigquery.Job.
	query.JobID = job.ID()
	return arrowIterator, js, totalRows, nil
}

func ipcReaderFromArrowIterator(arrowIterator bigquery.ArrowIterator, jobStatistics *bigquery.JobStatistics, jobID string, alloc memory.Allocator) (*ipc.Reader, *arrow.Schema, error) {
	arrowItReader := bigquery.NewArrowIteratorReader(arrowIterator)
	rdr, err := ipc.NewReader(arrowItReader, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, nil, errToAdbcErr(adbc.StatusInternal, err, "read arrow stream")
	}

	fields := make([]arrow.Field, len(arrowIterator.Schema()))
	for i, field := range arrowIterator.Schema() {
		fields[i], err = buildField(field, 0)
		if err != nil {
			rdr.Release()
			return nil, nil, err
		}
	}

	metadata, err := metadataFromJobStatistics(jobStatistics, jobID)
	if err != nil {
		rdr.Release()
		return nil, nil, err
	}
	return rdr, arrow.NewSchema(fields, metadata), nil
}

func getQueryParameter(values arrow.RecordBatch, row int, parameterMode string) ([]bigquery.QueryParameter, error) {
	parameters := make([]bigquery.QueryParameter, values.NumCols())
	includeName := parameterMode == OptionValueQueryParameterModeNamed
	schema := values.Schema()
	for i, v := range values.Columns() {
		pi, err := arrowValueToQueryParameterValue(schema.Field(i), v, row)
		if err != nil {
			return nil, err
		}
		parameters[i] = pi
		if includeName {
			parameters[i].Name = values.ColumnName(i)
		}
	}
	return parameters, nil
}

func makeDryRunReader(js *bigquery.JobStatus, jobID string) (array.RecordReader, error) {
	metadata, err := metadataFromJobStatistics(js.Statistics, jobID)
	if err != nil {
		return nil, err
	}

	statistics, ok := js.Statistics.Details.(*bigquery.QueryStatistics)
	var schema *arrow.Schema
	if !ok {
		// No schema, return an empty schema
		schema = arrow.NewSchema([]arrow.Field{}, metadata)
	} else {
		bqSchema := statistics.Schema
		fields := make([]arrow.Field, len(bqSchema))
		for i, field := range bqSchema {
			var err error
			fields[i], err = buildField(field, 0)
			if err != nil {
				return nil, err
			}
		}
		schema = arrow.NewSchema(fields, metadata)
	}
	rdr, _ := array.NewRecordReader(schema, []arrow.RecordBatch{})
	return rdr, nil
}

func runPlainQuery(ctx context.Context, logger *slog.Logger, client *bigquery.Client, query *bigquery.Query, alloc memory.Allocator, resultRecordBufferSize int, linkFailedJob bool) (array.RecordReader, int64, error) {
	ctx, cancelFn := context.WithCancel(ctx)

	arrowIterator, jobStatus, totalRows, err := runQuery(ctx, logger, client, query, false, linkFailedJob, alloc)
	if err != nil {
		cancelFn()
		return nil, -1, err
	} else if query.DryRun || arrowIterator == nil {
		cancelFn()
		rdr, err := makeDryRunReader(jobStatus, query.JobID)
		if err != nil {
			return nil, -1, err
		}
		return rdr, totalRows, nil
	}

	rdr, schema, err := ipcReaderFromArrowIterator(arrowIterator, jobStatus.Statistics, query.JobID, alloc)
	if err != nil {
		cancelFn()
		return nil, -1, err
	}

	ch := make(chan arrow.RecordBatch, resultRecordBufferSize)
	result := &reader{
		refCount: 1,
		chs:      []chan arrow.RecordBatch{ch},
		cancelFn: cancelFn,
		schema:   schema,
	}
	go func() {
		defer close(ch)
		result.setErr(sendRecordBatches(ctx, rdr, ch))
	}()
	return result, totalRows, nil
}

func sendRecordBatches(ctx context.Context, source array.RecordReader, ch chan<- arrow.RecordBatch) error {
	defer source.Release()
	for ctx.Err() == nil && source.Next() {
		rec := source.RecordBatch()
		rec.Retain()
		select {
		case ch <- rec:
		case <-ctx.Done():
			rec.Release()
			return checkContext(ctx, nil)
		}
	}
	return checkContext(ctx, source.Err())
}

func queryRecordWithSchemaCallback(ctx context.Context, logger *slog.Logger, client *bigquery.Client, query *bigquery.Query, rec arrow.RecordBatch, parameterMode string, alloc memory.Allocator, rdrSchema func(schema *arrow.Schema), addSource func(source array.RecordReader), linkFailedJob bool) (int64, error) {
	totalRows := int64(-1)
	for i := range int(rec.NumRows()) {
		parameters, err := getQueryParameter(rec, i, parameterMode)
		if err != nil {
			return -1, err
		}
		if parameters != nil {
			query.Parameters = parameters
		}

		arrowIterator, jobStatus, rows, err := runQuery(ctx, logger, client, query, false, linkFailedJob, alloc)
		if err != nil {
			return -1, err
		} else if arrowIterator == nil {
			// Dry run
			rdr, err := makeDryRunReader(jobStatus, query.JobID)
			if err != nil {
				return -1, err
			}
			rdrSchema(rdr.Schema())
			rdr.Release()
			continue
		}
		totalRows = rows
		rdr, schema, err := ipcReaderFromArrowIterator(arrowIterator, jobStatus.Statistics, query.JobID, alloc)
		if err != nil {
			return -1, err
		}
		rdrSchema(schema)
		addSource(rdr)
	}
	return totalRows, nil
}

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, logger *slog.Logger, client *bigquery.Client, query *bigquery.Query, boundParameters array.RecordReader, parameterMode string, alloc memory.Allocator, resultRecordBufferSize, prefetchConcurrency int, linkFailedJob bool) (bigqueryRdr array.RecordReader, totalRows int64, err error) {
	if boundParameters == nil {
		return runPlainQuery(ctx, logger, client, query, alloc, resultRecordBufferSize, linkFailedJob)
	}
	defer boundParameters.Release()

	ctx, cancelFn := context.WithCancel(ctx)
	var schema *arrow.Schema
	var sources []array.RecordReader
	totalRows = 0

	for boundParameters.Next() {
		rec := boundParameters.RecordBatch()
		// Each call to Record() on the record reader is allowed to release the previous record
		// and since we're doing this sequentially
		// we don't need to call rec.Retain() here and call call rec.Release() in queryRecordWithSchemaCallback
		batchRows, err := queryRecordWithSchemaCallback(ctx, logger, client, query, rec, parameterMode, alloc, func(s *arrow.Schema) {
			schema = s
		}, func(source array.RecordReader) {
			sources = append(sources, source)
		}, linkFailedJob)
		if err != nil {
			cancelFn()
			for _, source := range sources {
				source.Release()
			}
			return nil, -1, err
		}
		totalRows += batchRows
	}

	ch := make(chan arrow.RecordBatch, resultRecordBufferSize)
	rdr := &reader{
		refCount: 1,
		chs:      []chan arrow.RecordBatch{ch},
		cancelFn: cancelFn,
		schema:   schema,
	}

	go func() {
		defer close(ch)
		for i, source := range sources {
			if err := sendRecordBatches(ctx, source, ch); err != nil {
				for _, remaining := range sources[i+1:] {
					remaining.Release()
				}
				rdr.setErr(err)
				return
			}
		}
	}()
	return rdr, totalRows, nil
}

func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		r.cancelFn()
		// Buffered batches are retained and allocated with mallocator, so they
		// must be released explicitly. Draining until close also waits for the
		// producer to release its source before Release returns.
		for _, ch := range r.chs {
			for rec := range ch {
				rec.Release()
			}
		}
	}
}

func (r *reader) setErr(err error) {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	r.err = err
}

func (r *reader) Err() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

func (r *reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if r.curChIndex >= len(r.chs) {
		return false
	}
	var ok bool
	for r.curChIndex < len(r.chs) {
		if r.rec, ok = <-r.chs[r.curChIndex]; ok {
			break
		}
		r.curChIndex++
	}
	return r.rec != nil
}

func (r *reader) Schema() *arrow.Schema {
	return r.schema
}

func (r *reader) Record() arrow.RecordBatch {
	return r.rec
}

func (r *reader) RecordBatch() arrow.RecordBatch {
	return r.rec
}

type emptyArrowIterator struct {
	schema bigquery.Schema
}

func (e emptyArrowIterator) Next() (*bigquery.ArrowRecordBatch, error) {
	return nil, errors.New("Next should never be invoked on an empty iterator")
}

func (e emptyArrowIterator) Schema() bigquery.Schema {
	return e.schema
}

func (e emptyArrowIterator) SerializedArrowSchema() []byte {
	emptySchema := arrow.NewSchema([]arrow.Field{}, nil)

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(emptySchema))

	err := writer.Close()
	if err != nil {
		log.Fatalf("Error serializing an empty schema: %v", err)
	}

	return buf.Bytes()
}

var _ array.RecordReader = (*reader)(nil)
