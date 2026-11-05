// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	gax "github.com/googleapis/gax-go/v2"
	bqv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// fakeTablesAPI serves tables.list (two pages) and tables.get for
// projects/p/datasets/d, counting the requests of each kind.
type fakeTablesAPI struct {
	tables        map[string]string // table id -> type
	pages         [][]string        // table ids per tables.list page
	listCalls     atomic.Int32
	getCalls      atomic.Int32
	failFirstList atomic.Bool // fail the first tables.list call with a 503
}

func (f *fakeTablesAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const prefix = "/bigquery/v2/projects/p/datasets/d/tables"
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == prefix:
		if f.failFirstList.CompareAndSwap(true, false) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":503,"message":"backend unavailable","errors":[{"reason":"backendError"}]}}`))
			return
		}
		f.listCalls.Add(1)
		page := 0
		if tok := r.URL.Query().Get("pageToken"); tok != "" {
			page = 1
		}
		tables := make([]map[string]any, 0)
		for _, id := range f.pages[page] {
			tables = append(tables, map[string]any{
				"tableReference": map[string]string{"projectId": "p", "datasetId": "d", "tableId": id},
				"type":           f.tables[id],
			})
		}
		resp := map[string]any{"tables": tables}
		if page == 0 {
			resp["nextPageToken"] = "page2"
		}
		_ = json.NewEncoder(w).Encode(resp)
	case strings.HasPrefix(r.URL.Path, prefix+"/"):
		f.getCalls.Add(1)
		id := strings.TrimPrefix(r.URL.Path, prefix+"/")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tableReference": map[string]string{"projectId": "p", "datasetId": "d", "tableId": id},
			"type":           f.tables[id],
		})
	default:
		http.NotFound(w, r)
	}
}

func newFakeTablesConnection(t *testing.T, skipTableMetadata bool) (*connectionImpl, *fakeTablesAPI) {
	t.Helper()
	api := &fakeTablesAPI{
		tables: map[string]string{
			"orders":    "TABLE",
			"customers": "TABLE",
			"orders_v":  "VIEW",
			"orders_mv": "MATERIALIZED_VIEW",
			"ext":       "EXTERNAL",
		},
		pages: [][]string{{"orders", "customers", "orders_v"}, {"orders_mv", "ext"}},
	}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)

	ctx := context.Background()
	opts := []option.ClientOption{
		withBigQueryRESTEndpoint(srv.URL + "/"),
		option.WithHTTPClient(srv.Client()),
		option.WithoutAuthentication(),
	}
	client, err := bigquery.NewClient(ctx, "p", opts...)
	if err != nil {
		t.Fatalf("bigquery.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	svc, err := bqv2.NewService(ctx, opts...)
	if err != nil {
		t.Fatalf("bqv2.NewService: %v", err)
	}
	return &connectionImpl{
		client:                      client,
		tablesService:               svc,
		getObjectsSkipTableMetadata: skipTableMetadata,
	}, api
}

func tableTypes(infos []driverbase.TableInfo) map[string]string {
	res := make(map[string]string, len(infos))
	for _, info := range infos {
		res[info.TableName] = info.TableType
	}
	return res
}

func TestGetTablesForDBSchemaSkipTableMetadata(t *testing.T) {
	ctx := context.Background()

	// Default: one tables.get per listed table.
	conn, api := newFakeTablesConnection(t, false)
	want, err := conn.GetTablesForDBSchema(ctx, "p", "d", nil, nil, false)
	if err != nil {
		t.Fatalf("GetTablesForDBSchema (default): %v", err)
	}
	if got := api.getCalls.Load(); got != int32(len(api.tables)) {
		t.Fatalf("default path: expected %d tables.get calls, got %d", len(api.tables), got)
	}

	// With the option: same names and types, tables.list only.
	conn, api = newFakeTablesConnection(t, true)
	got, err := conn.GetTablesForDBSchema(ctx, "p", "d", nil, nil, false)
	if err != nil {
		t.Fatalf("GetTablesForDBSchema (skip metadata): %v", err)
	}
	if n := api.getCalls.Load(); n != 0 {
		t.Fatalf("skip metadata: expected no tables.get calls, got %d", n)
	}
	if n := api.listCalls.Load(); n != 2 {
		t.Fatalf("skip metadata: expected 2 tables.list pages, got %d", n)
	}
	wantTypes, gotTypes := tableTypes(want), tableTypes(got)
	if len(gotTypes) != len(wantTypes) {
		t.Fatalf("skip metadata: expected %v, got %v", wantTypes, gotTypes)
	}
	for name, typ := range wantTypes {
		if gotTypes[name] != typ {
			t.Fatalf("skip metadata: table %s: expected type %q, got %q", name, typ, gotTypes[name])
		}
	}
	for _, info := range got {
		if info.TableConstraints != nil || info.TableColumns != nil {
			t.Fatalf("skip metadata: expected no constraints/columns for %s", info.TableName)
		}
	}
}

func TestGetTablesForDBSchemaSkipTableMetadataFilter(t *testing.T) {
	conn, api := newFakeTablesConnection(t, true)
	filter := "orders%"
	got, err := conn.GetTablesForDBSchema(context.Background(), "p", "d", &filter, nil, false)
	if err != nil {
		t.Fatalf("GetTablesForDBSchema: %v", err)
	}
	gotTypes := tableTypes(got)
	if len(gotTypes) != 3 || gotTypes["orders"] != "TABLE" || gotTypes["orders_v"] != "VIEW" || gotTypes["orders_mv"] != "MATERIALIZED_VIEW" {
		t.Fatalf("unexpected filtered tables: %v", gotTypes)
	}
	if n := api.getCalls.Load(); n != 0 {
		t.Fatalf("expected no tables.get calls, got %d", n)
	}
}

func TestGetTablesForDBSchemaSkipTableMetadataRetries(t *testing.T) {
	useShortClientBackoff(t)
	conn, api := newFakeTablesConnection(t, true)
	api.failFirstList.Store(true)
	got, err := conn.GetTablesForDBSchema(context.Background(), "p", "d", nil, nil, false)
	if err != nil {
		t.Fatalf("GetTablesForDBSchema: %v", err)
	}
	if len(got) != len(api.tables) {
		t.Fatalf("expected %d tables after retry, got %d", len(api.tables), len(got))
	}
}

// Columns still need the table schema, so the option does not apply.
func TestGetTablesForDBSchemaSkipTableMetadataIgnoredWithColumns(t *testing.T) {
	conn, api := newFakeTablesConnection(t, true)
	if _, err := conn.GetTablesForDBSchema(context.Background(), "p", "d", nil, nil, true); err != nil {
		t.Fatalf("GetTablesForDBSchema: %v", err)
	}
	if got := api.getCalls.Load(); got != int32(len(api.tables)) {
		t.Fatalf("expected %d tables.get calls, got %d", len(api.tables), got)
	}
}

func TestGetObjectsSkipTableMetadataOption(t *testing.T) {
	ctx := context.Background()
	cnxn := &connectionImpl{}
	if got, err := cnxn.GetOption(ctx, OptionGetObjectsSkipTableMetadata); err != nil || got != "false" {
		t.Fatalf("expected default false, got %q (%v)", got, err)
	}
	if err := cnxn.SetOption(ctx, OptionGetObjectsSkipTableMetadata, "true"); err != nil {
		t.Fatalf("SetOption: %v", err)
	}
	if got, err := cnxn.GetOption(ctx, OptionGetObjectsSkipTableMetadata); err != nil || got != "true" {
		t.Fatalf("expected true, got %q (%v)", got, err)
	}
	// Only the ADBC boolean values are accepted, not everything strconv.ParseBool takes.
	for _, v := range []string{"maybe", "1", "TRUE"} {
		if err := cnxn.SetOption(ctx, OptionGetObjectsSkipTableMetadata, v); err == nil {
			t.Fatalf("expected error for %q", v)
		}
	}
}

// useShortClientBackoff keeps retry tests fast.
func useShortClientBackoff(t *testing.T) {
	t.Helper()
	old := clientBackoff
	clientBackoff = gax.Backoff{Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 1}
	t.Cleanup(func() { clientBackoff = old })
}

// fakeCall returns the given errors in order, then succeeds.
func fakeCall(errs ...error) (func(...googleapi.CallOption) (string, error), *int) {
	calls := 0
	return func(...googleapi.CallOption) (string, error) {
		calls++
		if calls <= len(errs) {
			return "", errs[calls-1]
		}
		return "ok", nil
	}, &calls
}

func TestDoWithRetryRetriesConnectionReset(t *testing.T) {
	useShortClientBackoff(t)
	do, calls := fakeCall(&url.Error{Op: "Get", URL: "https://bigquery.googleapis.com", Err: errors.New("read: connection reset by peer")})
	res, err := doWithRetry(context.Background(), do, defaultRetryReasons)
	if err != nil || res != "ok" || *calls != 2 {
		t.Fatalf("expected success on 2nd attempt, got res=%q err=%v calls=%d", res, err, *calls)
	}
}

func TestDoWithRetryReturnsOriginalAPIError(t *testing.T) {
	useShortClientBackoff(t)
	apiErr := &googleapi.Error{
		Code:    http.StatusForbidden,
		Message: "Access Denied: Dataset p:d",
		Errors:  []googleapi.ErrorItem{{Reason: "accessDenied", Message: "Access Denied: Dataset p:d"}},
	}
	do, calls := fakeCall(apiErr)
	_, err := doWithRetry(context.Background(), do, defaultRetryReasons)
	if *calls != 1 {
		t.Fatalf("expected no retry for accessDenied, got %d calls", *calls)
	}
	if err != apiErr {
		t.Fatalf("expected the original *googleapi.Error, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "accessDenied") {
		t.Fatalf("expected error message to keep the reason, got %q", err.Error())
	}
}

// bigquery.Client retries rate limits by reason, not by status code.
func TestDoWithRetryDoesNotRetryBare429(t *testing.T) {
	useShortClientBackoff(t)
	do, calls := fakeCall(&googleapi.Error{Code: http.StatusTooManyRequests, Message: "slow down"})
	if _, err := doWithRetry(context.Background(), do, defaultRetryReasons); err == nil || *calls != 1 {
		t.Fatalf("expected a single failed attempt, got err=%v calls=%d", err, *calls)
	}
	do, calls = fakeCall(&googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}}})
	if _, err := doWithRetry(context.Background(), do, defaultRetryReasons); err != nil || *calls != 2 {
		t.Fatalf("expected rateLimitExceeded to be retried, got err=%v calls=%d", err, *calls)
	}
}

func TestDoWithRetryKeepsAPIErrorWhenContextDone(t *testing.T) {
	old := clientBackoff
	clientBackoff = gax.Backoff{Initial: time.Hour, Max: time.Hour, Multiplier: 1}
	t.Cleanup(func() { clientBackoff = old })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	apiErr := &googleapi.Error{Code: http.StatusServiceUnavailable, Message: "backend unavailable"}
	do, _ := fakeCall(apiErr, apiErr, apiErr)
	_, err := doWithRetry(ctx, do, defaultRetryReasons)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline error, got %v", err)
	}
	if !errors.Is(err, apiErr) {
		t.Fatalf("expected the last API error to be kept, got %v", err)
	}
}

type fakeDatasetsAPI struct {
	datasets   []string
	pageSize   int
	listCalls  atomic.Int32
	getCalls   atomic.Int32
	maxResults atomic.Value
	getStatus  int
}

func (f *fakeDatasetsAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const prefix = "/bigquery/v2/projects/p/datasets"
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == prefix:
		f.listCalls.Add(1)
		f.maxResults.Store(r.URL.Query().Get("maxResults"))
		start := 0
		if tok := r.URL.Query().Get("pageToken"); tok != "" {
			start, _ = strconv.Atoi(tok)
		}
		end := min(start+f.pageSize, len(f.datasets))
		datasets := make([]map[string]any, 0)
		for _, id := range f.datasets[start:end] {
			datasets = append(datasets, map[string]any{
				"datasetReference": map[string]string{"projectId": "p", "datasetId": id},
			})
		}
		resp := map[string]any{"datasets": datasets}
		if end < len(f.datasets) {
			resp["nextPageToken"] = strconv.Itoa(end)
		}
		_ = json.NewEncoder(w).Encode(resp)
	case strings.HasPrefix(r.URL.Path, prefix+"/"):
		f.getCalls.Add(1)
		id := strings.TrimPrefix(r.URL.Path, prefix+"/")
		status := f.getStatus
		if status == 0 && !slices.Contains(f.datasets, id) {
			status = http.StatusNotFound
		}
		if status != 0 {
			w.WriteHeader(status)
			_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":"fake error for %s"}}`, status, id)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"datasetReference": map[string]string{"projectId": "p", "datasetId": id},
		})
	default:
		http.NotFound(w, r)
	}
}

func newFakeDatasetsConnection(t *testing.T, disableWildcards bool) (*connectionImpl, *fakeDatasetsAPI) {
	t.Helper()
	api := &fakeDatasetsAPI{
		datasets: []string{"a_b", "axb", "A_B", "other", "a_b_c"},
		pageSize: 2,
	}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)

	client, err := bigquery.NewClient(context.Background(), "p",
		withBigQueryRESTEndpoint(srv.URL+"/"),
		option.WithHTTPClient(srv.Client()),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("bigquery.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &connectionImpl{
		client:                     client,
		getObjectsDisableWildcards: disableWildcards,
	}, api
}

func TestGetDBSchemasForCatalogListsWithLargePages(t *testing.T) {
	conn, api := newFakeDatasetsConnection(t, false)
	filter := "a_b"
	got, err := conn.GetDBSchemasForCatalog(context.Background(), "p", &filter)
	if err != nil {
		t.Fatalf("GetDBSchemasForCatalog: %v", err)
	}
	if want := []string{"a_b", "axb", "A_B"}; !slices.Equal(got, want) {
		t.Fatalf("expected pattern matches %v, got %v", want, got)
	}
	if n := api.getCalls.Load(); n != 0 {
		t.Fatalf("expected no datasets.get calls, got %d", n)
	}
	if n := api.listCalls.Load(); n != 3 {
		t.Fatalf("expected 3 datasets.list pages, got %d", n)
	}
	if got := api.maxResults.Load(); got != strconv.Itoa(datasetsListPageSize) {
		t.Fatalf("expected maxResults=%d, got %v", datasetsListPageSize, got)
	}
}

func TestGetDBSchemasForCatalogDisableWildcards(t *testing.T) {
	for _, filter := range []string{"a_b", "A_B"} {
		conn, api := newFakeDatasetsConnection(t, true)
		got, err := conn.GetDBSchemasForCatalog(context.Background(), "p", &filter)
		if err != nil {
			t.Fatalf("%s: GetDBSchemasForCatalog: %v", filter, err)
		}
		if want := []string{filter}; !slices.Equal(got, want) {
			t.Fatalf("%s: expected %v, got %#v", filter, want, got)
		}
		if n := api.listCalls.Load(); n != 0 {
			t.Fatalf("%s: expected no datasets.list calls, got %d", filter, n)
		}
		if n := api.getCalls.Load(); n != 1 {
			t.Fatalf("%s: expected 1 datasets.get call, got %d", filter, n)
		}
	}
}

func TestGetDBSchemasForCatalogDisableWildcardsNilFilterLists(t *testing.T) {
	conn, api := newFakeDatasetsConnection(t, true)
	got, err := conn.GetDBSchemasForCatalog(context.Background(), "p", nil)
	if err != nil {
		t.Fatalf("GetDBSchemasForCatalog: %v", err)
	}
	if !slices.Equal(got, api.datasets) || api.getCalls.Load() != 0 {
		t.Fatalf("expected all datasets via list, got %v (%d gets)", got, api.getCalls.Load())
	}
}

func TestGetDBSchemasForCatalogDisableWildcardsErrors(t *testing.T) {
	for _, tc := range []struct {
		filter    string
		getStatus int
		want      adbc.Status
	}{
		{"missing", 0, adbc.StatusNotFound},
		{"a_b%", 0, adbc.StatusNotFound},
		{"a_b", http.StatusForbidden, adbc.StatusIO},
		{"a_b", http.StatusBadRequest, adbc.StatusInvalidArgument},
	} {
		conn, api := newFakeDatasetsConnection(t, true)
		api.getStatus = tc.getStatus
		got, err := conn.GetDBSchemasForCatalog(context.Background(), "p", &tc.filter)
		var adbcErr adbc.Error
		if !errors.As(err, &adbcErr) || adbcErr.Code != tc.want {
			t.Fatalf("%s (%d): expected status %v, got %#v (%v)", tc.filter, tc.getStatus, tc.want, got, err)
		}
		if n := api.listCalls.Load(); n != 0 {
			t.Fatalf("%s (%d): expected no datasets.list calls, got %d", tc.filter, tc.getStatus, n)
		}
	}
}

func TestGetTablesForDBSchemaDisableWildcards(t *testing.T) {
	for _, skipTableMetadata := range []bool{false, true} {
		for filter, want := range map[string][]string{
			"orders":   {"orders"},
			"ORDERS":   {},
			"orders%":  {},
			"orders_v": {"orders_v"},
		} {
			conn, _ := newFakeTablesConnection(t, skipTableMetadata)
			conn.getObjectsDisableWildcards = true
			got, err := conn.GetTablesForDBSchema(context.Background(), "p", "d", &filter, nil, false)
			if err != nil {
				t.Fatalf("%s (skip=%v): GetTablesForDBSchema: %v", filter, skipTableMetadata, err)
			}
			names := make([]string, 0, len(got))
			for _, info := range got {
				names = append(names, info.TableName)
			}
			if !slices.Equal(names, want) {
				t.Fatalf("%s (skip=%v): expected %v, got %v", filter, skipTableMetadata, want, names)
			}
		}
	}
}

func TestGetCatalogsDisableWildcards(t *testing.T) {
	conn, _ := newFakeDatasetsConnection(t, true)
	for filter, want := range map[string][]string{"p": {"p"}, "_": {}, "%": {}, "P": {}} {
		got, err := conn.GetCatalogs(context.Background(), &filter)
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("%s: expected %v, got %v (%v)", filter, want, got, err)
		}
	}
}

func TestGetObjectsDisableWildcardsOption(t *testing.T) {
	ctx := context.Background()
	cnxn := &connectionImpl{}
	if got, err := cnxn.GetOption(ctx, OptionGetObjectsDisableWildcards); err != nil || got != "false" {
		t.Fatalf("expected default false, got %q (%v)", got, err)
	}
	if err := cnxn.SetOption(ctx, OptionGetObjectsDisableWildcards, "true"); err != nil {
		t.Fatalf("SetOption: %v", err)
	}
	if got, err := cnxn.GetOption(ctx, OptionGetObjectsDisableWildcards); err != nil || got != "true" {
		t.Fatalf("expected true, got %q (%v)", got, err)
	}
	for _, v := range []string{"maybe", "1", "TRUE"} {
		if err := cnxn.SetOption(ctx, OptionGetObjectsDisableWildcards, v); err == nil {
			t.Fatalf("expected error for %q", v)
		}
	}
}
