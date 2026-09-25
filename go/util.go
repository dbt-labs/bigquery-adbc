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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/googleapi"
)

func quoteIdentifier(ident string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(ident, "`", "\\`"))
}

// XXX: Google SDK badness.  We can't use Wait here because queries that
// *fail* with a rateLimitExceeded (e.g. too many metadata operations) will
// get the *polling* retried infinitely in Google's SDK (I believe the SDK
// wants to retry "polling for job status" rate limit exceeded but doesn't
// differentiate between them because googleapi.CheckResponse appears to put
// the API error from the response object as an error of the API call, from
// digging around using a debugger.  In other words, it seems to be confusing
// "I got an error that my API request was rate limited" and "I got an error
// that my job was rate limited" because their internal APIs mix both errors
// into a single error path.)
func safeWaitForJob(ctx context.Context, logger *slog.Logger, job *bigquery.Job) (js *bigquery.JobStatus, err error) {
	logger.DebugContext(ctx, "waiting for job", "id", job.ID())
	backoff := gax.Backoff{
		Initial:    50 * time.Millisecond,
		Multiplier: 1.3,
		Max:        60 * time.Second,
	}

	// dry-run jobs already have a status. as an optimization, poll LastStatus (which is a simple getter)
	js = job.LastStatus()
	if js.Err() != nil || js.Done() {
		logger.DebugContext(ctx, "job complete", "id", job.ID())
		return js, nil
	}

	for {
		js, err = func() (*bigquery.JobStatus, error) {
			ctxWithDeadline, cancel := context.WithTimeout(ctx, time.Minute*5)
			defer cancel()
			js, err := job.Status(ctxWithDeadline)
			if err != nil {
				return nil, err
			}
			return js, err
		}()

		if err != nil {
			// Note that we do not retry cancellations because we
			// can't differentiate between our own timeout and the
			// user-supplied deadline. We can retry "rate limited"
			// here because job.Status does not behave like job.Wait
			// and does not put the job's error into the API call's
			// error.
			if isRetryableError(err, jobStatusRetryReasons) {
				duration := backoff.Pause()
				logger.DebugContext(ctx, "retry job", "id", job.ID(), "backoff", duration, "error", err)
				if err := gax.Sleep(ctx, duration); err != nil {
					return nil, err
				}

				continue
			}
			logger.DebugContext(ctx, "job failed", "id", job.ID(), "error", err)
			return nil, errToAdbcErr(adbc.StatusInternal, err, "poll job status")
		}

		if js.Err() != nil || js.Done() {
			break
		}

		duration := backoff.Pause()
		logger.DebugContext(ctx, "job not complete", "id", job.ID(), "backoff", duration)
		if err := gax.Sleep(ctx, duration); err != nil {
			return nil, err
		}
	}
	logger.DebugContext(ctx, "job complete", "id", job.ID())
	return
}

var (
	// jobStatusRetryReasons are the error reasons retried when polling job
	// status. rateLimitExceeded is deliberately absent: a job that fails with
	// it would otherwise be polled forever (see safeWaitForJob).
	jobStatusRetryReasons = []string{"backendError", "internalError"}

	// defaultRetryReasons are the reasons bigquery.Client retries for its
	// non-job API calls, such as tables.list:
	// https://github.com/googleapis/google-cloud-go/blob/bigquery/v1.85.0/bigquery/bigquery.go#L254-L255
	defaultRetryReasons = []string{"backendError", "rateLimitExceeded"}
)

// isRetryableError reports whether err is transient, retrying structured
// errors whose first reason is in retryableReasons.
func isRetryableError(err error, retryableReasons []string) bool {
	// Modeled on retryableError in bigquery.go:
	// https://github.com/googleapis/google-cloud-go/blob/bigquery/v1.85.0/bigquery/bigquery.go#L269-L325
	switch {
	case err == nil:
		return false
	case err == io.ErrUnexpectedEOF:
		return true
	case err.Error() == "http2: stream closed":
		return true
	}

	switch e := err.(type) {
	case *googleapi.Error:
		var reason string
		if len(e.Errors) > 0 {
			reason = e.Errors[0].Reason

			if slices.Contains(retryableReasons, reason) {
				return true
			}
		}

		if slices.Contains([]int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout}, e.Code) {
			return true
		}

	case *url.Error:
		for _, r := range []string{"connection refused", "connection reset"} {
			if strings.Contains(e.Error(), r) {
				return true
			}
		}

	case interface{ Temporary() bool }:
		if e.Temporary() {
			return true
		}
	}

	return isRetryableError(errors.Unwrap(err), retryableReasons)
}

// clientBackoff is the backoff bigquery.Client uses when retrying API calls:
// https://github.com/googleapis/google-cloud-go/blob/bigquery/v1.85.0/bigquery/bigquery.go#L238-L252
var clientBackoff = gax.Backoff{
	Initial:    1 * time.Second,
	Max:        32 * time.Second,
	Multiplier: 2,
}

// doWithRetry calls a generated REST API call
// https://github.com/googleapis/google-cloud-go/blob/bigquery/v1.85.0/bigquery/dataset.go#L658-L675
// https://github.com/googleapis/google-api-go-client/blob/v0.287.1/internal/gensupport/send.go#L85-L97
func doWithRetry[T any](ctx context.Context, do func(...googleapi.CallOption) (T, error), retryableReasons []string) (T, error) {
	var res T
	var lastErr error // lastErr is the raw error returned from the last call being retried
	err := gax.Invoke(ctx, func(context.Context, gax.CallSettings) error {
		res, lastErr = do()
		return lastErr
	}, gax.WithRetry(func() gax.Retryer {
		return gax.OnErrorFunc(clientBackoff, func(err error) bool {
			return isRetryableError(err, retryableReasons)
		})
	}))

	// err returned from gax.Invoke could be either a context end error (context.Canceled / DeadlineExceeded)
	// or a wrapped error if the lastErr is not retried
	// https://github.com/googleapis/google-cloud-go/blob/v0.123.0/internal/retry.go#L35-L55
	// https://github.com/googleapis/gax-go/blob/v2.23.0/v2/invoke.go#L121-L123
	if err != nil && lastErr != nil && !errors.Is(err, lastErr) {
		return res, errors.Join(err, lastErr)
	}
	return res, lastErr
}

// errToAdbcErr converts an error to an ADBC error, using the metadata from
// Google API errors if possible and including the supplied context
func errToAdbcErr(defaultStatus adbc.Status, err error, errContext string, contextArgs ...any) error {
	if errors.Is(err, adbc.Error{}) {
		return err
	} else if errors.Is(err, context.Canceled) {
		return adbc.Error{
			Code: adbc.StatusCancelled,
			Msg:  fmt.Sprintf("[bq] cancelled %s: %s", fmt.Sprintf(errContext, contextArgs...), err.Error()),
		}
	} else if errors.Is(err, context.DeadlineExceeded) {
		return adbc.Error{
			Code: adbc.StatusTimeout,
			Msg:  fmt.Sprintf("[bq] deadline exceeded: %s", fmt.Sprintf(errContext, contextArgs...)),
		}
	}

	adbcErr := adbc.Error{
		Code: defaultStatus,
	}
	var msg strings.Builder
	msg.WriteString("[bq] Could not")
	fmt.Fprintf(&msg, " %s", fmt.Sprintf(errContext, contextArgs...))
	msg.WriteString(": ")

	statusCode := -1
	if httpErr, ok := errors.AsType[*googleapi.Error](err); ok {
		statusCode = httpErr.Code
		fmt.Fprintf(&msg, "%d %s: %s", httpErr.Code, http.StatusText(httpErr.Code), httpErr.Message)
	} else if apiErr, ok := errors.AsType[*apierror.APIError](err); ok {
		// Despite all the structure inside the error, there isn't a great way to
		// extract or map it onto anything (e.g. there are two types of errors
		// depending on whether HTTP or gRPC is used, but you can't actually
		// branch on that because the HTTP error is not exposed to you)
		msg.WriteString(apiErr.Error())
	} else if urlErr, ok := errors.AsType[*url.Error](err); ok {
		cleanURL := urlErr.URL
		if url, err := url.Parse(urlErr.URL); err == nil {
			url.RawQuery = ""
			cleanURL = url.String()
		}
		fmt.Fprintf(&msg, "failed to %s %s", urlErr.Op, cleanURL)
		if urlErr.Err != nil {
			fmt.Fprintf(&msg, ": %s", urlErr.Err.Error())
		}
	} else if bqErr, ok := errors.AsType[*bigquery.Error](err); ok {
		fmt.Fprintf(&msg, "%s: %s (%s)", bqErr.Reason, bqErr.Message, bqErr.Location)

		switch bqErr.Reason {
		case "accessDenied", "billingNotEnabled", "blocked":
			adbcErr.Code = adbc.StatusUnauthorized
		case "attributeError", "badRequest", "billingTierLimitExceeded", "invalid", "invalidQuery", "invalidUser":
			adbcErr.Code = adbc.StatusInvalidArgument
		case "backendError", "jobBackendError", "jobInternalError", "jobRateLimitExceeded", "quotaExceeded", "rateLimitExceeded", "resourceInUse", "resourcesExceeded":
			adbcErr.Code = adbc.StatusInternal
		case "duplicate":
			adbcErr.Code = adbc.StatusAlreadyExists
		case "notFound", "tableUnavailable":
			adbcErr.Code = adbc.StatusNotFound
		case "notImplemented":
			adbcErr.Code = adbc.StatusNotImplemented
		case "proxyAuthenticationRequired", "responseTooLarge":
			adbcErr.Code = adbc.StatusIO
		case "stopped":
			adbcErr.Code = adbc.StatusCancelled
		case "timeout":
			adbcErr.Code = adbc.StatusTimeout
		}
	} else {
		msg.WriteString(err.Error())
	}

	if authErr, ok := errors.AsType[*auth.Error](err); ok && statusCode <= 0 {
		statusCode = authErr.Response.StatusCode
	}

	switch statusCode {
	case http.StatusBadRequest:
		adbcErr.Code = adbc.StatusInvalidArgument
	case http.StatusNotFound:
		adbcErr.Code = adbc.StatusNotFound
	case http.StatusUnauthorized:
		adbcErr.Code = adbc.StatusUnauthorized
	}

	adbcErr.Msg = msg.String()

	if isReauthError(err.Error()) {
		adbcErr.Code = adbc.StatusUnauthorized
		adbcErr.Msg += ". " + reauthGuidance
	}

	return adbcErr
}

const reauthGuidance = "Your Google Workspace admin requires re-authentication (RAPT). " +
	"Consider using a service account instead of user credentials, or re-authenticate " +
	"interactively with 'gcloud auth application-default login'. " +
	"See https://support.google.com/a/answer/9368756"

func isReauthError(s string) bool {
	return strings.Contains(s, "invalid_rapt") || strings.Contains(s, "reauth related error")
}

func retryWithBackoff(ctx context.Context, context string, maxAttempts int, backoff gax.Backoff, f func() (bool, error)) error {
	attempt := 0
	for {
		complete, err := f()
		if complete {
			return err
		}

		duration := backoff.Pause()
		if sleepErr := gax.Sleep(ctx, duration); sleepErr != nil {
			return err
		}
		attempt++

		if attempt >= maxAttempts {
			return adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("[bq] could not %s: maximum retry attempts exceeded: %v", context, err),
			}
		}
	}
}

func retry(ctx context.Context, context string, f func() (bool, error)) error {
	backoff := gax.Backoff{
		Initial:    100 * time.Millisecond,
		Multiplier: 2.0,
		Max:        15 * time.Second,
	}
	return retryWithBackoff(ctx, context, 20, backoff, f)
}
