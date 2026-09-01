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
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// getAccessToken previously built its transport without a Proxy function,
// silently bypassing HTTPS_PROXY/NO_PROXY (dbt-labs/dbt-core#14470). The
// wiring is pinned structurally because http.ProxyFromEnvironment caches the
// proxy environment on first use, which makes an in-process env-based
// end-to-end check order-dependent on other tests.
func TestTokenExchangeTransportUsesEnvProxy(t *testing.T) {
	tr := newTokenExchangeTransport("example.com")
	if tr.Proxy == nil {
		t.Fatal("token-exchange transport must set Proxy so HTTPS_PROXY/NO_PROXY are honored")
	}
	got := reflect.ValueOf(tr.Proxy).Pointer()
	want := reflect.ValueOf(http.ProxyFromEnvironment).Pointer()
	if got != want {
		t.Errorf("token-exchange transport Proxy must be http.ProxyFromEnvironment")
	}
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.ServerName != "example.com" {
		t.Errorf("token-exchange transport must preserve TLS ServerName, got %+v", tr.TLSClientConfig)
	}
}

// A failing token-exchange request (e.g. an unreachable or misconfigured
// proxy) must surface as an error to the caller, not kill the process.
func TestGetAccessTokenReturnsErrorOnRequestFailure(t *testing.T) {
	conn := &connectionImpl{
		clientID:            "test-client-id",
		clientSecret:        "test-client-secret",
		refreshToken:        "test-refresh-token",
		accessTokenEndpoint: "http://127.0.0.1:1",
	}
	if _, err := conn.getAccessToken(); err == nil {
		t.Fatal("expected an error for an unreachable token endpoint")
	}
}

// TestAPIEndpointRoutesToBigQueryV2Path confirms, through the real
// google-cloud-go BigQuery client, that a conforming apiEndpoint
// ("scheme://host[:port]/") is routed under "/bigquery/v2/".
func TestAPIEndpointRoutesToBigQueryV2Path(t *testing.T) {
	paths := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case paths <- r.URL.Path:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jobComplete":true,"jobReference":{"jobId":"x"},"schema":{"fields":[]},"rows":[]}`))
	}))
	defer srv.Close()

	client, err := bigquery.NewClient(context.Background(), "test-proj",
		withBigQueryRESTEndpoint(srv.URL+"/"),
		option.WithHTTPClient(srv.Client()),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("bigquery.NewClient returned error: %v", err)
	}
	defer client.Close()

	q := client.Query("SELECT 1")
	it, err := q.Read(context.Background())
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	_ = it

	select {
	case p := <-paths:
		if !strings.HasPrefix(p, "/bigquery/v2/") {
			t.Fatalf("expected request path under /bigquery/v2/, got %q", p)
		}
	default:
		t.Fatal("server did not receive a request")
	}
}
