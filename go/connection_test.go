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
	"net/http"
	"reflect"
	"testing"
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
