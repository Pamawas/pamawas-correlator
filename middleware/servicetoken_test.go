package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServiceTokenAuthRejectsUnauthenticatedCallers(t *testing.T) {
	reached := false
	handler := ServiceTokenAuth("secret-token", IsProbeOrMetricsPath)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			reached = true
			w.WriteHeader(http.StatusOK)
		},
	))

	cases := []struct {
		name   string
		header string
		want   int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"wrong token", "nope", http.StatusUnauthorized},
		{"token prefix", "secret", http.StatusUnauthorized},
		{"correct token", "secret-token", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reached = false
			req := httptest.NewRequest(http.MethodPost, "/api/v1/trigger", nil)
			if tc.header != "" {
				req.Header.Set("X-Service-Token", tc.header)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if (tc.want == http.StatusOK) != reached {
				t.Fatalf("handler reached = %v for status %d", reached, tc.want)
			}
		})
	}
}

func TestServiceTokenAuthExemptsProbes(t *testing.T) {
	handler := ServiceTokenAuth("secret-token", IsProbeOrMetricsPath)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) },
	))
	for _, path := range []string{"/healthz", "/ready", "/metrics"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want 200 without a token", path, rec.Code)
		}
	}
}

func TestServiceTokenAuthFailsClosedWithoutConfiguration(t *testing.T) {
	// A missing secret is a misconfiguration, not an open door.
	reached := false
	handler := ServiceTokenAuth("", IsProbeOrMetricsPath)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { reached = true },
	))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/trigger", nil))
	if rec.Code != http.StatusUnauthorized || reached {
		t.Fatalf("status = %d reached = %v, want 401 and unreached", rec.Code, reached)
	}
}
