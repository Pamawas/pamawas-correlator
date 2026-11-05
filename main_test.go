package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMetricsRoutes(t *testing.T) {
	mux := http.NewServeMux()
	registerMetricsRoutes(mux, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, test := range []struct {
		path       string
		statusCode int
	}{
		{path: "/metrics", statusCode: http.StatusOK},
		{path: "/api/v1/metrics", statusCode: http.StatusNotFound},
	} {
		t.Run(test.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
			if response.Code != test.statusCode {
				t.Fatalf("GET %s returned status %d, want %d", test.path, response.Code, test.statusCode)
			}
		})
	}
}
