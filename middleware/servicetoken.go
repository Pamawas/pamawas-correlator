package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/rs/zerolog/log"
)

// ServiceTokenAuth authenticates service-to-service calls with a shared token.
//
// Internal callers present PAMAWAS_SERVICE_TOKEN in the X-Service-Token header.
// The correlator's trigger endpoint runs a correlation cycle on demand, so an
// unauthenticated caller inside the cluster could force that work at will.
//
// Probe, readiness and scrape paths are exempt: Kubernetes and Prometheus
// cannot present a token, and requiring one would turn every probe into a
// false unhealthy cascade.
func ServiceTokenAuth(token string, exempt func(string) bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if exempt(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			presented := r.Header.Get("X-Service-Token")
			// Constant-time comparison: an early exit on the first differing
			// byte would leak the token prefix to anyone timing responses.
			if token == "" || subtle.ConstantTimeCompare([]byte(presented), []byte(token)) != 1 {
				log.Warn().
					Str("path", r.URL.Path).
					Str("remote_addr", r.RemoteAddr).
					Bool("token_presented", presented != "").
					Msg("Rejected request with invalid service token")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				// The status line is already written; a failure here means the
				// connection dropped, which errcheck must still see handled.
				if _, err := w.Write([]byte(`{"error":{"code":"unauthorized","message":"Invalid or missing service token"}}`)); err != nil {
					log.Debug().Err(err).Msg("Failed to write unauthorized response body")
				}
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// IsProbeOrMetricsPath reports whether a path must stay reachable without
// authentication: liveness/readiness probes and the metrics scrape.
func IsProbeOrMetricsPath(path string) bool {
	return path == "/healthz" || path == "/ready" || path == "/metrics"
}
