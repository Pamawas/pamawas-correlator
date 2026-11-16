package middleware

import (
	"net/http"

	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

// RouteLimit is a per-route token-bucket budget: rps sustained requests per
// second, burst peak allowed in a single instant.
type RouteLimit struct {
	RPS   float64
	Burst int
}

// DefaultRouteLimit applies to any path not listed explicitly. Unknown paths
// 404 at the mux anyway; giving them one shared limiter keeps a path-scanning
// client from amplifying load without growing this map.
var DefaultRouteLimit = RouteLimit{RPS: 20, Burst: 40}

// RateLimitMiddleware wraps an http.Handler with per-route token-bucket
// limiting (design review P5: protect inbound endpoints from traffic spikes
// and abuse).
//
// Limiters are built once at construction from `limits`, so the map cannot
// grow with attacker-controlled paths. Health, readiness, and metrics
// endpoints are exempt: Kubernetes probes and Prometheus scrapers must never
// be throttled — doing so would turn a load spike into a false unhealthy
// cascade. Rejection happens before the handler runs, so an expensive manual
// trigger cannot be used to amplify load.
func RateLimitMiddleware(serviceName string, limits map[string]RouteLimit, next http.Handler) http.Handler {
	built := make(map[string]*rate.Limiter, len(limits)+1)
	for path, l := range limits {
		burst := l.Burst
		if burst < 1 {
			burst = 1
		}
		built[path] = rate.NewLimiter(rate.Limit(l.RPS), burst)
	}
	defBurst := DefaultRouteLimit.Burst
	if defBurst < 1 {
		defBurst = 1
	}
	defaultLimiter := rate.NewLimiter(rate.Limit(DefaultRouteLimit.RPS), defBurst)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isExemptPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		lim, ok := built[r.URL.Path]
		if !ok {
			lim = defaultLimiter
		}
		if !lim.Allow() {
			log.Warn().
				Str("service", serviceName).
				Str("component", "ratelimit").
				Str("path", r.URL.Path).
				Str("remote_addr", r.RemoteAddr).
				Msg("rate limit exceeded")
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isExemptPath reports whether a path bypasses rate limiting: liveness and
// readiness probes plus the metrics scrape endpoint.
func isExemptPath(path string) bool {
	switch path {
	case "/healthz", "/ready", "/metrics":
		return true
	}
	return false
}
