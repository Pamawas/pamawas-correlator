package middleware

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// TracingMiddleware starts a server span for every request.
//
// Without this, Tempo only ever saw the Python investigator: the Go services
// initialised a tracer provider and exported nothing, because nothing in them
// ever started a span. Their logs carried a `trace_id`, but it was a random
// eight-character UUID generated in the logging middleware — trace-shaped, and
// unrelated to tracing. The result was a platform where four of five services
// were invisible in Tempo while looking instrumented.
//
// The incoming trace context is extracted first, so a request that originated
// in another service continues that trace instead of starting a second one.
// That is what makes ingest -> correlator -> investigator -> reporter one
// trace rather than four disconnected ones.
func TracingMiddleware(serviceName string, next http.Handler) http.Handler {
	tracer := otel.Tracer(serviceName)
	propagator := traceContextPropagator()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Probes and scrapes are not requests anyone will debug. Tracing them
		// fills the backend with spans that explain nothing and, at a sample
		// ratio below 1, competes with the traffic that does matter.
		if isProbePath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		// Continue an existing trace when the caller supplied one.
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		spanName := r.Method + " " + r.URL.Path
		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(r.Method),
				semconv.URLPath(r.URL.Path),
				semconv.ServerAddress(r.Host),
				attribute.String("http.request.header.user_agent", r.UserAgent()),
			),
		)
		defer span.End()

		wrapped := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		// Make the trace context available to handlers, so log lines can carry
		// the real trace and span IDs and downstream calls can propagate them.
		next.ServeHTTP(wrapped, r.WithContext(ctx))

		span.SetAttributes(semconv.HTTPResponseStatusCode(wrapped.statusCode))
		if wrapped.statusCode >= 500 {
			span.SetStatus(codes.Error, http.StatusText(wrapped.statusCode))
		}
	})
}

// traceContextPropagator returns the propagator used to read and write trace
// context.
//
// It prefers the configured global, but falls back to W3C trace-context when
// none is installed. The global is a no-op until InitTracer sets it, and
// depending on that ordering made propagation silently stop working — a
// request from another service then started a second, unrelated trace. Trace
// propagation is a wire contract, so it must not depend on initialisation
// order inside this process.
func traceContextPropagator() propagation.TextMapPropagator {
	if p := otel.GetTextMapPropagator(); p != nil && carriesTraceContext(p) {
		return p
	}
	return propagation.TraceContext{}
}

// carriesTraceContext reports whether p reads a W3C traceparent header.
func carriesTraceContext(p propagation.TextMapPropagator) bool {
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	carrier := propagation.MapCarrier{"traceparent": traceparent}
	return trace.SpanContextFromContext(p.Extract(context.Background(), carrier)).IsValid()
}

// statusRecorder captures the response status for the span.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

// TraceIDs returns the trace and span IDs for a context, for structured logs.
//
// Returns empty strings when the context carries no valid span, so a log line
// says "no trace" rather than inventing an identifier that correlates with
// nothing.
func TraceIDs(ctx context.Context) (traceID, spanID string) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", ""
	}
	return sc.TraceID().String(), sc.SpanID().String()
}

// probePaths are liveness, readiness and scrape endpoints.
//
// They run every few seconds forever, they have no incident behind them, and
// nobody opens a trace to ask why /healthz took 400 microseconds. Spanning them
// buries the requests that do matter, so they are served untraced.
var probePaths = map[string]struct{}{
	"/health":  {},
	"/healthz": {},
	"/livez":   {},
	"/ready":   {},
	"/readyz":  {},
	"/metrics": {},
}

func isProbePath(path string) bool {
	_, ok := probePaths[path]
	return ok
}
