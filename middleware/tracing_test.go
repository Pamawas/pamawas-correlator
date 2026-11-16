package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestTracingMiddlewareCreatesSpans is the regression test for the gap that
// made four of five services invisible in Tempo.
//
// The Go services initialised a tracer provider but never started a span, so
// nothing was ever exported. Their logs still carried a `trace_id`, generated
// randomly in the logging middleware — trace-shaped, and unrelated to tracing.
// Tempo only ever listed pamawas-investigator, the one Python service.
func TestTracingMiddlewareCreatesSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	var spanInHandler bool
	handler := TracingMiddleware("test-service", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// The handler must see a live span, which is what lets its logs and
			// its outbound calls attach to this trace.
			traceID, spanID := TraceIDs(r.Context())
			spanInHandler = traceID != "" && spanID != ""
			w.WriteHeader(http.StatusOK)
		},
	))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/reports", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if !spanInHandler {
		t.Fatal("handler context carried no span")
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if name := spans[0].Name(); name != "GET /api/v1/reports" {
		t.Errorf("span name = %q", name)
	}
}

// TestTracingMiddlewareContinuesAnIncomingTrace verifies propagation: a
// request from another service must extend that trace, not start a new one.
func TestTracingMiddlewareContinuesAnIncomingTrace(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	// Simulate the caller: a span in another service, injected into headers.
	ctx, parent := tp.Tracer("upstream").Start(context.Background(), "upstream.work")
	// Inject with the W3C propagator explicitly: the global is a no-op in a
	// test process, and what matters here is what this service does *with* a
	// correctly-populated traceparent header.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investigations", nil)
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))
	if req.Header.Get("traceparent") == "" {
		t.Fatal("test setup failed to inject a traceparent header")
	}
	parent.End()

	TracingMiddleware("test-service", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) },
	)).ServeHTTP(httptest.NewRecorder(), req)

	for _, span := range recorder.Ended() {
		if span.Name() == "POST /api/v1/investigations" {
			if span.SpanContext().TraceID() != parent.SpanContext().TraceID() {
				t.Error("span started a new trace instead of continuing the caller's")
			}
			return
		}
	}
	t.Fatal("no server span recorded")
}

// TestAccessLogCarriesTheRealSpanIDs is the check that matters for debugging:
// the trace_id in a log line must be the trace it was actually emitted in.
//
// Before this, the logging middleware invented an eight-character UUID and
// called it trace_id. It looked like tracing and correlated with nothing: a
// search in Tempo for an ID copied from a log line always returned no results.
func TestAccessLogCarriesTheRealSpanIDs(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	// Capture the zerolog output the middleware writes.
	var logBuf bytes.Buffer
	previousLogger := log.Logger
	log.Logger = zerolog.New(&logBuf)
	t.Cleanup(func() { log.Logger = previousLogger })

	handler := TracingMiddleware("test-service",
		LoggingMiddleware("test-service", http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) },
		)),
	)
	handler.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	wantTrace := spans[0].SpanContext().TraceID().String()
	wantSpan := spans[0].SpanContext().SpanID().String()

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logBuf.Bytes()), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, logBuf.String())
	}

	if got, _ := entry["trace_id"].(string); got != wantTrace {
		t.Errorf("log trace_id = %q, span trace ID = %q", got, wantTrace)
	}
	if got, _ := entry["span_id"].(string); got != wantSpan {
		t.Errorf("log span_id = %q, span ID = %q", got, wantSpan)
	}
	// A random eight-character ID is the shape of the old bug.
	if len(wantTrace) != 32 {
		t.Errorf("trace ID %q is not a 16-byte hex W3C trace id", wantTrace)
	}
}
