package middleware

import (
	"context"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// LoggingMiddleware wraps an http.Handler with structured logging for Loki
func LoggingMiddleware(serviceName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Generate or extract trace ID
		traceID := r.Header.Get("X-Trace-ID")
		if traceID == "" {
			traceID = uuid.New().String()[:8]
		}
		spanID := uuid.New().String()[:8]

		// Create logger with context fields
		logger := log.With().
			Str("service", serviceName).
			Str("component", "http").
			Str("trace_id", traceID).
			Str("span_id", spanID).
			Logger()

		ctx := logger.WithContext(r.Context())

		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r.WithContext(ctx))

		duration := time.Since(start)

		// Structured log with Loki labels
		logger.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("query", r.URL.RawQuery).
			Str("remote_addr", r.RemoteAddr).
			Str("user_agent", r.UserAgent()).
			Int("status", wrapped.statusCode).
			Dur("duration", duration).
			Int64("duration_ms", duration.Milliseconds()).
			Msg("HTTP request")
	})
}

// ErrorLoggingMiddleware wraps an http.Handler with panic recovery and error logging
func ErrorLoggingMiddleware(serviceName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				traceID := r.Header.Get("X-Trace-ID")
				if traceID == "" {
					traceID = "unknown"
				}

				log.Error().
					Str("service", serviceName).
					Str("component", "http").
					Str("trace_id", traceID).
					Str("method", r.Method).
					Str("path", r.URL.Path).
					Interface("error", err).
					Str("stack", string(debug.Stack())).
					Msg("Panic recovered")

				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// GetLoggerFromContext extracts the logger from context
func GetLoggerFromContext(ctx context.Context) zerolog.Logger {
	return *zerolog.Ctx(ctx)
}

// AddContextFields adds fields to the logger in context
func AddContextFields(ctx context.Context, fields map[string]interface{}) context.Context {
	logger := *zerolog.Ctx(ctx)
	for k, v := range fields {
		logger = logger.With().Interface(k, v).Logger()
	}
	return logger.WithContext(ctx)
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}