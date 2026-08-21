package otel

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds OTel configuration
type Config struct {
	ServiceName  string
	OTLPEndpoint string // e.g., "tempo:4317"
	Insecure     bool
	SampleRatio  float64 // 0.0 to 1.0
	Enabled      bool
}

// InitTracer initializes the OpenTelemetry tracer provider with OTLP gRPC exporter
func InitTracer(cfg Config) (func(context.Context) error, error) {
	if !cfg.Enabled || cfg.OTLPEndpoint == "" {
		log.Printf("[otel] tracing disabled for %s", cfg.ServiceName)
		return func(context.Context) error { return nil }, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create OTLP gRPC exporter
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource with service info
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			attribute.String("service.version", "1.0.0"),
			attribute.String("deployment.environment", "development"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace provider with batch span processor
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Set global propagator for W3C trace context
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("[otel] tracer initialized for %s, exporting to %s", cfg.ServiceName, cfg.OTLPEndpoint)

	// Return shutdown function
	return tp.Shutdown, nil
}

// Tracer returns a tracer for the given service name
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
