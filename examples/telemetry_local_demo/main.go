package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hafslundkraft/golib/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	ctx := context.Background()

	// Create telemetry provider with local mode
	tp, shutdown := telemetry.New(
		ctx,
		"telemetry-demo",
		telemetry.WithLocal(true),
	)
	defer func() {
		if err := shutdown(ctx); err != nil {
			panic(fmt.Sprintf("Failed to shutdown telemetry: %v", err))
		}
	}()

	// Get tracer, meter, and logger from provider
	tracer := tp.Tracer()
	meter := tp.Meter()
	logger := tp.Logger()

	// Create a counter metric
	demoCounter, err := meter.Int64Counter(
		"demo.counter",
		metric.WithDescription("Demo counter for local telemetry output"),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to create counter: %v", err))
	}

	logger.WarnContext(ctx, "Starting telemetry demo...")

	// Start a span with proper configuration
	ctx, span := tracer.Start(
		ctx,
		"demo-operation",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	// Use semantic convention attributes for standardized naming
	span.SetAttributes(
		attribute.String("demo.environment", "local"),
		attribute.String("demo.version", "1.0.0"),
	)

	for i := 0; i < 3; i++ {
		logger.InfoContext(ctx, fmt.Sprintf("Counter value: %d", i))
		demoCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("operation", "demo"),
			attribute.Int("iteration", i),
		))
		time.Sleep(50 * time.Millisecond)
	}

	// Set span status to indicate success
	span.SetStatus(codes.Ok, "demo completed successfully")

	logger.ErrorContext(ctx, "Stopping telemetry demo...")

	// Give the exporter time to flush
	time.Sleep(2 * time.Second)
}
