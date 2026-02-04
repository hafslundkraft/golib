// Package main demonstrates how to use the telemetry library for observability.
//
// This example shows:
//   - Setting up telemetry with local output (prints to console)
//   - Using structured logging with context
//   - Creating and using metrics (counters)
//   - Creating and using distributed tracing (spans)
//
// In local mode, all telemetry output is printed to the console for easy debugging.
package main

import (
	"context"
	"log"
	"time"

	"github.com/hafslundkraft/golib/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	ctx := context.Background()

	// ========================================
	// SETUP: Create telemetry provider
	// ========================================
	// WithLocal(true) prints all telemetry to console instead of sending to a backend
	tp, shutdown := telemetry.New(
		ctx,
		"telemetry-demo",
		telemetry.WithLocal(true),
	)
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("Failed to shutdown telemetry: %v\n", err)
		}
	}()

	// ========================================
	// MAIN DEMO: Using telemetry features
	// ========================================

	// 1. LOGGING - Structured logging with context
	logger := tp.Logger()
	logger.InfoContext(ctx, "Starting telemetry demo")

	// 2. TRACING - Track operations with distributed tracing
	tracer := tp.Tracer()
	ctx, span := tracer.Start(
		ctx,
		"demo-operation",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	// Add attributes to spans for rich context
	span.SetAttributes(
		attribute.String("demo.environment", "local"),
		attribute.String("demo.version", "1.0.0"),
	)

	// 3. METRICS - Track numerical data (counters, gauges, histograms)
	meter := tp.Meter()
	demoCounter, err := meter.Int64Counter(
		"demo.counter",
		metric.WithDescription("Demo counter for local telemetry output"),
	)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create counter", "error", err)
		return
	}

	// Use all three together: log, trace, and count
	for i := 0; i < 3; i++ {
		// Log with structured fields
		logger.InfoContext(ctx, "Counter incremented", "value", i)

		// Increment metric with attributes
		demoCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("operation", "demo"),
			attribute.Int("iteration", i),
		))

		time.Sleep(50 * time.Millisecond)
	}

	// Mark span as successful
	span.SetStatus(codes.Ok, "demo completed successfully")
	logger.InfoContext(ctx, "Telemetry demo completed")

	// Give the exporter time to flush
	time.Sleep(2 * time.Second)
}
