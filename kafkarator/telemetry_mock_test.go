package kafkarator

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// ---- Telemetry mock ----

type mockTelemetry struct {
	tracer trace.Tracer
	meter  metric.Meter
	logger Logger
}

func newMockTelemetry() TelemetryProvider {
	return &mockTelemetry{
		tracer: tracenoop.NewTracerProvider().Tracer("test"),
		meter:  noop.NewMeterProvider().Meter("test"),
		logger: mockLogger{},
	}
}

func (m *mockTelemetry) Tracer() trace.Tracer {
	return m.tracer
}

func (m *mockTelemetry) Meter() metric.Meter {
	return m.meter
}

func (m *mockTelemetry) Logger() Logger {
	return m.logger
}

// ---- Logger mock ----

type mockLogger struct{}

func (mockLogger) ErrorContext(context.Context, string, ...any) {}
func (mockLogger) InfoContext(context.Context, string, ...any)  {}
func (mockLogger) DebugContext(context.Context, string, ...any) {}
