package kafkarator

import (
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// ---- Telemetry mock ----

type mockTelemetry struct {
	tracer trace.Tracer
	meter  metric.Meter
	logger *slog.Logger
}

func newMockTelemetry() TelemetryProvider {
	return &mockTelemetry{
		tracer: tracenoop.NewTracerProvider().Tracer("test"),
		meter:  noop.NewMeterProvider().Meter("test"),
		logger: newTestLogger(io.Discard),
	}
}

func (m *mockTelemetry) Tracer() trace.Tracer {
	return m.tracer
}

func (m *mockTelemetry) Meter() metric.Meter {
	return m.meter
}

func (m *mockTelemetry) Logger() *slog.Logger {
	return m.logger
}

func newTestLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}
