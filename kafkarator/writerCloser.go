package kafkarator

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
)

func newWriteCloser(w *kafka.Writer, pmc metric.Int64Counter, logger *slog.Logger) WriterCloser {
	return &writerCloser{
		writer:                  w,
		logger:                  logger,
		producedMessagesCounter: pmc,
		closed:                  false,
	}
}

type writerCloser struct {
	producedMessagesCounter metric.Int64Counter
	writer                  *kafka.Writer
	logger                  *slog.Logger
	closed                  bool
}

func (wc *writerCloser) Close(ctx context.Context) error {
	if wc.closed {
		return nil // It's ok to close multiple times.
	}

	if err := wc.writer.Close(); err != nil {
		wc.logger.ErrorContext(ctx, fmt.Sprintf("failed to close writer %v", err))
		return fmt.Errorf("failed to close writer: %w", err)
	}
	wc.closed = true
	return nil
}

func (wc *writerCloser) Write(ctx context.Context, msg []byte, headers map[string][]byte) error {
	if wc.closed {
		return fmt.Errorf("writer is closed")
	}

	traceHeaders := injectTraceContext(ctx, headers)
	if err := wc.writer.WriteMessages(ctx, kafkaMessage(msg, traceHeaders)); err != nil {
		wc.logger.ErrorContext(ctx, fmt.Sprintf("while writing messages to Kafka %v", err))
		return fmt.Errorf("failed to write messages to Kafka %w", err)
	}
	wc.producedMessagesCounter.Add(ctx, 1)
	return nil
}

// injectTraceContext extracts the trace context from the current span and injects it into the headers
func injectTraceContext(ctx context.Context, headers map[string][]byte) map[string][]byte {
	// Create a new map to avoid modifying the original
	propagatedHeaders := make(map[string][]byte, len(headers))
	for k, v := range headers {
		propagatedHeaders[k] = v
	}

	// Use a MapCarrier to inject trace context
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	// Add trace context headers to the Kafka headers
	for k, v := range carrier {
		propagatedHeaders[k] = []byte(v)
	}

	return propagatedHeaders
}
