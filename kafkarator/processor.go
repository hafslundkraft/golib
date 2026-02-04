package kafkarator

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/codes"
)

type Processor struct {
	reader             *Reader
	tel                TelemetryProvider
	handler            ProcessFunc
	defaultReadTimeout time.Duration
}

type ProcessFunc func(ctx context.Context, msg Message) (err error)

func newProcessor(
	reader *Reader,
	tel TelemetryProvider,
	handler ProcessFunc,
	readTimeout time.Duration,
) *Processor {
	return &Processor{
		reader:             reader,
		tel:                tel,
		handler:            handler,
		defaultReadTimeout: readTimeout,
	}
}

// Close releases the underlying reader resources.
func (p *Processor) Close(ctx context.Context) error {
	return p.reader.Close(ctx)
}

func (p *Processor) processNext(ctx context.Context, maxMessages int, readTimeout time.Duration) (int, error) {
	msgs, commit, err := p.reader.Read(ctx, maxMessages, readTimeout)
	if err != nil {
		return 0, fmt.Errorf("read messages: %w", err)
	}

	processedCount := 0
	for _, msg := range msgs {
		// Extract trace context from message headers to continue the distributed trace
		msgCtx := msg.ExtractTraceContext(ctx)
		msgCtx, span := startProcessingSpan(msgCtx, p.tel.Tracer(), p.reader.topic, p.reader.consumerGroup, int32(msg.Partition), int64(msg.Offset))

		// Call user's processing function
		processErr := p.handler(msgCtx, msg)

		// Set span status based on processing result
		if processErr != nil {
			span.RecordError(processErr)
			span.SetStatus(codes.Error, processErr.Error())
			span.End()
			return processedCount, fmt.Errorf("process message (partition=%d, offset=%d): %w", msg.Partition, msg.Offset, processErr)
		}

		span.SetStatus(codes.Ok, "message processed successfully")
		span.End()
		processedCount++
	}

	// Commit offsets only after all messages are successfully processed
	if err := commit(ctx); err != nil {
		return processedCount, fmt.Errorf("commit offsets: %w", err)
	}

	return processedCount, nil
}
