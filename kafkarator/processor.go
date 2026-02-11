package kafkarator

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/codes"
)

// Processor provides automatic processing of Kafka messages with built-in
// distributed tracing and offset management. It reads messages in groups (up to maxMessages),
// calls a handler function for each message, and commits offsets only after successful
// processing of all messages.
type Processor struct {
	reader                 *Reader
	tel                    TelemetryProvider
	handler                ProcessFunc
	defaultReadTimeout     time.Duration
	defaultMaxMessages     int
	defaultAutoOffsetReset autoOffsetReset
}

// ProcessFunc is a function that processes a single Kafka message.
// It receives a context with propagated trace information and should return
// an error if processing fails. If an error is returned, the Processor will
// stop processing and will not commit offsets.
type ProcessFunc func(ctx context.Context, msg *Message) error

// ProcessorOption configures a Processor instance.
type ProcessorOption func(*processorConfig)

type processorConfig struct {
	readTimeout     time.Duration
	maxMessages     int
	autoOffsetReset autoOffsetReset
}

func defaultProcessorConfig() processorConfig {
	return processorConfig{
		readTimeout:     10 * time.Second,
		maxMessages:     10,
		autoOffsetReset: offsetEarliest,
	}
}

// WithProcessorReadTimeout sets the default read timeout for the processor.
// Default is 10 seconds.
func WithProcessorReadTimeout(timeout time.Duration) ProcessorOption {
	return func(cfg *processorConfig) {
		if timeout < 0 {
			timeout = 0
		}
		cfg.readTimeout = timeout
	}
}

// WithProcessorMaxMessages sets the default maximum number of messages to process per batch.
// Default is 10.
func WithProcessorMaxMessages(maxMessages int) ProcessorOption {
	return func(cfg *processorConfig) {
		if maxMessages < 1 {
			maxMessages = 10
		}
		cfg.maxMessages = maxMessages
	}
}

// WithProcessorAutoOffsetReset sets the default auto offset reset policy for the processor.
// Default is `earliest`.
// Valid values are:
//   - `earliest`: start from the earliest available offset when no committed offset exists
//   - `latest`: start from the latest offset when no committed offset exists
func WithProcessorAutoOffsetReset(v autoOffsetReset) ProcessorOption {
	v.validate()
	return func(cfg *processorConfig) {
		cfg.autoOffsetReset = v
	}
}

func newProcessor(
	reader *Reader,
	tel TelemetryProvider,
	handler ProcessFunc,
	readTimeout time.Duration,
	maxMessages int,
	autoOffsetReset autoOffsetReset,
) *Processor {
	return &Processor{
		reader:                 reader,
		tel:                    tel,
		handler:                handler,
		defaultReadTimeout:     readTimeout,
		defaultMaxMessages:     maxMessages,
		defaultAutoOffsetReset: autoOffsetReset,
	}
}

// Close releases the underlying reader resources.
func (p *Processor) Close(ctx context.Context) error {
	return p.reader.Close(ctx)
}

// ProcessNext processes the next batch of messages from Kafka.
// It reads messages, processes each one with the configured handler,
// and commits offsets only after all messages are successfully processed.
//
// The number of messages and read timeout are configured when creating the Processor
// using WithProcessorMaxMessages and WithProcessorReadTimeout.
//
// Returns the number of messages successfully processed and any error encountered.
func (p *Processor) ProcessNext(ctx context.Context) (int, error) {
	msgs, commit, err := p.reader.Read(ctx, p.defaultMaxMessages, p.defaultReadTimeout)
	if err != nil {
		return 0, fmt.Errorf("read messages: %w", err)
	}

	processedCount := 0
	for i := range msgs {
		// Check if context has been canceled before processing next message
		if err := ctx.Err(); err != nil {
			return processedCount, fmt.Errorf("context canceled: %w", err)
		}
		// Extract trace context from message headers to continue the distributed trace
		msgCtx := msgs[i].ExtractTraceContext(ctx)
		msgCtx, span := startProcessingSpan(
			msgCtx,
			p.tel.Tracer(),
			p.reader.topic,
			p.reader.consumerGroup,
			//nolint:gosec // Partition is a Kafka partition ID, non-negative and defined by Kafka as int32
			int32(msgs[i].Partition),
			msgs[i].Offset,
		)

		// Call user's processing function
		processErr := p.handler(msgCtx, &msgs[i])

		// Set span status based on processing result
		if processErr != nil {
			span.RecordError(processErr)
			span.SetStatus(codes.Error, processErr.Error())
			span.End()
			return processedCount, fmt.Errorf(
				"process message (partition=%d, offset=%d): %w",
				msgs[i].Partition,
				msgs[i].Offset,
				processErr,
			)
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
