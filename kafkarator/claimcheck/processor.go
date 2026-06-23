package claimcheck

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"

	kafkarator "github.com/hafslundkraft/golib/kafkarator"
)

// deserializer decodes Confluent Avro wire-format bytes into a Go value.
// Satisfied by *kafkarator.AvroDeserializer (via Connection.Deserializer()).
type deserializer interface {
	Deserialize(ctx context.Context, topic string, value []byte, out any) error
}

// Handler is the user-supplied callback for each claim-check message.
// Return a non-nil error to stop processing and prevent offset commit.
type Handler func(ctx context.Context, msg *Message) error

// Processor wraps a kafkarator.Processor and presents each Kafka message as a
// *Message with lazy S3 access. Default MaxMessages is 1 because each
// envelope triggers a full S3 + Parquet fetch.
type Processor struct {
	processor *kafkarator.Processor
}

// ProcessorOption configures NewProcessor.
type ProcessorOption func(*processorConfig)

// processorConfig holds configuration for NewProcessor.
type processorConfig struct {
	s3             S3Reader
	bucketResolver BucketResolver
	tracer         trace.Tracer
	kafkaOpts      []kafkarator.ProcessorOption
}

// WithProcessorS3Client sets a fixed S3 reader client for the processor. Use this
// in tests with FakeS3Client, or when managing production clients manually.
// If omitted, a production client is constructed automatically from
// HAPPI_SYSTEM_NAME, HAPPI_ENV, and HAPPI_IDP_ISSUER_URL.
func WithProcessorS3Client(s3 S3Reader) ProcessorOption {
	return func(c *processorConfig) { c.s3 = s3 }
}

// WithProcessorBucketResolver overrides the default topic→bucket naming convention
// used when constructing a production S3 client. Must match the [WithWriterBucketResolver]
// used on the corresponding writer; mismatches result in the wrong IAM role being assumed.
func WithProcessorBucketResolver(fn BucketResolver) ProcessorOption {
	return func(c *processorConfig) { c.bucketResolver = fn }
}

// WithProcessorMaxMessages sets the maximum number of Kafka messages received
// (and handler calls made) per ProcessNext call. Defaults to 1 because each
// message triggers an S3 fetch.
func WithProcessorMaxMessages(n int) ProcessorOption {
	return func(c *processorConfig) {
		c.kafkaOpts = append(c.kafkaOpts, kafkarator.WithProcessorMaxMessages(n))
	}
}

// WithProcessorReadTimeout sets how long ProcessNext waits for new messages before
// returning. Defaults to the kafkarator processor default (10 s).
func WithProcessorReadTimeout(d time.Duration) ProcessorOption {
	return func(c *processorConfig) {
		c.kafkaOpts = append(c.kafkaOpts, kafkarator.WithProcessorReadTimeout(d))
	}
}

// WithProcessorTracer sets the OpenTelemetry tracer used to instrument S3 payload fetches.
// Defaults to the tracer from the kafkarator.Connection's TelemetryProvider.
func WithProcessorTracer(t trace.Tracer) ProcessorOption {
	return func(c *processorConfig) { c.tracer = t }
}

// WithProcessorAutoOffsetReset sets the auto-offset-reset policy used when no
// committed offset exists for the consumer group.
func WithProcessorAutoOffsetReset(v kafkarator.AutoOffsetReset) ProcessorOption {
	return func(c *processorConfig) {
		c.kafkaOpts = append(c.kafkaOpts, kafkarator.WithProcessorAutoOffsetReset(v))
	}
}

// NewProcessor creates a Processor from a kafkarator.Connection. If no
// [WithProcessorS3Client] option is provided, a production S3 client is constructed
// automatically from HAPPI_SYSTEM_NAME, HAPPI_ENV, and HAPPI_IDP_ISSUER_URL.
//
// Options:
//   - [WithProcessorS3Client] — inject a custom/test S3 reader (e.g. [FakeS3Client])
//   - [WithProcessorMaxMessages] — max Kafka messages received (and handler calls made) per ProcessNext call (default 1)
//   - [WithProcessorReadTimeout] — max time ProcessNext blocks waiting for a message before returning (0, nil)
//   - [WithProcessorAutoOffsetReset] — offset reset policy when no committed offset exists
func NewProcessor(
	conn *kafkarator.Connection,
	topic string,
	handler Handler,
	opts ...ProcessorOption,
) (*Processor, error) {
	cfg := &processorConfig{}
	for _, o := range opts {
		o(cfg)
	}

	bucketResolver := cfg.bucketResolver
	if bucketResolver == nil {
		bucketResolver = DefaultBucketResolver
	}

	connCfg := conn.Config()

	// The S3 reader is resolved per-envelope, keyed by the producing system named
	// in the envelope, because the bucket is owned by that system and the assumed
	// IAM role must be scoped to it. defaultSystem is the consumer's own system,
	// used as a fallback for legacy envelopes that predate the system field.
	var s3Factory s3ReaderFactory
	if cfg.s3 != nil {
		fixed := cfg.s3
		s3Factory = func(_, _ string) (S3Reader, error) { return fixed, nil }
	} else {
		exchanger, err := newTokenExchanger()
		if err != nil {
			return nil, fmt.Errorf("claimcheck: init token exchanger: %w", err)
		}
		s3Factory = defaultS3ReaderFactory(exchanger, connCfg.Env)
	}

	tracer := cfg.tracer
	if tracer == nil {
		tracer = conn.Tracer()
	}

	res := newResolver(
		s3Factory,
		connCfg.SystemName,
		&avroDeserializer{de: conn.Deserializer()},
		tracer,
		bucketResolver,
	)

	// Default to maxMessages=1 — each envelope is a heavyweight S3 fetch.
	kafkaOpts := append(
		[]kafkarator.ProcessorOption{kafkarator.WithProcessorMaxMessages(1)},
		cfg.kafkaOpts...,
	)

	inner := func(ctx context.Context, msg *kafkarator.Message) error {
		return handler(ctx, &Message{
			Topic:     msg.Topic,
			Key:       msg.Key,
			value:     msg.Value,
			Headers:   msg.Headers,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			resolver:  res,
		})
	}

	proc, err := conn.Processor(topic, inner, kafkaOpts...)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: create processor: %w", err)
	}
	return &Processor{processor: proc}, nil
}

// ProcessNext processes the next batch of claim-check messages.
func (p *Processor) ProcessNext(ctx context.Context) (int, error) {
	n, err := p.processor.ProcessNext(ctx)
	if err != nil {
		return n, fmt.Errorf("claimcheck: process next: %w", err)
	}
	return n, nil
}

// Close releases the underlying consumer resources.
func (p *Processor) Close(ctx context.Context) error {
	if err := p.processor.Close(ctx); err != nil {
		return fmt.Errorf("claimcheck: close processor: %w", err)
	}
	return nil
}

type avroDeserializer struct {
	de deserializer
}

func (a *avroDeserializer) DeserializeEnvelope(ctx context.Context, topic string, data []byte) (*Envelope, error) {
	var env Envelope
	if err := a.de.Deserialize(ctx, topic, data, &env); err != nil {
		return nil, fmt.Errorf("claimcheck: deserialize envelope: %w", err)
	}
	return &env, nil
}
