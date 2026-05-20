package claimcheck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"

	parquet "github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Message wraps a raw Kafka message value and provides lazy access
// to the claim-check payload stored in S3.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Partition int
	Offset    int64
	resolver  *resolver
}

// IsTombstone returns true when the Kafka message has no value (null payload).
func (m *Message) IsTombstone() bool {
	return m.Value == nil
}

// PeekEnvelope decodes the envelope without fetching the payload from S3.
// Returns nil if the message is a tombstone.
func (m *Message) PeekEnvelope(ctx context.Context) (*Envelope, error) {
	if m.IsTombstone() {
		return nil, nil
	}
	return m.resolver.peekEnvelope(ctx, m.Topic, m.Value)
}

// Payload decodes the envelope and returns a PayloadReader backed by the S3
// object. The caller is responsible for calling Close when done.
//
// This is the low-level escape hatch for callers that need direct access to
// the raw Parquet bytes — for example to open the file with a Parquet library:
//
//	pr, err := msg.Payload(ctx)
//	if err != nil { ... }
//	defer pr.Close()
//	f, err := parquet.OpenFile(pr, pr.Size())
//
// Returns nil if the message is a tombstone.
func (m *Message) Payload(ctx context.Context) (*PayloadReader, error) {
	if m.IsTombstone() {
		return nil, nil
	}
	return m.resolver.fetchPayload(ctx, m.Topic, m.Value)
}

// Records yields each record in the payload decoded into a T. T must be a
// struct whose exported fields carry `parquet:"..."` tags matching the Parquet
// column names.
//
// Go does not allow generic methods, so this is a package-level function:
//
//	for row, err := range claimcheck.Records[SensorRow](ctx, msg) { ... }
//
// Returns an empty iterator for tombstone messages.
func Records[T any](ctx context.Context, m *Message) iter.Seq2[T, error] {
	var zero T
	if m.IsTombstone() {
		return func(yield func(T, error) bool) {}
	}
	return func(yield func(T, error) bool) {
		env, err := m.PeekEnvelope(ctx)
		if err != nil {
			yield(zero, err)
			return
		}
		ctx, span := m.resolver.tracer.Start(ctx, "claim_check resolve "+m.Topic,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("messaging.destination.name", m.Topic),
				attribute.String("claim_check.batch_id", env.BatchID),
				attribute.Int64("claim_check.record_count", env.RecordCount),
				attribute.Int64("claim_check.byte_size", env.ByteSize),
				attribute.String("claim_check.storage_uri", env.StorageURI),
			),
		)
		defer span.End()

		pr, err := m.resolver.fetchPayloadFromEnvelope(ctx, m.Topic, env)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			yield(zero, err)
			return
		}
		defer pr.Close() //nolint:errcheck // Close on PayloadReader is a no-op

		r := parquet.NewGenericReader[T](pr)
		defer r.Close() //nolint:errcheck // parquet.GenericReader.Close flushes nothing on read

		buf := make([]T, 64)
		for {
			n, readErr := r.Read(buf)
			for i := range n {
				if !yield(buf[i], nil) {
					return
				}
			}
			if errors.Is(readErr, io.EOF) {
				return
			}
			if readErr != nil {
				yield(zero, fmt.Errorf("claimcheck: read typed rows: %w", readErr))
				return
			}
		}
	}
}
