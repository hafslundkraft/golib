package kafkarator

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Message is a message that has been read off of a topic. It is more or less identical to the struct that is
// implemented by the underlying kafka library. We choose to expose our own type in order to insulate the consumer from
// such implementation details.
type Message struct {
	// Topic indicates which topic this message was consumed from.
	Topic string

	// Partition is the partition of the topic that the message came from.
	Partition int

	// Offset is the offset, or "address", of the message on the partition.
	Offset int64

	// Key is the key of the message.
	Key []byte

	// Value is the actual payload of the message. This is what you want to unmarshal!
	Value []byte

	Decoded any

	// Headers are keys value header pairs associated with the message.
	Headers map[string][]byte
}

// ExtractTraceContext extracts the OpenTelemetry trace context from the message headers
// and returns a new context with the extracted trace information. This allows consumers
// to continue the trace that was started by the producer.
//
// Example usage:
//
//	for msg := range messageChan {
//	    ctx := msg.ExtractTraceContext(ctx)
//	    // Use ctx for downstream operations to continue the trace
//	    processMessage(ctx, msg.Value)
//	}
func (m *Message) ExtractTraceContext(ctx context.Context) context.Context {
	// Convert headers map to MapCarrier
	carrier := propagation.MapCarrier{}
	for k, v := range m.Headers {
		carrier[k] = string(v)
	}

	// Extract trace context from headers and create new context
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
