package kafkarator

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Connection represents a connection to a Kafka service.
type Connection interface {
	// Test tests whether a connection to Kafka has been established. It is designed to be called early by the client
	// application so that apps can fail early if something is wrong with the connection.
	Test(ctx context.Context) error

	// Writer returns a writer for writing messages to Kafka.
	Writer(topic string) (WriterCloser, error)

	// Reader returns a channel that sends out messages from the given Kafka topic. This library (currently)
	// only supports message consumption via consumer group, so the group to use must be supplied. This means
	// that multiple copies of the service using this library can be started simultaneously, and Kafka will
	// automatically balance consumption between the consumers, i.e. the service can be scaled horizontally. Of
	// course, this only makes sense if the topic has more than one partition. Another consequence/feature is that
	// Kafka automatically tracks the progress of the worker.
	Reader(ctx context.Context, topic, consumerGroup string) (<-chan Message, error)
}

// WriterCloser provides an interface for writing messages to the Kafka topic, as well
// as closing it when the client is done writing.
type WriterCloser interface {
	// Close closes the underlying infrastructure, and renders this interface unusable for writing messages.
	Close(ctx context.Context) error

	// Write writes the given message with headers to the topic. An important side effect is
	// that if there is an OpenTelemetry tracing span associated with the context, it is extracted
	// and included in the header that is sent to Kafka.
	Write(ctx context.Context, msg []byte, headers map[string][]byte) error
}

// ReadCloser provides an interface for reading messages from a Kafka topic, as well
// as closing it when the client is done reading. Additionally, the act of fetching
// messages and committing them ("committing" == registering the largest offset per
// partition as the high watermark within the consumer group) is split giving the
// client total control and responsibility.
type ReadCloser interface {
	// Close closes releases the underlying infrastructure, and renders this instance unusable.
	Close(ctx context.Context) error

	// Read returns a slice of messages at most maxMessages long. If the duration maxWait
	// is exceeded before maxMessages have been fetched from the topic, the func will
	// return with as many messages in the list as were fetched before timeout.
	Read(ctx context.Context, maxMessages int, maxWait time.Duration) ([]Message, error)

	// Commit commits the given messages within the consumer group. Internally, the max
	// offset per partition is computed, and this offset+1 is set as high watermark.
	Commit(ctx context.Context, messages []Message) error
}

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
