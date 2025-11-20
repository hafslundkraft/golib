package kafkarator

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Connection represents a connection to a Kafka service.
type Connection interface {
	// Test tests whether a connection to Kafka has been established. It is designed to be called early by the client
	// application so that apps can fail early if something is wrong with the connection.
	Test(ctx context.Context) error

	// Writer returns a channel that is used to write messages to the Kafka topic.
	Writer(ctx context.Context, topic string) (chan<- MessageAndContext, error)

	// Reader returns a channel that sends out messages from Kafka.
	Reader(ctx context.Context, topic, consumerGroup string) (<-chan Message, error)

	// topicPartitions returns the number of partitions for the given topic
	topicPartitions(ctx context.Context, topic string) (int, error)
}

// Producer is able to write messaged to a Kafka topic.
type Producer interface {
	// Produce writes the given bytes as a message to Kafka with the given headers.
	Produce(ctx context.Context, msg []byte, headers map[string][]byte) error
}

// Consumer is able to read messages from a Kafka topic.
type Consumer interface {
	// Consume returns a channel of Messages that have been read off of the given topic. A consumer group must also be
	// provided. This means that the progress of this consumer is automatically tracked.
	Consume(ctx context.Context) (<-chan Message, error)
}

// NewMessageAndContext creates a message and associates it with a context.
func NewMessageAndContext(ctx context.Context, msg []byte, headers map[string][]byte) MessageAndContext {
	m := Message{
		Value:   msg,
		Headers: headers,
	}

	return MessageAndContext{
		Context: ctx,
		Message: m,
	}
}

// MessageAndContext associates a message with a context, the purpose being to be able to
// propagate trace information over channels.
type MessageAndContext struct {
	// Message is the thing going onto the Kafka topic.
	Message Message

	// Context is the context.
	Context context.Context
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
