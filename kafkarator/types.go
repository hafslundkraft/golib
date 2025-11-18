package kafkarator

import "context"

// Connection represents a connection to the Kafka service.
type Connection interface {
	// Test tests whether a connection to Kafka has been established. It is designed to be called early by the client
	// application so that apps can fail early if something is wrong with the connection.
	Test(ctx context.Context) error
}

// Producer is able to write messaged to a Kafka topic.
type Producer interface {
	Connection

	// Produce writes the given bytes as a message to Kafka with the given headers.
	Produce(ctx context.Context, msg []byte, headers map[string][]byte) error
}

// Consumer is able to read messages from a Kafka topic.
type Consumer interface {
	Connection

	// Consume returns a channel of Messages that have been read off of the given topic. A consumer group must also be
	// provided. This means that the progress of this consumer is automatically tracked.
	Consume(ctx context.Context, topic, consumerGroup string) (<-chan Message, error)
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
