package kafkarator

// Message is a message that can either be written to topic or be read off of a topic. It is more or less identical to the struct that is
// implemented by the underlying kafka library. We choose to expose our own type in order to insulate the writer/consumer from
// such implementation details.
type Message struct {
	// Topic indicates which topic this message is written to/consumed from.
	Topic string

	// Partition is the partition of the topic that the message came from. Only relevant for consuming
	Partition int

	// Offset is the offset, or "address", of the message on the partition. Only relevant for consuming
	Offset int64

	// Key is the key of the message.
	Key []byte

	// Value is the actual payload of the message. This is what you want to unmarshal when consuming
	Value []byte

	// Headers are keys value header pairs associated with the message.
	Headers map[string][]byte
}
