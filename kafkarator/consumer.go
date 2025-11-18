package kafkarator

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// NewConsumer returns a service able to consume messages from Kafka.
func NewConsumer(topic, consumerGroup string, config Config) (Consumer, error) {
	d, err := dialer(config)
	if err != nil {
		return nil, err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: config.Brokers,
		Topic:   topic,
		GroupID: consumerGroup,
		Dialer:  d,
	})

	c := &consumer{
		reader:  reader,
		dialer:  d,
		brokers: config.Brokers,
		topic:   topic,
	}

	return c, nil
}

type consumer struct {
	reader  *kafka.Reader
	dialer  *kafka.Dialer
	brokers []string
	topic   string
}

func (c *consumer) Test(ctx context.Context) error {
	return testConnection(ctx, c.brokers, c.dialer)
}

func (c *consumer) Consume(ctx context.Context, topic, consumerGroup string) (<-chan Message, error) {
	ch := make(chan Message)

	go func() {
		defer close(ch)
		defer c.reader.Close()

		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				// Context canceled or error reading
				if ctx.Err() != nil {
					return
				}
				// Log error and continue (or handle differently based on your needs)
				continue
			}

			select {
			case ch <- message(&msg):
				// Message sent successfully, commit it
				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					// Context canceled during commit
					if ctx.Err() != nil {
						return
					}
					// Handle commit error (log, etc.)
				}
			case <-ctx.Done():
				// Context canceled while trying to send message
				return
			}
		}
	}()

	return ch, nil
}

func message(m *kafka.Message) Message {
	headers := make(map[string][]byte)
	for _, header := range m.Headers {
		headers[header.Key] = header.Value
	}
	return Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Headers:   headers,
	}
}
