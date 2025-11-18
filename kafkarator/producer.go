package kafkarator

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// NewProducer returns a service able to write messages to a Kafka topic.
func NewProducer(topic string, config Config) (Producer, error) {
	d, err := dialer(config)
	if err != nil {
		return nil, err
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: config.Brokers,
		Topic:   topic,
		Dialer:  d,
	})

	p := &producer{
		writer:  w,
		topic:   topic,
		dialer:  d,
		brokers: config.Brokers,
	}

	return p, err
}

type producer struct {
	writer  *kafka.Writer
	dialer  *kafka.Dialer
	brokers []string
	topic   string
}

func (p *producer) Test(ctx context.Context) error {
	return testConnection(ctx, p.brokers, p.dialer)
}

func (p *producer) Produce(ctx context.Context, msg []byte, headers map[string][]byte) error {
	if err := p.writer.WriteMessages(ctx, kafkaMessage(msg, headers)); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	return nil
}

func kafkaMessage(b []byte, headers map[string][]byte) kafka.Message {
	headerList := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		headerList = append(headerList, kafka.Header{Key: k, Value: v})
	}

	return kafka.Message{
		Value:   b,
		Headers: headerList,
	}
}
