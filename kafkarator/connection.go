package kafkarator

import (
	"context"
	"fmt"
	"github.com/hafslundkraft/golib/telemetry"

	"github.com/segmentio/kafka-go"
)

// New returns a new connection to the Kafka service.
func New(config Config, tel *telemetry.Provider) (Connection, error) {
	d, err := dialer(config)
	if err != nil {
		return nil, err
	}

	c := &connection{
		dialer: d,
		config: config,
		tel:    tel,
	}

	return c, nil
}

type connection struct {
	dialer *kafka.Dialer
	config Config

	tel *telemetry.Provider
}

func (c *connection) Test(ctx context.Context) error {
	if err := testConnection(ctx, c.config.Brokers, c.dialer); err != nil {
		return fmt.Errorf("kafkarator test brokers: %w", err)
	}

	return nil
}

func (c *connection) Producer(topic string) (Producer, error) {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		Dialer:  c.dialer,
	})

	p := &producer{
		writer: w,
		tel:    c.tel,
	}

	return p, nil
}

func (c *connection) Consumer(topic, consumerGroup string) (Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		GroupID: consumerGroup,
		Dialer:  c.dialer,
	})

	return &consumer{reader: reader}, nil
}
