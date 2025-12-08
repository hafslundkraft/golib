package kafkarator

import (
	"context"
	"fmt"
	"time"

	"github.com/hafslundkraft/golib/telemetry"
	"github.com/segmentio/kafka-go"
)

const (
	meterProducedMessages = "messages_produced_total"
	meterConsumedMessages = "kafka_messages_consumed"
	gaugeLag              = "kafka_message_lag"
)

// New creates and returns a new connection.
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
	tel    *telemetry.Provider
}

func (c *connection) Test(ctx context.Context) error {
	if err := testConnection(ctx, c.config.Brokers, c.dialer); err != nil {
		return fmt.Errorf("kafkarator test brokers: %w", err)
	}

	return nil
}

func (c *connection) Writer(topic string) (WriteCloser, error) {
	producedMessagesCounter, err := c.tel.Meter().Int64Counter(meterProducedMessages)
	if err != nil {
		return nil, fmt.Errorf("kafkarator msgCounter produced messages: %w", err)
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		Dialer:  c.dialer,
	})

	return newWriteCloser(w, producedMessagesCounter, c.tel), nil
}

func (c *connection) Reader(topic, consumerGroup string) (ReadCloser, error) {
	m := c.tel.Meter()
	lagGauge, err := m.Int64Gauge(gaugeLag)
	if err != nil {
		return nil, fmt.Errorf("while creating lag gauge: %w", err)
	}

	consumedMessagesCounter, err := c.tel.Meter().Int64Counter(meterConsumedMessages)
	if err != nil {
		return nil, fmt.Errorf("kafkarator msgCounter consumed messages: %w", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		GroupID: consumerGroup,
		Dialer:  c.dialer,
	})

	return newReadCloser(reader, consumedMessagesCounter, lagGauge, c.tel), nil
}

func (c *connection) ChannelReader(ctx context.Context, topic, consumerGroup string) (<-chan Message, error) {
	rc, err := c.Reader(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("creating readCloser: %w", err)
	}

	outgoing := make(chan Message)

	go func() {
		defer rc.Close(ctx)
		defer close(outgoing)
		for {
			messages, commiter, err := rc.Read(ctx, 1, 10*time.Second)
			if err != nil {
				return
			}
			for _, msg := range messages {
				outgoing <- msg
			}
			if err := commiter(ctx); err != nil {
				return
			}
		}
	}()

	return outgoing, nil
}
