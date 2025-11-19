package kafkarator

import (
	"context"
	"fmt"

	"github.com/hafslundkraft/golib/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/metric"
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

	producedMessagesCounter, err := c.tel.Meter().Int64Counter(meterProducedMessages)
	if err != nil {
		return nil, fmt.Errorf("kafkarator msgCounter produced messages: %w", err)
	}

	p := &producer{
		writer:     w,
		tel:        c.tel,
		msgCounter: producedMessagesCounter,
	}

	return p, nil
}

func (c *connection) Consumer(ctx context.Context, topic, consumerGroup string) (Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		GroupID: consumerGroup,
		Dialer:  c.dialer,
	})

	partitionCount, err := c.TopicPartitions(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafkarator topic partitions: %w", err)
	}

	m := c.tel.Meter()
	gauges := map[int]metric.Int64Gauge{}
	for i := 0; i < partitionCount; i++ {
		g, err := m.Int64Gauge(fmt.Sprintf(gaugeLagTemplate, i))
		if err != nil {
			return nil, fmt.Errorf("kafkarator msgCounter gauge %d: %w", i, err)
		}
		gauges[i] = g
	}

	consumedMessagesCounter, err := c.tel.Meter().Int64Counter(meterConsumedMessages)
	if err != nil {
		return nil, fmt.Errorf("kafkarator msgCounter consumed messages: %w", err)
	}

	return &consumer{
		reader:     reader,
		tel:        c.tel,
		msgCounter: consumedMessagesCounter,
		lagGauges:  gauges,
	}, nil
}

func (c *connection) TopicPartitions(ctx context.Context, topic string) (int, error) {
	// Connect to the first broker to get metadata
	conn, err := c.dialer.DialContext(ctx, "tcp", c.config.Brokers[0])
	if err != nil {
		return 0, fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	// Get partition information for the topic
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, fmt.Errorf("read partitions for topic %s: %w", topic, err)
	}

	return len(partitions), nil
}
