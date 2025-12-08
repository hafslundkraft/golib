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

// New creates and returns a new Connection.
func New(config Config, tel *telemetry.Provider) (*Connection, error) {
	d, err := dialer(config)
	if err != nil {
		return nil, err
	}

	c := &Connection{
		dialer: d,
		config: config,
		tel:    tel,
	}

	return c, nil
}

// Connection represents a Connection to a Kafka service. Connection (currently) only supports
// message consumption via consumer group, so the group to use must be supplied. This means
// that multiple copies of the service using this library can be started simultaneously, and Kafka
// will automatically balance consumption between the consumers, i.e. the service can be scaled
// horizontally. Of course, this only makes sense if the topic has more than one partition.
//
// Two modes of reading are supported: ChannelReader and Reader. The former exposes a channel
// that emits messages, while the latter exposes a reader. They support two different
// use-cases where ChannelReader is best for low volume scenarios, while Reader is best for
// high volume scenarios and/or situations where the client needs to control exactly how and
// when high watermark offsets are committed.
//
// For writing, a writer is exposed. It supported writing messages, one at a time.
type Connection struct {
	dialer *kafka.Dialer
	config Config
	tel    *telemetry.Provider
}

// Test tests whether a Connection to Kafka has been established. It is designed to be called early by the client
// application so that apps can fail early if something is wrong with the Connection.
func (c *Connection) Test(ctx context.Context) error {
	if err := testConnection(ctx, c.config.Brokers, c.dialer); err != nil {
		return fmt.Errorf("kafkarator test brokers: %w", err)
	}

	return nil
}

// Writer returns a writer for writing messages to Kafka.
func (c *Connection) Writer(topic string) (*Writer, error) {
	producedMessagesCounter, err := c.tel.Meter().Int64Counter(meterProducedMessages)
	if err != nil {
		return nil, fmt.Errorf("kafkarator msgCounter produced messages: %w", err)
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		Dialer:  c.dialer,
	})

	return newWriter(w, producedMessagesCounter, c.tel), nil
}

// Reader returns a reader that is used to fetch messages from Kafka.
func (c *Connection) Reader(topic, consumerGroup string) (*Reader, error) {
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

	return newReader(reader, consumedMessagesCounter, lagGauge, c.tel), nil
}

// ChannelReader returns a channel that emits messages from the given Kafka topic.
//
// If an internal error is raised, the error will be logged, and the channel will be closed.
//
// The high watermark offset is automatically committed for each message. This potentially has significant
// performance consequences. Also, it sacrifices control, for instance the client's handling of a message
// might fail its offset is committed. Please use Reader if you are concerned about performance, or you
// want to explicitly commit offsets.
func (c *Connection) ChannelReader(ctx context.Context, topic, consumerGroup string) (<-chan Message, error) {
	rc, err := c.Reader(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("creating Reader: %w", err)
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
