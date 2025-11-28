package kafkarator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hafslundkraft/golib/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
		logger: tel.Logger(),
	}

	return c, nil
}

type connection struct {
	dialer *kafka.Dialer
	config Config
	tel    *telemetry.Provider
	logger *slog.Logger
}

func (c *connection) Test(ctx context.Context) error {
	if err := testConnection(ctx, c.config.Brokers, c.dialer); err != nil {
		return fmt.Errorf("kafkarator test brokers: %w", err)
	}

	return nil
}

func (c *connection) Writer(topic string) (WriterCloser, error) {
	producedMessagesCounter, err := c.tel.Meter().Int64Counter(meterProducedMessages)
	if err != nil {
		return nil, fmt.Errorf("kafkarator msgCounter produced messages: %w", err)
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		Dialer:  c.dialer,
	})

	return newWriteCloser(w, producedMessagesCounter, c.logger), nil
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

	return newReadCloser(reader, consumedMessagesCounter, lagGauge, c.logger), nil
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

func (c *connection) ChannelReaderOld(ctx context.Context, topic, consumerGroup string) (<-chan Message, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		GroupID: consumerGroup,
		Dialer:  c.dialer,
	})

	m := c.tel.Meter()
	lagGauge, err := m.Int64Gauge(gaugeLag)
	if err != nil {
		return nil, fmt.Errorf("while creating lag gauge: %w", err)
	}

	consumedMessagesCounter, err := c.tel.Meter().Int64Counter(meterConsumedMessages)
	if err != nil {
		return nil, fmt.Errorf("kafkarator msgCounter consumed messages: %w", err)
	}

	outgoing := make(chan Message)

	go func() {
		defer close(outgoing)
		defer func() {
			if err := reader.Close(); err != nil {
				c.logger.ErrorContext(ctx, fmt.Sprintf("failed to close reader %v", err))
			}
		}()

		for {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				// Context canceled or error reading
				if ctx.Err() != nil {
					return
				}
				// Log error and continue (or handle differently based on your needs)
				continue
			}

			select {
			case outgoing <- message(&msg):
				// Message sent successfully, commit it
				if err := reader.CommitMessages(ctx, msg); err != nil {
					// Context canceled during commit
					if ctx.Err() != nil {
						return
					}
					// Handle commit error (log, etc.)
				}
				lag := msg.HighWaterMark - msg.Offset - 1
				lagGauge.Record(
					ctx,
					lag,
					metric.WithAttributes(
						attribute.KeyValue{Key: "partition", Value: attribute.StringValue(fmt.Sprint(msg.Partition))},
					),
				)
				consumedMessagesCounter.Add(ctx, int64(1))
			case <-ctx.Done():
				// Context canceled while trying to send message
				return
			}
		}
	}()

	return outgoing, nil
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
