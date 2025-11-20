package kafkarator

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hafslundkraft/golib/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
)

const (
	meterProducedMessages = "messages_produced_total"
	meterConsumedMessages = "kafka_messages_consumed"
	gaugeLagTemplate      = "kafka_lag_partition_%d"
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

func (c *connection) Writer(ctx context.Context, topic string) (chan<- MessageAndContext, error) {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		Dialer:  c.dialer,
	})

	producedMessagesCounter, err := c.tel.Meter().Int64Counter(meterProducedMessages)
	if err != nil {
		return nil, fmt.Errorf("kafkarator msgCounter produced messages: %w", err)
	}

	incomingMessages := make(chan MessageAndContext)

	go func() {
		defer func() {
			if err := w.Close(); err != nil {
				c.logger.ErrorContext(ctx, fmt.Sprintf("failed to close writer %v", err))
			}
		}()
		defer close(incomingMessages)

		select {
		case msg := <-incomingMessages:
			headers := c.injectTraceContext(msg.Context, msg.Message.Headers)
			if err := w.WriteMessages(ctx, kafkaMessage(msg.Message.Value, headers)); err != nil {
				c.logger.ErrorContext(ctx, fmt.Sprintf("while writing messages to Kafka %v", err))
			}
			producedMessagesCounter.Add(ctx, 1)
		case <-ctx.Done():
			c.logger.InfoContext(ctx, "writer exiting")
			return
		}
	}()

	return incomingMessages, nil
}

func (c *connection) Reader(ctx context.Context, topic, consumerGroup string) (<-chan Message, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: c.config.Brokers,
		Topic:   topic,
		GroupID: consumerGroup,
		Dialer:  c.dialer,
	})

	partitionCount, err := c.topicPartitions(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafkarator topic partitions: %w", err)
	}

	m := c.tel.Meter()
	lagGauges := map[int]metric.Int64Gauge{}
	for i := 0; i < partitionCount; i++ {
		g, err := m.Int64Gauge(fmt.Sprintf(gaugeLagTemplate, i))
		if err != nil {
			return nil, fmt.Errorf("kafkarator msgCounter gauge %d: %w", i, err)
		}
		lagGauges[i] = g
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
				lagGauges[msg.Partition].Record(ctx, lag)
				consumedMessagesCounter.Add(ctx, int64(1))
			case <-ctx.Done():
				// Context canceled while trying to send message
				return
			}
		}
	}()

	return outgoing, nil
}

func (c *connection) topicPartitions(ctx context.Context, topic string) (int, error) {
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

// injectTraceContext extracts the trace context from the current span and injects it into the headers
func (c *connection) injectTraceContext(ctx context.Context, headers map[string][]byte) map[string][]byte {
	// Create a new map to avoid modifying the original
	propagatedHeaders := make(map[string][]byte, len(headers))
	for k, v := range headers {
		propagatedHeaders[k] = v
	}

	// Use a MapCarrier to inject trace context
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	// Add trace context headers to the Kafka headers
	for k, v := range carrier {
		propagatedHeaders[k] = []byte(v)
	}

	return propagatedHeaders
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
