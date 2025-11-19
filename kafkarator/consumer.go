package kafkarator

import (
	"context"

	"github.com/hafslundkraft/golib/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/metric"
)

const (
	meterConsumedMessages = "kafka_messages_consumed"
	gaugeLagTemplate      = "kafka_lag_partition_%d"
)

type consumer struct {
	reader     *kafka.Reader
	tel        *telemetry.Provider
	msgCounter metric.Int64Counter
	lagGauges  map[int]metric.Int64Gauge
}

func (c *consumer) Consume(ctx context.Context) (<-chan Message, error) {
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
				lag := msg.HighWaterMark - msg.Offset - 1
				c.lagGauges[msg.Partition].Record(ctx, lag)
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
