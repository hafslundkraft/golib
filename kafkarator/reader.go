package kafkarator

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hafslundkraft/golib/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func newReader(
	c *kafka.Consumer,
	rmc metric.Int64Counter,
	lagGauge metric.Int64Gauge,
	tel *telemetry.Provider,
) *Reader {
	return &Reader{
		consumer:            c,
		readMessagesCounter: rmc,
		lagGauge:            lagGauge,
		tel:                 tel,
	}
}

// Reader provides an interface for reading messages from a Kafka topic, as well
// as closing it when the client is done reading. Additionally, the act of fetching
// messages and committing them ("committing" == registering the largest offset per
// partition as the high watermark within the consumer group) is split giving the
// client total control and responsibility.
type Reader struct {
	consumer            *kafka.Consumer
	readMessagesCounter metric.Int64Counter
	lagGauge            metric.Int64Gauge
	tel                 *telemetry.Provider
	closed              bool
}

// Close closes releases the underlying infrastructure, and renders this instance unusable.
func (r *Reader) Close(ctx context.Context) error {
	if r.closed {
		return nil // It's ok to close multiple times.
	}

	if err := r.consumer.Close(); err != nil {
		return fmt.Errorf("error closing reader: %w", err)
	}
	r.closed = true
	return nil
}

// Read returns a slice of messages at most maxMessages long. If the duration maxWait
// is exceeded before maxMessages have been fetched from the topic, the func will
// return with as many messages in the list as were fetched before timeout.
//
// commiter can be used to commit the high watermark per partition to the consumer group. It
// is up to the client if and when commiter is invoked. Committing often can affect
// performance considerably in a high-volume scenario, so the client could for example
// employ a strategy where commiter is only invoked every N iterations.
func (rc *Reader) Read(
	ctx context.Context,
	maxMessages int,
	maxWait time.Duration,
) ([]Message, func(ctx context.Context) error, error) {

	ctx, span := rc.tel.Tracer().Start(ctx, "kafkarator.Reader.Read")
	defer span.End()

	deadline := time.Now().Add(maxWait)
	msgs := make([]Message, 0, maxMessages)

	// Track last seen offsets per partition
	latestOffsets := map[int32]kafka.Offset{}

	commiter := func(ctx context.Context) error {
		for partition, off := range latestOffsets {
			_, err := rc.consumer.CommitOffsets([]kafka.TopicPartition{
				{
					Topic:     nil, // Use last polled topic
					Partition: partition,
					Offset:    off + 1,
				},
			})
			if err != nil {
				return fmt.Errorf("commit failed: %w", err)
			}
		}
		return nil
	}

	for len(msgs) < maxMessages {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		ev := rc.consumer.Poll(int(remaining.Milliseconds()))
		if ev == nil {
			break
		}

		switch e := ev.(type) {

		case *kafka.Message:
			msgs = append(msgs, Message{
				Topic:     *e.TopicPartition.Topic,
				Partition: int(e.TopicPartition.Partition),
				Offset:    int64(e.TopicPartition.Offset),
				Key:       e.Key,
				Value:     e.Value,
				Headers:   convertHeaders(e.Headers),
			})

			latestOffsets[e.TopicPartition.Partition] = e.TopicPartition.Offset
			rc.readMessagesCounter.Add(ctx, 1)

		case kafka.Error:
			if e.IsTimeout() {
				return msgs, commiter, nil
			}
			span.RecordError(e)
			return msgs, commiter, fmt.Errorf("poll error: %w", e)

		default:
			// ignore rebalance events etc.
		}
	}

	span.SetAttributes(attribute.Int("message_count", len(msgs)))

	return msgs, commiter, nil
}

func convertHeaders(hdrs []kafka.Header) map[string][]byte {
	m := make(map[string][]byte, len(hdrs))
	for _, h := range hdrs {
		m[h.Key] = h.Value
	}
	return m
}
