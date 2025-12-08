package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hafslundkraft/golib/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func newReader(
	r *kafka.Reader,
	rmc metric.Int64Counter,
	lagGauge metric.Int64Gauge,
	tel *telemetry.Provider,
) *Reader {
	return &Reader{
		reader:              r,
		readMessagesCounter: rmc,
		lagGauge:            lagGauge,
		tel:                 tel,
		closed:              false,
	}
}

// Reader provides an interface for reading messages from a Kafka topic, as well
// as closing it when the client is done reading. Additionally, the act of fetching
// messages and committing them ("committing" == registering the largest offset per
// partition as the high watermark within the consumer group) is split giving the
// client total control and responsibility.
type Reader struct {
	reader              *kafka.Reader
	readMessagesCounter metric.Int64Counter
	lagGauge            metric.Int64Gauge
	tel                 *telemetry.Provider
	closed              bool
}

// Close closes releases the underlying infrastructure, and renders this instance unusable.
func (rc *Reader) Close(ctx context.Context) error {
	if rc.closed {
		return nil // It's ok to close multiple times.
	}

	if err := rc.reader.Close(); err != nil {
		return fmt.Errorf("error closing reader: %w", err)
	}
	rc.closed = true
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
) (messages []Message, commiter func(ctx context.Context) error, err error) {
	spanCtx, span := rc.tel.Tracer().Start(ctx, "kafkarator.Reader.Read")
	defer span.End()

	messages = make([]Message, 0, maxMessages)
	deadline := time.Now().Add(maxWait)

	partitionOffsets := map[int]int64{}
	commiter = func(ctx context.Context) error {
		topic := rc.reader.Config().Topic
		for p, o := range partitionOffsets {
			msg := kafka.Message{
				Topic:     topic,
				Partition: p,
				Offset:    o,
			}
			if err := rc.reader.CommitMessages(ctx, msg); err != nil {
				return fmt.Errorf("error committing messages: %w", err)
			}
		}
		return nil
	}

	partitionLags := map[int]int64{}

	for len(messages) < maxMessages {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		readCtx, cancel := context.WithTimeout(spanCtx, remaining)
		msg, err := rc.reader.FetchMessage(readCtx)
		cancel()

		if err != nil {
			if errors.Is(readCtx.Err(), context.DeadlineExceeded) {
				// Timeout reached, return what we have
				break
			}
			if readCtx.Err() != nil {
				// Parent context canceled
				return messages, commiter, fmt.Errorf("parent context canceled: %w", ctx.Err())
			}
			span.RecordError(err)
			return messages, commiter, fmt.Errorf("error fetching message: %w", err)
		}

		var hwm int64
		var ok bool
		if hwm, ok = partitionOffsets[msg.Partition]; !ok {
			hwm = -1
		}
		if msg.Offset > hwm {
			partitionOffsets[msg.Partition] = msg.Offset
			lag := msg.HighWaterMark - msg.Offset - 1
			partitionLags[msg.Partition] = lag
		}

		messages = append(messages, message(&msg))

		rc.readMessagesCounter.Add(spanCtx, 1)
		for p, l := range partitionLags {
			rc.lagGauge.Record(
				spanCtx,
				l,
				metric.WithAttributes(
					attribute.KeyValue{Key: "partition", Value: attribute.StringValue(fmt.Sprint(p))},
				),
			)
		}
	}

	span.SetAttributes(attribute.Int("message_count", len(messages)))

	return messages, commiter, nil
}
