package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const readToEndTimeout = 60 * time.Second

// createTopic creates a topic with the given number of partitions, so a test
// can leave some of them empty.
func createTopic(t *testing.T, partitions int) string {
	t.Helper()
	topic := fmt.Sprintf("kafkarator-it-%s", generateID())

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": broker})
	require.NoError(t, err)
	defer admin.Close()

	results, err := admin.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, kafka.ErrNoError, results[0].Error.Code(), results[0].Error.String())
	return topic
}

// writeMessages writes n messages with keys and values prefix0, prefix1, ...,
// and returns the values.
func writeMessages(t *testing.T, conn *Connection, topic, prefix string, n int) [][]byte {
	t.Helper()
	ctx := context.Background()
	writer, err := conn.Writer()
	require.NoError(t, err)
	defer writer.Close(ctx)

	values := make([][]byte, 0, n)
	for i := range n {
		value := fmt.Appendf(nil, "%s%d", prefix, i)
		require.NoError(t, writer.Write(ctx, &Message{Topic: topic, Key: value, Value: value}))
		values = append(values, value)
	}
	require.NoError(t, writer.Flush(ctx))
	return values
}

func values(msgs []Message) [][]byte {
	out := make([][]byte, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Value)
	}
	return out
}

func newTestConnection(t *testing.T) *Connection {
	t.Helper()
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)
	return conn
}

func readToEndOrFail(t *testing.T, conn *Connection, topic string) []Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), readToEndTimeout)
	defer cancel()

	var msgs []Message
	for msg, err := range conn.ReadToEnd(ctx, topic) {
		require.NoError(t, err)
		msgs = append(msgs, msg)
	}
	return msgs
}

// errorsOf ranges over seq and returns the errors it yielded.
func errorsOf(seq func(func(Message, error) bool)) []error {
	var errs []error
	for _, err := range seq {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func TestReadToEndReadsEveryPartition(t *testing.T) {
	topic := createTopic(t, 3)
	conn := newTestConnection(t)
	written := writeMessages(t, conn, topic, "v", 1200)

	msgs := readToEndOrFail(t, conn, topic)
	assert.ElementsMatch(t, written, values(msgs))

	partitions := map[int]bool{}
	for _, m := range msgs {
		partitions[m.Partition] = true
	}
	assert.Len(t, partitions, 3, "the messages should have been spread over every partition")
}

// One message in a topic of three partitions leaves two empty. ReadToEnd must
// count them as read to their end rather than wait for them.
func TestReadToEndCountsEmptyPartitions(t *testing.T) {
	topic := createTopic(t, 3)
	conn := newTestConnection(t)
	written := writeMessages(t, conn, topic, "v", 1)

	assert.Equal(t, written, values(readToEndOrFail(t, conn, topic)))
}

func TestReadToEndOnAnEmptyTopic(t *testing.T) {
	topic := createTopic(t, 3)
	conn := newTestConnection(t)

	assert.Empty(t, readToEndOrFail(t, conn, topic))
}

func TestReadToEndReadsFromTheBeginningEveryTime(t *testing.T) {
	topic := createTopic(t, 3)
	conn := newTestConnection(t)
	first := writeMessages(t, conn, topic, "first", 30)
	require.ElementsMatch(t, first, values(readToEndOrFail(t, conn, topic)))

	second := writeMessages(t, conn, topic, "second", 20)
	assert.ElementsMatch(t, append(first, second...), values(readToEndOrFail(t, conn, topic)))
}

func TestReadToEndAfterBreak(t *testing.T) {
	topic := createTopic(t, 1)
	conn := newTestConnection(t)
	written := writeMessages(t, conn, topic, "v", 10)

	ctx, cancel := context.WithTimeout(context.Background(), readToEndTimeout)
	defer cancel()
	var firstPart []Message
	for msg, err := range conn.ReadToEnd(ctx, topic) {
		require.NoError(t, err)
		firstPart = append(firstPart, msg)
		if len(firstPart) == 3 {
			break
		}
	}
	require.Equal(t, written[:3], values(firstPart))

	assert.Equal(t, written, values(readToEndOrFail(t, conn, topic)))
}

// Replicas of a workload share a consumer group name. Each must still read the
// whole topic, also while a group member is subscribed.
func TestReadToEndReadsTheWholeTopicForEveryReplica(t *testing.T) {
	ctx := context.Background()
	topic := createTopic(t, 3)
	conn := newTestConnection(t)
	written := writeMessages(t, conn, topic, "v", 30)

	member, err := conn.Reader(topic)
	require.NoError(t, err)
	defer member.Close(ctx)
	waitForConsumerAssignment(t, member, 30*time.Second)

	assert.ElementsMatch(t, written, values(readToEndOrFail(t, conn, topic)))
	assert.ElementsMatch(t, written, values(readToEndOrFail(t, newTestConnection(t), topic)))
}

// ReadToEnd commits nothing, so a reader in the workload's consumer group still
// starts from the beginning.
func TestReadToEndCommitsNothing(t *testing.T) {
	ctx := context.Background()
	topic := createTopic(t, 1)
	conn := newTestConnection(t)
	written := writeMessages(t, conn, topic, "v", 10)

	require.Len(t, readToEndOrFail(t, conn, topic), 10)

	reader, err := conn.Reader(topic)
	require.NoError(t, err)
	defer reader.Close(ctx)
	msgs, _, err := retryRead(ctx, reader, 10, readToEndTimeout)
	require.NoError(t, err)
	assert.Equal(t, written, values(msgs))
}

// The messages of an open transaction are past the last stable offset, which
// read_committed, librdkafka's default, cannot read past. ReadToEnd must stop
// there rather than wait for the high watermark.
func TestReadToEndStopsAtAnOpenTransaction(t *testing.T) {
	ctx := context.Background()
	topic := createTopic(t, 1)
	conn := newTestConnection(t)
	committed := writeMessages(t, conn, topic, "v", 5)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"transactional.id":  "kafkarator-it-" + generateID(),
	})
	require.NoError(t, err)
	defer producer.Close()
	txCtx, cancel := context.WithTimeout(ctx, readToEndTimeout)
	defer cancel()
	require.NoError(t, producer.InitTransactions(txCtx))
	require.NoError(t, producer.BeginTransaction())
	pending := [][]byte{[]byte("tx0"), []byte("tx1")}
	delivered := make(chan kafka.Event, len(pending))
	for _, v := range pending {
		require.NoError(t, producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          v,
		}, delivered))
	}
	for range pending {
		m := (<-delivered).(*kafka.Message)
		require.NoError(t, m.TopicPartition.Error)
	}

	assert.Equal(
		t,
		committed,
		values(readToEndOrFail(t, conn, topic)),
		"an open transaction should not hold up the read",
	)
	assert.Equal(
		t,
		committed,
		values(readToEndOrFail(t, conn, topic)),
		"a later read should not wait for the transaction either",
	)

	require.NoError(t, producer.CommitTransaction(txCtx))
	assert.Equal(
		t,
		append(committed, pending...),
		values(readToEndOrFail(t, conn, topic)),
		"the transaction should be read once committed",
	)
}

func TestReadToEndYieldsAnErrorWhenTheContextEnds(t *testing.T) {
	topic := createTopic(t, 3)
	conn := newTestConnection(t)
	writeMessages(t, conn, topic, "v", 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	errs := errorsOf(conn.ReadToEnd(ctx, topic))
	require.Len(t, errs, 1, "the error should be yielded once")
	assert.ErrorIs(t, errs[0], context.Canceled)
}

func TestReadToEndYieldsAnErrorWhenTheContextEndsWhileReading(t *testing.T) {
	topic := createTopic(t, 1)
	conn := newTestConnection(t)
	writeMessages(t, conn, topic, "v", 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var errs []error
	for _, err := range conn.ReadToEnd(ctx, topic) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cancel()
	}
	require.Len(t, errs, 1, "the error should be yielded once")
	assert.ErrorIs(t, errs[0], context.Canceled)
}

func TestReadToEndOnAMissingTopic(t *testing.T) {
	conn := newTestConnection(t)

	ctx, cancel := context.WithTimeout(context.Background(), readToEndTimeout)
	defer cancel()
	errs := errorsOf(conn.ReadToEnd(ctx, "kafkarator-it-missing-"+generateID()))
	require.Len(t, errs, 1)
	assert.NotErrorIs(t, errs[0], context.DeadlineExceeded, "a missing topic should fail fast, not wait for ctx")
}

func TestReadToEndWithoutAConsumerGroupName(t *testing.T) {
	cfg := config
	cfg.WorkloadName = ""
	conn, err := NewConnection(&cfg, newMockTelemetry())
	require.NoError(t, err)

	errs := errorsOf(conn.ReadToEnd(context.Background(), "any-topic"))
	require.Len(t, errs, 1)
	assert.ErrorContains(t, errs[0], "consumer group name")
}

func TestRetryTransient(t *testing.T) {
	transient := kafka.NewError(kafka.ErrTransport, "broker down", false)
	permanent := kafka.NewError(kafka.ErrTopicAuthorizationFailed, "denied", false)

	t.Run("retries transient errors", func(t *testing.T) {
		calls := 0
		err := retryTransient(context.Background(), func(int) error {
			calls++
			if calls == 1 {
				return transient
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 2, calls)
	})

	t.Run("returns other errors at once", func(t *testing.T) {
		calls := 0
		err := retryTransient(context.Background(), func(int) error {
			calls++
			return permanent
		})
		require.ErrorIs(t, err, permanent)
		assert.Equal(t, 1, calls)
	})

	t.Run("stops when ctx ends", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := retryTransient(ctx, func(timeoutMs int) error {
			assert.LessOrEqual(t, timeoutMs, 50, "a try should not outlast ctx")
			return transient
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.ErrorIs(t, err, transient, "the last Kafka error should be kept")
	})
}

func TestIsTransient(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{fmt.Errorf("poll error: %w", kafka.NewError(kafka.ErrTransport, "broker down", false)), true},
		{fmt.Errorf("poll error: %w", kafka.NewError(kafka.ErrAllBrokersDown, "all down", false)), true},
		{fmt.Errorf("watermarks: %w", kafka.NewError(kafka.ErrOffsetNotAvailable, "leader election", false)), true},
		{fmt.Errorf("watermarks: %w", kafka.NewError(kafka.ErrFencedLeaderEpoch, "stale leader", false)), true},
		{fmt.Errorf("poll error: %w", kafka.NewError(kafka.ErrTopicAuthorizationFailed, "denied", false)), false},
		{fmt.Errorf("poll error: %w", kafka.NewError(kafka.ErrUnknownTopicOrPart, "no topic", false)), false},
		{errors.New("not a Kafka error"), false},
		{nil, false},
	} {
		assert.Equal(t, tc.want, isTransient(tc.err), "%v", tc.err)
	}
}
