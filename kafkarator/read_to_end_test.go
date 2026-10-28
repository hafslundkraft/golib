package kafkarator

import (
	"context"
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

func writeMessages(t *testing.T, conn *Connection, topic string, n int) [][]byte {
	t.Helper()
	ctx := context.Background()
	writer, err := conn.Writer()
	require.NoError(t, err)
	defer writer.Close(ctx)

	values := make([][]byte, 0, n)
	for i := range n {
		value := fmt.Appendf(nil, "v%d", i)
		require.NoError(t, writer.Write(ctx, &Message{Topic: topic, Key: fmt.Appendf(nil, "k%d", i), Value: value}))
		values = append(values, value)
	}
	require.NoError(t, writer.Flush(ctx))
	return values
}

func readToEnd(t *testing.T, conn *Connection, topic string) ([]Message, CommitFunc) {
	t.Helper()
	reader, err := conn.Reader(topic, WithPartitionEOF())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), readToEndTimeout)
	defer cancel()
	msgs, commit, err := reader.ReadToEnd(ctx)
	require.NoError(t, err)
	return msgs, commit
}

func values(msgs []Message) [][]byte {
	out := make([][]byte, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Value)
	}
	return out
}

func TestReadToEndReadsEveryPartition(t *testing.T) {
	topic := createTopic(t, 3)
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)
	written := writeMessages(t, conn, topic, 30)

	msgs, _ := readToEnd(t, conn, topic)
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
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)
	written := writeMessages(t, conn, topic, 1)

	msgs, _ := readToEnd(t, conn, topic)
	assert.Equal(t, written, values(msgs))
}

func TestReadToEndOnAnEmptyTopic(t *testing.T) {
	topic := createTopic(t, 3)
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)

	msgs, _ := readToEnd(t, conn, topic)
	assert.Empty(t, msgs)
}

// The CommitFunc covers every batch ReadToEnd read, so the next reader in the
// same consumer group starts after all of it.
func TestReadToEndCommitsEverythingRead(t *testing.T) {
	ctx := context.Background()
	topic := createTopic(t, 3)
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)
	writeMessages(t, conn, topic, 1200) // more than one batch

	reader, err := conn.Reader(topic, WithPartitionEOF())
	require.NoError(t, err)
	readCtx, cancel := context.WithTimeout(ctx, readToEndTimeout)
	defer cancel()
	msgs, commit, err := reader.ReadToEnd(readCtx)
	require.NoError(t, err)
	require.Len(t, msgs, 1200)
	require.NoError(t, commit(ctx))
	// Leave the group, so the next reader is its only member and gets every
	// partition.
	require.NoError(t, reader.Close(ctx))

	again, _ := readToEnd(t, conn, topic)
	assert.Empty(t, again, "the next reader in the group should start after what was committed")
}

func TestReadToEndNeedsPartitionEOF(t *testing.T) {
	topic := createTopic(t, 1)
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)
	reader, err := conn.Reader(topic)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	_, _, err = reader.ReadToEnd(context.Background())
	require.ErrorIs(t, err, ErrPartitionEOFNotEnabled)
}

func TestReadToEndStopsWhenTheContextEnds(t *testing.T) {
	topic := createTopic(t, 3)
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)
	reader, err := conn.Reader(topic, WithPartitionEOF())
	require.NoError(t, err)
	defer reader.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = reader.ReadToEnd(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
