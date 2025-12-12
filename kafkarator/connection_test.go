package kafkarator

// import (
// 	"bytes"
// 	"context"
// 	"testing"
// 	"time"
//
// 	"github.com/hafslundkraft/golib/telemetry"
// 	"github.com/stretchr/testify/require"
// )

// func Test_connection_ChannelConsumer(t *testing.T) {
// 	ctx := context.Background()
//
// 	// Telemetry buffer so we can inspect metrics output
// 	var telemetryBuf bytes.Buffer
// 	tel, telClose := telemetry.New(
// 		ctx,
// 		"kafka-test",
// 		telemetry.WithLocal(true),
// 		telemetry.WithLocalWriter(&telemetryBuf),
// 	)
// 	defer telClose(ctx)
//
// 	topic := "test-channel-reader"
// 	closeTopic := createTopic(t, topic)
// 	defer closeTopic()
//
// 	// Create connection
// 	conn, err := New(config, tel)
// 	require.NoError(t, err)
//
// 	// Check metadata works
// 	require.NoError(t, conn.Test(ctx))
//
// 	// Start channel reader
// 	ch, err := conn.ChannelReader(ctx, topic, "test-group")
// 	require.NoError(t, err)
// 	require.NotNil(t, ch)
//
// 	// Create writer
// 	writer, err := conn.Writer(topic)
// 	require.NoError(t, err)
// 	defer writer.Close(ctx)
//
// 	value := []byte("hello world")
// 	headers := map[string][]byte{"Content-Type": []byte("test/json")}
//
// 	// Write the message
// 	err = writer.Write(ctx, []byte("key1"), value, headers)
// 	require.NoError(t, err)
//
// 	// Wait to receive from channel
// 	var msg Message
// 	select {
// 	case msg = <-ch:
// 	case <-time.After(5 * time.Second):
// 		t.Fatal("timeout waiting for message from ChannelReader")
// 	}
//
// 	require.Equal(t, "hello world", string(msg.Value))
// 	require.Equal(t, topic, msg.Topic)
// 	require.Equal(t, []byte("test/json"), msg.Headers["Content-Type"])
// 	require.Contains(t, msg.Headers, "traceparent")
//
// 	// Flush telemetry output
// 	time.Sleep(50 * time.Millisecond)
// 	out := telemetryBuf.String()
//
// 	// Metrics must exist
// 	require.Contains(t, out, "messages_produced_total")
// 	require.Contains(t, out, "kafka_message_lag")
// }
//
// func Test_connection_committer(t *testing.T) {
// 	ctx := context.Background()
//
// 	var telemetryBuf bytes.Buffer
// 	tel, telClose := telemetry.New(
// 		ctx,
// 		"kafka-test",
// 		telemetry.WithLocal(true),
// 		telemetry.WithLocalWriter(&telemetryBuf),
// 	)
// 	defer telClose(ctx)
//
// 	topic := "reader-committer"
// 	closeTopic := createTopic(t, topic)
// 	defer closeTopic()
//
// 	conn, err := New(config, tel)
// 	require.NoError(t, err)
//
// 	// Write two messages
// 	writer, err := conn.Writer(topic)
// 	require.NoError(t, err)
//
// 	require.NoError(t, writer.Write(ctx, nil, []byte("msg1"), map[string][]byte{"nr": []byte("1")}))
// 	require.NoError(t, writer.Write(ctx, nil, []byte("msg2"), map[string][]byte{"nr": []byte("2")}))
// 	require.NoError(t, writer.Close(ctx))
//
// 	group1 := "group-no-commit"
// 	group2 := "group-with-commit"
//
// 	longWait := time.Minute
//
// 	// --- GROUP 1: Read without commit ---
// 	reader1, err := conn.Reader(topic, group1)
// 	require.NoError(t, err)
//
// 	msgs, _, err := reader1.Read(ctx, 1, longWait)
// 	require.NoError(t, err)
// 	require.Len(t, msgs, 1)
// 	require.Equal(t, "1", string(msgs[0].Headers["nr"]))
// 	require.NoError(t, reader1.Close(ctx))
//
// 	// --- GROUP 2: Read with commit ---
// 	reader2, err := conn.Reader(topic, group2)
// 	require.NoError(t, err)
//
// 	msgs, commit, err := reader2.Read(ctx, 1, longWait)
// 	require.NoError(t, err)
// 	require.NoError(t, commit(ctx))
// 	require.Len(t, msgs, 1)
// 	require.Equal(t, "1", string(msgs[0].Headers["nr"]))
// 	require.NoError(t, reader2.Close(ctx))
//
// 	// --- GROUP 1 again: should still see msg1 (no commit done) ---
// 	reader1b, err := conn.Reader(topic, group1)
// 	require.NoError(t, err)
//
// 	msgs, _, err = reader1b.Read(ctx, 1, longWait)
// 	require.NoError(t, err)
// 	require.Len(t, msgs, 1)
// 	require.Equal(t, "1", string(msgs[0].Headers["nr"]))
// 	require.NoError(t, reader1b.Close(ctx))
//
// 	// --- GROUP 2 again: should now see msg2 ---
// 	reader2b, err := conn.Reader(topic, group2)
// 	require.NoError(t, err)
//
// 	msgs, _, err = reader2b.Read(ctx, 1, longWait)
// 	require.NoError(t, err)
// 	require.Len(t, msgs, 1)
// 	require.Equal(t, "2", string(msgs[0].Headers["nr"]))
// 	require.NoError(t, reader2b.Close(ctx))
// }
