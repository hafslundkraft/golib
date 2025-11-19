package kafkarator

import (
	"context"
	"github.com/hafslundkraft/golib/telemetry"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testkafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

const kafkaImage = "confluentinc/confluent-local:7.5.0"

func Test_connection_Consumer(t *testing.T) {
	ctx := context.Background()

	// Start Kafka container
	_, brokers, closer := startTestContainer(ctx, t)
	defer closer()

	// Create a topic using kafka-go
	topicName := "test-topic"
	adminCloser := createTopic(t, topicName, brokers[0])
	defer adminCloser()

	// Create Config for New() - TLS is optional, so we can omit cert files for local testing
	config := Config{
		Brokers: brokers,
	}

	tel, err := telemetry.New(ctx, "kafka-test", telemetry.WithLocal(true))
	require.NoError(t, err)

	// Initialize connection
	conn, err := New(config, tel)
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	// Test the connection
	err = conn.Test(ctx)
	if err != nil {
		t.Fatalf("failed to test connection: %v", err)
	}

	// Now you can test Consumer
	cnsmr, err := conn.Consumer(topicName, "test-group")
	if err != nil {
		t.Fatalf("failed to create cnsmr: %v", err)
	}

	// Verify cnsmr was created successfully
	if cnsmr == nil {
		t.Fatal("cnsmr is nil")
	}

	prdcr, err := conn.Producer(topicName)
	require.NoError(t, err)

	msg := []byte("hello world")
	headers := map[string][]byte{
		"Content-Type": []byte("application/json"),
	}

	errChan := make(chan error)
	go func() {
		produceErr := prdcr.Produce(ctx, msg, headers)
		if produceErr != nil {
			errChan <- produceErr
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)

	messageChan, err := cnsmr.Consume(ctx)
	require.NoError(t, err)

	var receivedMessage *Message
	var receivedError error
	go func() {
		select {
		case err := <-errChan:
			receivedError = err
			return
		case m := <-messageChan:
			receivedMessage = &m
		}
		defer wg.Done()
	}()

	wg.Wait()

	require.NoError(t, receivedError)
	require.NotNil(t, receivedMessage)
	require.Equal(t, "hello world", string(receivedMessage.Value))
	require.Equal(t, topicName, receivedMessage.Topic)
	require.Equal(t, headers, receivedMessage.Headers)
}

func startTestContainer(
	ctx context.Context,
	t *testing.T,
) (kafkaContainer *testkafka.KafkaContainer, brokers []string, closer func()) {
	t.Helper()

	var err error
	kafkaContainer, err = testkafka.Run(
		ctx,
		kafkaImage,
		testkafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		t.Fatalf("failed to start kafka container: %v", err)
	}

	brokers, err = kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get brokers: %v", err)
	}

	closer = func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Errorf("failed to terminate kafka container: %v", err)
		}
	}

	return
}

func createTopic(t *testing.T, topicName, broker string) func() error {
	t.Helper()

	adminConn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("failed to dial kafka: %v", err)
	}

	err = adminConn.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	return adminConn.Close
}
