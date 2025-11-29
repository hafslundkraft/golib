package kafkarator

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	testkafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const kafkaImage = "confluentinc/confluent-local:7.5.0"

var (
	broker string //nolint:gochecknoglobals // Shared kafka container brokers
	config Config //nolint:gochecknoglobals // Shared kafka config
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Set up the global text map propagator for W3C Trace Context
	// This is required for trace context propagation to work
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	kafkaContainer, err := testkafka.Run(
		ctx,
		kafkaImage,
		testkafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		log.Fatalf("failed to start kafka container: %v", err)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		log.Fatalf("failed to get brokers: %v", err)
	}

	if len(brokers) == 0 {
		log.Fatalf("failed to get brokers")
	}

	broker = brokers[0]

	config = Config{
		Brokers: brokers,
	}

	// Run all tests
	exitCode := m.Run()

	if err := kafkaContainer.Terminate(ctx); err != nil {
		log.Fatalf("failed to terminate kafka container: %v", err)
	}

	os.Exit(exitCode)
}

func createTopic(t *testing.T, topicName string) func() error {
	t.Helper()

	adminConn, err := kafka.Dial("tcp", broker)
	require.NoError(t, err, "failed to dial kafka")

	err = adminConn.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		require.NoError(t, err, "failed to create topic")
	}

	return adminConn.Close
}
