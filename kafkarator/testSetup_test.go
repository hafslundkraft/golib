package kafkarator

import (
	"context"
	"log"
	"os"
	"testing"

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

	os.Setenv("ENV", "test")
	os.Setenv("KAFKA_SASL_SCOPE", "dummy-scope")
	os.Setenv("KAFKA_AUTH_TYPE", "tls")
	os.Setenv("SCHEMA_REGISTRY_PASSWORD", "dummy")

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
		Env:      "test",
		Broker:   broker,
		AuthMode: AuthNone, // No auth for test container
		SchemaRegistryConfig: SchemaRegistryConfig{
			SchemaRegistryPassword: "dummy",
			SchemaRegistryUser:     "none",
			SchemaRegistryURL:      "",
		},
	}

	// Run all tests
	exitCode := m.Run()

	if err := kafkaContainer.Terminate(ctx); err != nil {
		log.Fatalf("failed to terminate kafka container: %v", err)
	}

	os.Exit(exitCode)
}

// func createTopic(t *testing.T, topicName string) func() error {
// 	t.Helper()
//
// 	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
// 		"bootstrap.servers": broker,
// 	})
// 	require.NoError(t, err)
//
// 	results, err := admin.CreateTopics(
// 		context.Background(),
// 		[]kafka.TopicSpecification{
// 			{
// 				Topic:             topicName,
// 				NumPartitions:     1,
// 				ReplicationFactor: 1,
// 			},
// 		},
// 	)
// 	require.NoError(t, err)
// 	require.Len(t, results, 1)
//
// 	if results[0].Error.Code() != kafka.ErrNoError {
// 		require.NoError(t, results[0].Error)
// 	}
//
// 	return func() error {
// 		admin.Close()
// 		return nil
// 	}
// }
