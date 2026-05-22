package demohelpers

import (
	"context"
	"fmt"
	"log"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	redpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/hafslundkraft/golib/kafkarator"
	"github.com/hafslundkraft/golib/telemetry"
)

// StartRedpandaContainer starts a Redpanda testcontainer using the provided image.
func StartRedpandaContainer(ctx context.Context, redpandaImage string) *redpanda.Container {
	container, err := redpanda.Run(ctx,
		redpandaImage,
		redpanda.WithAutoCreateTopics(),
	)
	if err != nil {
		log.Fatalf("failed to start redpanda container: %v", err)
	}
	return container
}

// GetRedpandaBrokerAddress returns the Kafka seed broker address.
func GetRedpandaBrokerAddress(ctx context.Context, container *redpanda.Container) string {
	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		log.Fatalf("failed to get redpanda broker: %v", err)
	}
	return broker
}

// GetRedpandaSchemaRegistryAddress returns the schema registry endpoint.
func GetRedpandaSchemaRegistryAddress(ctx context.Context, container *redpanda.Container) string {
	url, err := container.SchemaRegistryAddress(ctx)
	if err != nil {
		log.Fatalf("failed to get redpanda schema registry address: %v", err)
	}
	return url
}

// SetupKafkaConnection creates a kafkarator connection and registers a topic value schema.
func SetupKafkaConnection(
	broker, schemaRegistryURL, topic, schema, systemName, workloadName string,
	tp *telemetry.Provider,
) *kafkarator.Connection {
	config := &kafkarator.Config{
		Broker:   broker,
		AuthMode: kafkarator.AuthNone,
		SchemaRegistryConfig: kafkarator.SchemaRegistryConfig{
			SchemaRegistryURL:      schemaRegistryURL,
			SchemaRegistryUser:     "",
			SchemaRegistryPassword: "",
		},
		Env:          "test",
		SystemName:   systemName,
		WorkloadName: workloadName,
	}

	srClient, err := kafkarator.NewSchemaRegistryTestHelper(schemaRegistryURL, topic, schema, tp)
	if err != nil {
		log.Fatalf("failed to create schema registry test helper: %v", err)
	}
	conn, err := kafkarator.NewConnection(config, tp, kafkarator.WithSchemaRegistryClient(srClient))
	if err != nil {
		log.Fatalf("failed to create connection: %v", err)
	}
	return conn
}

// RegisterSchema registers a schema under the given subject in schema registry.
func RegisterSchema(schemaRegistryURL, subject, schema string) {
	client, err := sr.NewClient(sr.NewConfig(schemaRegistryURL))
	if err != nil {
		log.Fatalf("failed to create schema registry client: %v", err)
	}
	if _, err := client.Register(subject, sr.SchemaInfo{Schema: schema}, false); err != nil {
		log.Fatalf("failed to register schema for subject %q: %v", subject, err)
	}
	log.Printf("Schema registered under %q", subject)
}

// ClaimCheckPayloadSubject returns the subject used for claim-check payload schemas.
func ClaimCheckPayloadSubject(topic string) string {
	return fmt.Sprintf("%s-claim-check-payload", topic)
}
