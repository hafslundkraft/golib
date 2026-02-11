package main

import (
	"context"
	"log"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hafslundkraft/golib/kafkarator"
	"github.com/hafslundkraft/golib/telemetry"
	redpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
)

func startRedpandaContainer(ctx context.Context, redpandaImage string) *redpanda.Container {
	container, err := redpanda.Run(ctx,
		redpandaImage,
		redpanda.WithAutoCreateTopics(),
	)
	if err != nil {
		log.Fatalf("failed to start redpanda container %v", err)
	}
	return container
}

func getRedpandaBrokerAddress(ctx context.Context, container *redpanda.Container) string {
	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		log.Fatalf("Failed to get redpanda broker: %v", err)
	}
	return broker
}

func getRedpandaSchemaRegistryAddress(ctx context.Context, container *redpanda.Container) string {
	url, err := container.SchemaRegistryAddress(ctx)
	if err != nil {
		log.Fatalf("Failed to get redpanda schema registry address: %v", err)
	}
	return url
}

func newSchemaRegistryClient(url string) (sr.Client, error) {
	cfg := sr.NewConfig(url)
	return sr.NewClient(cfg)
}

func ensureSchemaRegistered(client sr.Client, topic, schema string) error {
	subject := topic + "-value"

	// Fast path: already registered
	if _, err := client.GetLatestSchemaMetadata(subject); err == nil {
		return nil
	}

	// If not found, register it
	_, err := client.Register(subject, sr.SchemaInfo{
		Schema: schema,
	}, false)
	if err != nil {
		log.Fatalf("register schema for subject %s: %v", subject, err)
	}

	return nil
}

// setupKafkaConnection creates a kafkarator connection with mock schema registry
func setupKafkaConnection(broker, schemaRegistryURL, schema string, tp *telemetry.Provider) *kafkarator.Connection {
	config := &kafkarator.Config{
		Broker:   broker,
		AuthMode: kafkarator.AuthNone,
		SchemaRegistryConfig: kafkarator.SchemaRegistryConfig{
			SchemaRegistryURL:      schemaRegistryURL,
			SchemaRegistryUser:     "",
			SchemaRegistryPassword: "",
		},
	}
	srClient, err := newSchemaRegistryClient(schemaRegistryURL)
	if err != nil {
		log.Fatalf("Failed to create schema registry client: %v", err)
	}

	conn, err := kafkarator.New(
		config,
		tp,
		kafkarator.WithSchemaRegistryClient(srClient),
	)
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	if err := ensureSchemaRegistered(srClient, topic, schema); err != nil {
		log.Fatalf("Failed to ensure schema is registered: %v", err)
	}
	return conn
}
