package main

import (
	"context"
	"log"

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

// setupKafkaConnection creates a kafkarator connection with schema registry
func setupKafkaConnection(broker, schemaRegistryURL, schema string, tp *telemetry.Provider) *kafkarator.Connection {
	config := &kafkarator.Config{
		Broker:   broker,
		AuthMode: kafkarator.AuthNone,
		SchemaRegistryConfig: kafkarator.SchemaRegistryConfig{
			SchemaRegistryURL:      schemaRegistryURL,
			SchemaRegistryUser:     "",
			SchemaRegistryPassword: "",
		},
		Env:          "test",
		SystemName:   "kafkarator",
		WorkloadName: "processor-demo",
	}
	testHelper, err := kafkarator.NewSchemaRegistryTestHelper(schemaRegistryURL, topic, schema, tp)
	if err != nil {
		log.Fatalf("Failed to create schema registry test helper: %v", err)
	}
	conn, err := kafkarator.NewConnection(
		config,
		tp,
		kafkarator.WithSchemaRegistryClient(testHelper),
	)
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	return conn
}
