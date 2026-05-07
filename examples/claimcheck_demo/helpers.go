package main

import (
	"context"
	"fmt"
	"log"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	redpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/hafslundkraft/golib/kafkarator"
	"github.com/hafslundkraft/golib/telemetry"
)

func startRedpandaContainer(ctx context.Context) *redpanda.Container {
	container, err := redpanda.Run(ctx,
		"docker.redpanda.com/redpandadata/redpanda:v23.3.3",
		redpanda.WithAutoCreateTopics(),
	)
	if err != nil {
		log.Fatalf("failed to start redpanda: %v", err)
	}
	return container
}

func getRedpandaBrokerAddress(ctx context.Context, c *redpanda.Container) string {
	broker, err := c.KafkaSeedBroker(ctx)
	if err != nil {
		log.Fatalf("failed to get broker address: %v", err)
	}
	return broker
}

func getRedpandaSchemaRegistryAddress(ctx context.Context, c *redpanda.Container) string {
	url, err := c.SchemaRegistryAddress(ctx)
	if err != nil {
		log.Fatalf("failed to get schema registry address: %v", err)
	}
	return url
}

// setupKafkaConnection creates a kafkarator connection with a pre-registered
// envelope schema under "{topic}-value".
func setupKafkaConnection(
	broker, srURL, envelopeSchema string,
	tp *telemetry.Provider,
) *kafkarator.Connection {
	config := &kafkarator.Config{
		Broker:   broker,
		AuthMode: kafkarator.AuthNone,
		SchemaRegistryConfig: kafkarator.SchemaRegistryConfig{
			SchemaRegistryURL:      srURL,
			SchemaRegistryUser:     "",
			SchemaRegistryPassword: "",
		},
		Env:          "test",
		SystemName:   "claimcheck",
		WorkloadName: "claimcheck-demo",
	}
	// NewSchemaRegistryTestHelper registers envelopeSchema under "{topic}-value".
	srClient, err := kafkarator.NewSchemaRegistryTestHelper(srURL, topic, envelopeSchema, tp)
	if err != nil {
		log.Fatalf("failed to register envelope schema: %v", err)
	}
	conn, err := kafkarator.NewConnection(config, tp, kafkarator.WithSchemaRegistryClient(srClient))
	if err != nil {
		log.Fatalf("failed to create kafkarator connection: %v", err)
	}
	return conn
}

// registerPayloadSchema registers payloadSchema under "{topic}-claim-check-payload"
// in Schema Registry so the Stager can fetch it when opening a batch.
func registerPayloadSchema(srURL, payloadSchema string) {
	client, err := sr.NewClient(sr.NewConfig(srURL))
	if err != nil {
		log.Fatalf("failed to create SR client: %v", err)
	}
	subject := topic + "-claim-check-payload"
	if _, err := client.Register(subject, sr.SchemaInfo{Schema: payloadSchema}, false); err != nil {
		log.Fatalf("failed to register payload schema: %v", err)
	}
	fmt.Printf("Payload schema registered under %q\n", subject)
}
