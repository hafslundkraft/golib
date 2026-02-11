package kafkarator

import (
	"fmt"
	"log"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

type SchemaRegistryTestHelper struct {
	Client sr.Client
}

func NewSchemaRegistryTestHelper(schemaRegistryURL, topic, schema string) *SchemaRegistryTestHelper {
	schemaRegistryConfig := SchemaRegistryConfig{
		SchemaRegistryURL:      schemaRegistryURL,
		SchemaRegistryUser:     "",
		SchemaRegistryPassword: "",
	}
	srClient, err := newTestHelperSchemaRegistryClient(schemaRegistryConfig)
	if err != nil {
		log.Fatalf("Failed to create schema registry client: %v", err)
	}
	if err := ensureSchemaRegistered(srClient, topic, schema); err != nil {
		log.Fatalf("Failed to ensure schema registered: %v", err)
	}
	return &SchemaRegistryTestHelper{
		Client: srClient,
	}
}

func ensureSchemaRegistered(client sr.Client, topic, schema string) error {
	subject := topicValueSubject(topic)
	// Fast path: already registered
	if _, err := client.GetLatestSchemaMetadata(subject); err == nil {
		return nil
	}

	// If not found, register it
	_, err := client.Register(subject, sr.SchemaInfo{
		Schema: schema,
	}, false)
	if err != nil {
		return fmt.Errorf("register schema for subject %s: %w", subject, err)
	}

	return nil
}

func topicValueSubject(topic string) string {
	return topic + "-value"
}

func newTestHelperSchemaRegistryClient(cfg SchemaRegistryConfig) (sr.Client, error) {
	srCfg := sr.NewConfig(cfg.SchemaRegistryURL)
	return sr.NewClient(srCfg)
}
