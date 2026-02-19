package kafkarator

import (
	"fmt"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

// NewSchemaRegistryTestHelper creates a test helper that ensures the given schema is registered in the schema registry
func NewSchemaRegistryTestHelper(
	schemaRegistryURL, topic, schema string,
	tel TelemetryProvider,
) (SchemaRegistryClient, error) {
	schemaRegistryConfig := SchemaRegistryConfig{
		SchemaRegistryURL:      schemaRegistryURL,
		SchemaRegistryUser:     "",
		SchemaRegistryPassword: "",
	}
	srClient, err := newTestHelperSchemaRegistryClient(schemaRegistryConfig, tel)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry client: %w", err)
	}
	if err := ensureSchemaRegistered(srClient, topic, schema); err != nil {
		return nil, fmt.Errorf("failed to ensure schema registered: %w", err)
	}
	client, err := newSchemaRegistryClient(&schemaRegistryConfig, tel)
	if err != nil {
		return nil, fmt.Errorf("failed to create traced schema registry client: %w", err)
	}
	return client, nil
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
		return fmt.Errorf("register schema error %s: %w", subject, err)
	}

	return nil
}

func topicValueSubject(topic string) string {
	return topic + "-value"
}

func newTestHelperSchemaRegistryClient(cfg SchemaRegistryConfig, tel TelemetryProvider) (sr.Client, error) {
	if cfg.SchemaRegistryURL == "" {
		return nil, fmt.Errorf("schema registry url is empty")
	}

	var srCfg *sr.Config
	if cfg.SchemaRegistryUser != "" || cfg.SchemaRegistryPassword != "" {
		srCfg = sr.NewConfigWithBasicAuthentication(
			cfg.SchemaRegistryURL,
			cfg.SchemaRegistryUser,
			cfg.SchemaRegistryPassword,
		)
	} else {
		srCfg = sr.NewConfig(cfg.SchemaRegistryURL)
	}

	srCfg.HTTPClient = instrumentHTTPClient(srCfg.HTTPClient, tel)

	client, err := sr.NewClient(srCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create client %w", err)
	}
	return client, nil
}
