package kafkarator

import (
	"fmt"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

// SchemaRegistryClient implements the package's own abstraction for the
// schema registry, the purpose being to make it as easy as possible to
// inject a mock in testing scenarios.
type SchemaRegistryClient interface {
	// GetBySubjectAndID returns a schema for the given topic and ID.
	GetBySubjectAndID(subject string, id int) (schema sr.SchemaInfo, err error)

	// GetLatestSchemaMetadata returns the latest metadata for the subject.
	GetLatestSchemaMetadata(subject string) (sr.SchemaMetadata, error)
}

func newSchemaRegistryClient(cfg *SchemaRegistryConfig) (SchemaRegistryClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	cfgSR := sr.NewConfigWithBasicAuthentication(
		cfg.SchemaRegistryURL, cfg.SchemaRegistryUser, cfg.SchemaRegistryPassword)

	client, err := sr.NewClient(cfgSR)
	if err != nil {
		return nil, fmt.Errorf(
			"create schema registry client (url=%s): %w",
			cfg.SchemaRegistryURL,
			err,
		)
	}
	return client, nil
}
