package kafkarator

import (
	"fmt"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

func newSchemaRegistryClient(cfg *SchemaRegistryConfig) (sr.Client, error) {
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
