package kafkarator

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hafslundkraft/golib/telemetry"
)

func buildKafkaConfigMap(c Config, tel *telemetry.Provider) (*kafka.ConfigMap, error) {
	if c.SASL.Enabled {
		return saslConfigMap(c, tel)
	}
	return tlsConfigMap(c)
}

func saslConfigMap(c Config, tel *telemetry.Provider) (*kafka.ConfigMap, error) {
	if c.SASL.Scope == "" {
		return nil, fmt.Errorf("failed to create Azure token provider: %w")
	}

	return &kafka.ConfigMap{
		"bootstrap.servers": c.Brokers,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "OAUTHBEARER",

		// Required for OAuthBearer
		"enable.sasl.oauthbearer.unsecure.jwt": true,
	}, nil
}

func tlsConfigMap(c Config) (*kafka.ConfigMap, error) {
	if c.CertFile == "" || c.KeyFile == "" || c.CAFile == "" {
		return nil, fmt.Errorf("TLS mode enabled but certificate variables are missing")
	}

	conf := &kafka.ConfigMap{
		"bootstrap.servers":        c.Brokers,
		"security.protocol":        "SSL",
		"ssl.key.location":         c.KeyFile,
		"ssl.certificate.location": c.CertFile,
		"ssl.ca.location":          c.CAFile,
	}

	return conf, nil
}

func cloneConfigMap(src *kafka.ConfigMap) kafka.ConfigMap {
	dst := make(kafka.ConfigMap, len(*src))

	for k, v := range *src {
		dst[k] = v
	}

	return dst
}
