package kafkarator

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func buildKafkaConfigMap(c *Config) (*kafka.ConfigMap, error) {
	if c.AuthMode == "sasl" {
		return saslConfigMap(c)
	}
	return tlsConfigMap(c)
}

func saslConfigMap(c *Config) (*kafka.ConfigMap, error) {
	if c.SASL.Scope == "" {
		return nil, fmt.Errorf("failed to create Azure token provider")
	}

	return &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.Brokers, ","),
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "OAUTHBEARER",

		// Required for OAuthBearer
		"enable.sasl.oauthbearer.unsecure.jwt": true,
	}, nil
}

func tlsConfigMap(c *Config) (*kafka.ConfigMap, error) {
	if c.TLS.CertFile == "" || c.TLS.KeyFile == "" || c.TLS.CAFile == "" {
		return nil, fmt.Errorf("TLS mode enabled but certificate variables are missing")
	}

	conf := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(c.Brokers, ","),
		"security.protocol":        "SSL",
		"ssl.key.location":         c.TLS.KeyFile,
		"ssl.certificate.location": c.TLS.CertFile,
		"ssl.ca.location":          c.TLS.CAFile,
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
