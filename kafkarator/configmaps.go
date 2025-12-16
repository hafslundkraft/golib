package kafkarator

import (
	"encoding/base64"
	"fmt"
	"log"
	"os"
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

	resolvedCA, err := resolveCertPath(c.TLS.CACert, "kafka-ca-cert-")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve CA cert: %w", err)
	}

	return &kafka.ConfigMap{
		"bootstrap.servers": c.Broker,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "OAUTHBEARER",
		"ssl.ca.location":   resolvedCA,

		// Required for OAuthBearer
		"enable.sasl.oauthbearer.unsecure.jwt": true,
	}, nil
}

func tlsConfigMap(c *Config) (*kafka.ConfigMap, error) {
	if c.TLS.CertFile == "" || c.TLS.KeyFile == "" || c.TLS.CACert == "" {
		return nil, fmt.Errorf("TLS mode enabled but certificate variables are missing")
	}

	conf := &kafka.ConfigMap{
		"bootstrap.servers":        c.Broker,
		"security.protocol":        "SSL",
		"ssl.key.location":         c.TLS.KeyFile,
		"ssl.certificate.location": c.TLS.CertFile,
		"ssl.ca.location":          c.TLS.CACert,
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

func resolveCertPath(certEnv, prefix string) (string, error) {
	// the env is already a file path
	if fi, err := os.Stat(certEnv); err == nil && !fi.IsDir() {
		return certEnv, nil
	}

	trimmed := strings.TrimSpace(certEnv)

	// helper to write cert content to a temp file
	writeTemp := func(content string) (string, error) {
		tmpFile, err := os.CreateTemp("", prefix)
		if err != nil {
			return "", fmt.Errorf("failed to create temp cert file: %w", err)
		}

		defer func() {
			if cerr := tmpFile.Close(); cerr != nil {
				log.Printf("failed to close temp cert file %q: %v", tmpFile.Name(), cerr)
			}
		}()

		if _, err := tmpFile.WriteString(content); err != nil {
			return "", fmt.Errorf("failed to write cert content: %w", err)
		}

		return tmpFile.Name(), nil
	}

	// raw PEM content
	if strings.HasPrefix(trimmed, "-----BEGIN") {
		return writeTemp(trimmed)
	}

	// base64-encoded PEM
	if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil {
		decodedStr := strings.TrimSpace(string(decoded))
		if strings.HasPrefix(decodedStr, "-----BEGIN") {
			return writeTemp(decodedStr)
		}
	}

	return "", fmt.Errorf(
		"KAFKA_CA_CERT is neither a valid file, PEM content, nor base64-encoded PEM",
	)
}
