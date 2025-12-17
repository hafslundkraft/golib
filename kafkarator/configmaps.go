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

	resolvedCA, err := resolveCertPath(c.CACert, "kafka-ca-cert-")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve CA cert: %w", err)
	}

	conf := &kafka.ConfigMap{
		"bootstrap.servers": c.Broker,
		"security.protocol": "sasl_ssl",
		"sasl.mechanisms":   "OAUTHBEARER",
		"ssl.ca.location":   resolvedCA,
		"debug":             "security,broker,protocol,admin",
	}

	return conf, nil
}

func tlsConfigMap(c *Config) (*kafka.ConfigMap, error) {
	if c.TLS.CertFile == "" || c.TLS.KeyFile == "" {
		return nil, fmt.Errorf("TLS mode enabled but certificate variables are missing")
	}

	resolvedCA, err := resolveCertPath(c.CACert, "kafka-ca-cert-")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve CA cert: %w", err)
	}

	conf := &kafka.ConfigMap{
		"bootstrap.servers":        c.Broker,
		"security.protocol":        "SSL",
		"ssl.key.location":         c.TLS.KeyFile,
		"ssl.certificate.location": c.TLS.CertFile,
		"ssl.ca.location":          resolvedCA,
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
	certEnv = strings.TrimSpace(certEnv)
	if certEnv == "" {
		return "", fmt.Errorf("KAFKA_CA_CERT is empty")
	}

	// Replace escaped newlines (VERY important for K8s/Helm)
	certEnv = strings.ReplaceAll(certEnv, `\n`, "\n")

	// Already a file path
	if fi, err := os.Stat(certEnv); err == nil && !fi.IsDir() {
		return certEnv, nil
	}

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

	// PEM content
	if strings.Contains(certEnv, "BEGIN CERTIFICATE") {
		return writeTemp(certEnv)
	}

	// Base64 PEM (edge cases)
	clean := strings.ReplaceAll(certEnv, "\n", "")
	if decoded, err := base64.StdEncoding.DecodeString(clean); err == nil {
		decodedStr := strings.TrimSpace(string(decoded))
		if strings.Contains(decodedStr, "BEGIN CERTIFICATE") {
			return writeTemp(decodedStr)
		}
	}

	return "", fmt.Errorf(
		"KAFKA_CA_CERT is not a file, PEM, or base64 PEM (starts with %.30q)",
		certEnv,
	)
}
