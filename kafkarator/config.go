package kafkarator

import (
	"fmt"
	"os"
	"strings"
)

const (
	// envBrokers is the environment variable for Kafka broker addresses (comma-separated)
	envBrokers = "KAFKA_BROKERS"

	// envCertFile is the environment variable for the Kafka certificate file path
	envCertFile = "KAFKA_CERT_FILE"

	// envKeyFile is the environment variable for the Kafka key file path
	envKeyFile = "KAFKA_KEY_FILE"

	// envCAFile is the environment variable for the Kafka CA file path
	envCAFile = "KAFKA_CA_FILE"

	envSASLScope = "KAFKA_SASL_SCOPE"
)

type SASLConfig struct {
	Scope       string // Azure AD scope (audience)
	ExpectedOID string // Optional: validate service account OID
	Enabled     bool
}

// Config contains all necessary configuration needed to connect to Kafka.
type Config struct {
	// Brokers is the list of Kafka brokers
	Brokers []string

	// CertFile is the certificate file of the Kafka service.
	CertFile string

	// KeyFile is the file containing the key to the Kafka service.
	KeyFile string

	// CAFile is the certificate authority file.
	CAFile string

	//SASL configuration
	SASL SASLConfig
}

// ConfigFromEnvVars loads and returns an instance with values that are fetched from environment variables defined
// in this package. If any of these variables do not exist, an error is returned.
func ConfigFromEnvVars() (Config, error) {
	env := os.Getenv("ENV")
	cfg := Config{}
	brokers := os.Getenv(envBrokers)
	if brokers == "" {
		return Config{}, fmt.Errorf("env variable %s is not set", envBrokers)
	}

	brokerList := strings.Split(brokers, ",")
	for i, broker := range brokerList {
		brokerList[i] = strings.TrimSpace(broker)
	}

	cfg.Brokers = brokerList

	if env != "local" {
		// Will use SASL since this is production/test
		cfg.SASL.Enabled = true
		cfg.SASL.Scope = os.Getenv("KAFKA_SASL_SCOPE")
		cfg.SASL.ExpectedOID = os.Getenv("KAFKA_SASL_EXPECTED_OID")

		if cfg.SASL.Scope == "" {
			return Config{}, fmt.Errorf("env variable %s is not set", envSASLScope)
		}

		return cfg, nil
	}

	certFile := os.Getenv(envCertFile)
	if certFile == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", envCertFile)
	}

	keyFile := os.Getenv(envKeyFile)
	if keyFile == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", envKeyFile)
	}
	cfg.KeyFile = keyFile

	caFile := os.Getenv(envCAFile)
	if caFile == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", envCAFile)
	}
	cfg.KeyFile = keyFile
	cfg.CAFile = caFile
	cfg.CertFile = certFile
	cfg.SASL.Enabled = false

	return cfg, nil
}
