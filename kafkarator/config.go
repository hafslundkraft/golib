package kafkarator

import (
	"fmt"
	"os"
	"strings"
)

const (
	// EnvBrokers is the environment variable for Kafka broker addresses (comma-separated)
	EnvBrokers = "KAFKA_BROKERS"

	// EnvCertFile is the environment variable for the Kafka certificate file path
	EnvCertFile = "KAFKA_CERT_FILE"

	// EnvKeyFile is the environment variable for the Kafka key file path
	EnvKeyFile = "KAFKA_KEY_FILE"

	// EnvCAFile is the environment variable for the Kafka CA file path
	EnvCAFile = "KAFKA_CA_FILE"
)

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
}

// ConfigFromEnvVars loads and returns an instance with values that are fetched from environment variables defined
// in this package. If any of these variables do not exist, an error is returned.
func ConfigFromEnvVars() (Config, error) {
	brokers := os.Getenv(EnvBrokers)
	if brokers == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", EnvBrokers)
	}

	certFile := os.Getenv(EnvCertFile)
	if certFile == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", EnvCertFile)
	}

	keyFile := os.Getenv(EnvKeyFile)
	if keyFile == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", EnvKeyFile)
	}

	caFile := os.Getenv(EnvCAFile)
	if caFile == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", EnvCAFile)
	}

	// Split brokers by comma and trim whitespace
	brokerList := strings.Split(brokers, ",")
	for i, broker := range brokerList {
		brokerList[i] = strings.TrimSpace(broker)
	}

	c := Config{
		Brokers:  brokerList,
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	}

	return c, nil
}
