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
	brokers := os.Getenv(envBrokers)
	if brokers == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", envBrokers)
	}

	certFile := os.Getenv(envCertFile)
	if certFile == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", envCertFile)
	}

	keyFile := os.Getenv(envKeyFile)
	if keyFile == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", envKeyFile)
	}

	caFile := os.Getenv(envCAFile)
	if caFile == "" {
		return Config{}, fmt.Errorf("environment variable %s is not set", envCAFile)
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
