package kafkarator

import (
	"fmt"
	"os"
	"strings"
)

const (
	// envBrokers is the environment variable for Kafka broker addresses (comma-separated)
	envBroker = "KAFKA_BROKER"

	// envCertFile is the environment variable for the Kafka certificate file path
	envCertFile = "KAFKA_CERT_FILE"

	// envKeyFile is the environment variable for the Kafka key file path
	envKeyFile = "KAFKA_KEY_FILE"

	// envCACert is the environment variable for the Kafka CA certification
	envCACert = "KAFKA_CA_CERT"

	// envAzureOID is the client object id for the app in Azure
	envAzureScope = "AZURE_KAFKA_SCOPE"

	// envAuthMode tells which auth mode to use for Kafka
	envAuthType = "KAFKA_AUTH_TYPE"

	// envSchemaRegistryUser is the user for the Aiven Schema Registry
	envKafkaUser = "KAFKA_USERNAME"

	// envSchemaRegistryPassword is the password for the Aiven Schema Registry user
	// #nosec G101 -- This is an environment variable *name*, not a credential.
	envKafkaPassword = "KAFKA_PASSWORD"

	// envSchemaRegistryURL is the URL for the Aiven Schema Registry
	envSchemaRegistryURL = "KAFKA_SCHEMA_REGISTRY_URL"

	// envHappiWorkloadName is the name of the workload on the happi platform.
	envHappiWorkloadName = "HAPPI_WORKLOAD_NAME"

	// envHappiSystemName is the name of the system on the happi platform.
	envHappiSystemName = "HAPPI_SYSTEM_NAME"

	// envHappiEnvName is the name of the environment on the happi platform, e.g. "dev", "prod", etc.
	envHappiEnvName = "HAPPI_ENV"
)

// SASLConfig contains necessary configuration needed to connect to Kafka with SASL
type SASLConfig struct {
	Scope       string // Azure scope
	ExpectedOID string // Optional: validate service account OID
}

// TLSConfig contains necessary configuration needed to connect to Kafka with TLS
type TLSConfig struct {
	// CertFile is the certificate file of the Kafka service.
	CertFile string

	// KeyFile is the file containing the key to the Kafka service.
	KeyFile string
}

// SchemaRegistryConfig contains the config for the schema registry
type SchemaRegistryConfig struct {
	// SchemaRegistryURL is the URL for the schema registry URL
	SchemaRegistryURL string

	// SchemaRegistryUser is the username for the schema registry
	SchemaRegistryUser string

	// SchemaRegistryPassword is the password to use for authentication towards the registry
	SchemaRegistryPassword string
}

// AuthMode indicates which type of authentication (if any) should
// be used against Kafka.
type AuthMode int

const (
	// AuthNotSet indicates that the authentication mode has not been set. This
	// is probably an error.
	AuthNotSet AuthMode = iota

	// AuthNone indicates that no authentication should be performed. This is
	// mostly in use in tests.
	AuthNone

	// AuthSASL indicates that SASL auth shall be used.
	AuthSASL

	// AuthTLS indicates that TLS auth shall be used.
	AuthTLS
)

// Config contains all necessary configuration needed to connect to Kafka.
type Config struct {
	// Broker is the Kafka broker
	Broker string

	// Which authentication mode to use towards Kafka service
	AuthMode AuthMode

	// CA certificate for the service
	CACert string

	// SASL configuration
	SASL SASLConfig

	// SASL configuration
	TLS TLSConfig

	// Schema registry configuration
	SchemaRegistryConfig SchemaRegistryConfig

	// Env is the environment name on the happi platform, e.g. "dev", "prod", etc. Used for consumer group generation.
	Env string

	// SystemName is the name of the system on the happi platform. Used for consumer group generation.
	SystemName string

	// WorkloadName is the name of the workload on the happi platform. Used for consumer group generation.
	WorkloadName string
}

// ConfigFromEnvVars loads and returns an instance with values that are fetched from environment variables defined
// in this package. If any of these variables do not exist, an error is returned.
func ConfigFromEnvVars() (*Config, error) {
	cfg := Config{}

	authType := strings.ToLower(os.Getenv(envAuthType))
	var authMode AuthMode
	switch authType {
	case "noauth":
		authMode = AuthNone
	case "sasl":
		authMode = AuthSASL
	case "tls":
		authMode = AuthTLS
	default:
		return &Config{}, fmt.Errorf("illegal value %s for env (%s)", authType, envAuthType)
	}

	cfg.AuthMode = authMode

	broker := os.Getenv(envBroker)
	if broker == "" {
		return &Config{}, fmt.Errorf("env is not set (%s)", envBroker)
	}

	cfg.Broker = broker

	caCert := os.Getenv(envCACert)
	if caCert == "" {
		return &Config{}, fmt.Errorf("env is not set (%s)", envCACert)
	}

	cfg.CACert = caCert

	srConfig, err := getSRConfig()
	if err != nil {
		return &Config{}, err
	}
	cfg.SchemaRegistryConfig = *srConfig

	if authType == "sasl" {
		saslConfig, err := getSASLConfig()
		if err != nil {
			return &Config{}, err
		}
		cfg.SASL = *saslConfig
	} else {

		tlsConfig, err := getTLSConfig()
		if err != nil {
			return &Config{}, err
		}

		cfg.TLS = *tlsConfig
	}

	cfg.SystemName = strings.TrimSpace(os.Getenv(envHappiSystemName))
	cfg.WorkloadName = strings.TrimSpace(os.Getenv(envHappiWorkloadName))
	cfg.Env = strings.TrimSpace(os.Getenv(envHappiEnvName))

	if cfg.SystemName == "" {
		return &Config{}, fmt.Errorf("environment variable %s is not set or is empty", envHappiSystemName)
	}

	if cfg.WorkloadName == "" {
		return &Config{}, fmt.Errorf("environment variable %s is not set or is empty", envHappiWorkloadName)
	}

	if cfg.Env == "" {
		return &Config{}, fmt.Errorf("environment variable %s is not set or is empty", envHappiEnvName)
	}

	return &cfg, nil
}

// getSASLConfig returns config for SASL
// If a custom token provider is given, then the scope does not need to be populated, therefore also no empty check
func getSASLConfig() (*SASLConfig, error) {
	return &SASLConfig{
		Scope:       os.Getenv(envAzureScope), // This does only need to be set if using default token provider
		ExpectedOID: os.Getenv("KAFKA_SASL_EXPECTED_OID"),
	}, nil
}

func getTLSConfig() (*TLSConfig, error) {
	certFile := os.Getenv(envCertFile)
	if certFile == "" {
		return &TLSConfig{}, fmt.Errorf("environment variable %s is not set", envCertFile)
	}
	keyFile := os.Getenv(envKeyFile)
	if keyFile == "" {
		return &TLSConfig{}, fmt.Errorf("environment variable %s is not set", envKeyFile)
	}

	return &TLSConfig{
		KeyFile:  keyFile,
		CertFile: certFile,
	}, nil
}

func getSRConfig() (*SchemaRegistryConfig, error) {
	srURL := os.Getenv(envSchemaRegistryURL)
	if srURL == "" {
		return &SchemaRegistryConfig{}, fmt.Errorf("environment variable %s is not set", envSchemaRegistryURL)
	}

	srUser := os.Getenv(envKafkaUser)
	if srUser == "" {
		return &SchemaRegistryConfig{}, fmt.Errorf("environment variable %s is not set", envKafkaUser)
	}

	srPassword := os.Getenv(envKafkaPassword)
	if srPassword == "" {
		return &SchemaRegistryConfig{}, fmt.Errorf("environment variable %s is not set", envKafkaPassword)
	}

	return &SchemaRegistryConfig{
		SchemaRegistryURL:      srURL,
		SchemaRegistryUser:     srUser,
		SchemaRegistryPassword: srPassword,
	}, nil
}
