package kafkarator

import (
	"fmt"
	"os"
)

const (
	// env is the environment that the app is running in
	envEnv = "ENV"

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

	// envUseSchemaRegistry tells us whether schema registry should be used or not
	envUseSchemaRegistry = "USE_SCHEMA_REGISTRY"
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

// Config contains all necessary configuration needed to connect to Kafka.
type Config struct {
	Env string

	// Broker is the Kafka broker
	Broker string

	// Which authentication mode to use towards Kafka service
	AuthMode string

	// UseSchemaRegistry enables or unenables creation of schema registry client
	UseSchemaRegistry bool

	// CA certificate for the service
	CACert string

	// SASL configuration
	SASL SASLConfig

	// SASL configuration
	TLS TLSConfig

	// Schema registry configuration
	SchemaRegistryConfig SchemaRegistryConfig
}

// ConfigFromEnvVars loads and returns an instance with values that are fetched from environment variables defined
// in this package. If any of these variables do not exist, an error is returned.
func ConfigFromEnvVars() (*Config, error) {
	cfg := Config{}
	env := os.Getenv(envEnv)
	if env == "" {
		return &Config{}, fmt.Errorf("env is not set (%s)", envEnv)
	}
	cfg.Env = env

	authType := os.Getenv(envAuthType)

	if authType == "" {
		return &Config{}, fmt.Errorf("env is not set (%s)", envAuthType)
	}
	cfg.AuthMode = authType

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

	useSchemaRegistry := os.Getenv(envUseSchemaRegistry)

	if useSchemaRegistry != "" && useSchemaRegistry == "true" {
		cfg.UseSchemaRegistry = true
		srConfig, err := getSRConfig()
		if err != nil {
			return &Config{}, err
		}

		cfg.SchemaRegistryConfig = *srConfig
	}

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
