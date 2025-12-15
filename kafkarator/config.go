package kafkarator

import (
	"fmt"
	"os"
	"strings"
)

const (
	// env is the environment that the app is running in
	envEnv = "ENV"

	// envBrokers is the environment variable for Kafka broker addresses (comma-separated)
	envBrokers = "KAFKA_BROKERS"

	// envCertFile is the environment variable for the Kafka certificate file path
	envCertFile = "KAFKA_CERT_FILE"

	// envKeyFile is the environment variable for the Kafka key file path
	envKeyFile = "KAFKA_KEY_FILE"

	// envCAFile is the environment variable for the Kafka CA file path
	envCAFile = "KAFKA_CA_FILE"

	// envAzureOID is the client object id for the app in Azure
	envAzureScope = "AZURE_SCOPE"

	// envAuthMode tells which auth mode to use for Kafka
	envAuthMode = "KAFKA_AUTH_MODE"

	// envSchemaRegistryUser is the user for the Aiven Schema Registry
	envSchemaRegistryUser = "SCHEMA_REGISTRY_USER"

	// envSchemaRegistryPassword is the password for the Aiven Schema Registry user
	// #nosec G101 -- This is an environment variable *name*, not a credential.
	envSchemaRegistryPassword = "SCHEMA_REGISTRY_PASSWORD"

	// envSchemaRegistryURL is the URL for the Aiven Schema Registry
	envSchemaRegistryURL = "SCHEMA_REGISTRY_URL"

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

	// CAFile is the certificate authority file.
	CAFile string
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

	// Brokers is the list of Kafka brokers
	Brokers []string

	// Which authentication mode to use towards Kafka service
	AuthMode string

	// UseSchemaRegistry enables or unenables creation of schema registry client
	UseSchemaRegistry bool

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

	authMode := os.Getenv(envAuthMode)

	if authMode == "" {
		return &Config{}, fmt.Errorf("env is not set (%s)", envAuthMode)
	}
	cfg.AuthMode = authMode

	brokers, err := getBrokers(env, authMode)
	if err != nil {
		return nil, err
	}
	cfg.Brokers = brokers

	useSchemaRegistry := os.Getenv(envUseSchemaRegistry)

	if useSchemaRegistry != "" && useSchemaRegistry == "true" {
		cfg.UseSchemaRegistry = true
		srConfig, err := getSRConfig(env)
		if err != nil {
			return &Config{}, err
		}

		cfg.SchemaRegistryConfig = *srConfig
	}

	if authMode == "sasl" {
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

func getSASLConfig() (*SASLConfig, error) {
	scope := os.Getenv(envAzureScope)
	if scope == "" {
		return &SASLConfig{}, fmt.Errorf("env variable %s is not set for SASL", envAzureScope)
	}

	return &SASLConfig{
		Scope:       scope,
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
	caFile := os.Getenv(envCAFile)
	if caFile == "" {
		return &TLSConfig{}, fmt.Errorf("environment variable %s is not set", envCAFile)
	}

	return &TLSConfig{
		KeyFile:  keyFile,
		CAFile:   caFile,
		CertFile: certFile,
	}, nil
}

func getSRConfig(env string) (*SchemaRegistryConfig, error) {
	srURL := os.Getenv(envSchemaRegistryURL)
	if srURL == "" {
		switch env {
		case "prod":
			srURL = kafkaProdSchemaRegistryURL
		case "test":
			srURL = kafkaTestSchemaRegistryURL
		default:
		}
	}

	srUser := os.Getenv(envSchemaRegistryUser)
	if srUser == "" {
		srUser = kafkaUsername
	}

	srPassword := os.Getenv(envSchemaRegistryPassword)
	if srPassword == "" {
		return &SchemaRegistryConfig{}, fmt.Errorf("environment variable %s is not set", envSchemaRegistryPassword)
	}

	return &SchemaRegistryConfig{
		SchemaRegistryURL:      srURL,
		SchemaRegistryUser:     srUser,
		SchemaRegistryPassword: srPassword,
	}, nil
}

// getBrokers resolves the Kafka broker list based on:
//  1. Explicit KAFKA_BROKERS env var (always wins if set)
//  2. Environment (prod / test / local)
//  3. Auth mode (e.g. sasl / tls)
//
// It returns a cleaned slice suitable for kafka.ConfigMap.
func getBrokers(env, authMode string) ([]string, error) {
	// 1. Explicit override always wins
	if brokers := strings.TrimSpace(os.Getenv(envBrokers)); brokers != "" {
		return splitAndCleanBrokers(brokers)
	}

	// 2. Defaults based on env + auth mode
	var brokers string

	switch env {
	case "prod":
		switch authMode {
		case "sasl":
			brokers = kafkaProdBrokerSASL
		case "tls":
			brokers = kafkaProdBrokerTLS
		default:
			return nil, fmt.Errorf("unsupported auth mode %q for env=prod", authMode)
		}

	case "test":
		switch authMode {
		case "sasl":
			brokers = kafkaTestBrokerSASL
		case "tls":
			brokers = kafkaTestBrokerTLS
		default:
			return nil, fmt.Errorf("unsupported auth mode %q for env=test", authMode)
		}

	case "local":
		switch authMode {
		case "tls":
			brokers = kafkaTestBrokerTLS
		default:
			return nil, fmt.Errorf(
				"KAFKA_BROKERS must be set for env=local with authMode=%q",
				authMode,
			)
		}

	default:
		return nil, fmt.Errorf("unknown ENV=%q", env)
	}

	if brokers == "" {
		return nil, fmt.Errorf(
			"no default brokers configured for env=%q authMode=%q",
			env, authMode,
		)
	}

	return splitAndCleanBrokers(brokers)
}

func splitAndCleanBrokers(brokers string) ([]string, error) {
	parts := strings.Split(brokers, ",")
	out := make([]string, 0, len(parts))

	for _, b := range parts {
		b = strings.TrimSpace(b)
		if b != "" {
			out = append(out, b)
		}
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("broker list resolved to empty")
	}

	return out, nil
}
