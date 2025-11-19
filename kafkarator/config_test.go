package kafkarator

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigFromEnvVars_Success(t *testing.T) {
	// Set up environment variables
	t.Setenv(EnvBrokers, "broker1:9092,broker2:9092,broker3:9092")
	t.Setenv(EnvCertFile, "/path/to/cert.pem")
	t.Setenv(EnvKeyFile, "/path/to/key.pem")
	t.Setenv(EnvCAFile, "/path/to/ca.pem")

	config, err := ConfigFromEnvVars()

	require.NoError(t, err)
	require.Equal(t, []string{"broker1:9092", "broker2:9092", "broker3:9092"}, config.Brokers)
	require.Equal(t, "/path/to/cert.pem", config.CertFile)
	require.Equal(t, "/path/to/key.pem", config.KeyFile)
	require.Equal(t, "/path/to/ca.pem", config.CAFile)
}

func TestConfigFromEnvVars_SingleBroker(t *testing.T) {
	// Set up environment variables with single broker
	t.Setenv(EnvBrokers, "localhost:9092")
	t.Setenv(EnvCertFile, "/cert.pem")
	t.Setenv(EnvKeyFile, "/key.pem")
	t.Setenv(EnvCAFile, "/ca.pem")

	config, err := ConfigFromEnvVars()

	require.NoError(t, err)
	require.Equal(t, []string{"localhost:9092"}, config.Brokers)
}

func TestConfigFromEnvVars_BrokersWithWhitespace(t *testing.T) {
	// Test that whitespace around broker names is trimmed
	t.Setenv(EnvBrokers, " broker1:9092 , broker2:9092 ,  broker3:9092  ")
	t.Setenv(EnvCertFile, "/path/to/cert.pem")
	t.Setenv(EnvKeyFile, "/path/to/key.pem")
	t.Setenv(EnvCAFile, "/path/to/ca.pem")

	config, err := ConfigFromEnvVars()

	require.NoError(t, err)
	require.Equal(t, []string{"broker1:9092", "broker2:9092", "broker3:9092"}, config.Brokers)
}

func TestConfigFromEnvVars_MissingBrokers(t *testing.T) {
	// Ensure brokers is not set
	os.Unsetenv(EnvBrokers)
	t.Setenv(EnvCertFile, "/path/to/cert.pem")
	t.Setenv(EnvKeyFile, "/path/to/key.pem")
	t.Setenv(EnvCAFile, "/path/to/ca.pem")

	config, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), EnvBrokers)
	require.Empty(t, config.Brokers)
}

func TestConfigFromEnvVars_MissingCertFile(t *testing.T) {
	t.Setenv(EnvBrokers, "broker1:9092")
	os.Unsetenv(EnvCertFile)
	t.Setenv(EnvKeyFile, "/path/to/key.pem")
	t.Setenv(EnvCAFile, "/path/to/ca.pem")

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), EnvCertFile)
}

func TestConfigFromEnvVars_MissingKeyFile(t *testing.T) {
	t.Setenv(EnvBrokers, "broker1:9092")
	t.Setenv(EnvCertFile, "/path/to/cert.pem")
	os.Unsetenv(EnvKeyFile)
	t.Setenv(EnvCAFile, "/path/to/ca.pem")

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), EnvKeyFile)
}

func TestConfigFromEnvVars_MissingCAFile(t *testing.T) {
	t.Setenv(EnvBrokers, "broker1:9092")
	t.Setenv(EnvCertFile, "/path/to/cert.pem")
	t.Setenv(EnvKeyFile, "/path/to/key.pem")
	os.Unsetenv(EnvCAFile)

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), EnvCAFile)
}

func TestConfigFromEnvVars_AllMissing(t *testing.T) {
	// Ensure all env vars are not set
	os.Unsetenv(EnvBrokers)
	os.Unsetenv(EnvCertFile)
	os.Unsetenv(EnvKeyFile)
	os.Unsetenv(EnvCAFile)

	config, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), EnvBrokers)
	require.Empty(t, config.Brokers)
}

func TestConfigFromEnvVars_EmptyBrokers(t *testing.T) {
	// Test empty string vs unset
	t.Setenv(EnvBrokers, "")
	t.Setenv(EnvCertFile, "/path/to/cert.pem")
	t.Setenv(EnvKeyFile, "/path/to/key.pem")
	t.Setenv(EnvCAFile, "/path/to/ca.pem")

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), EnvBrokers)
}

func TestConfigFromEnvVars_EmptyPaths(t *testing.T) {
	t.Setenv(EnvBrokers, "broker1:9092")
	t.Setenv(EnvCertFile, "")
	t.Setenv(EnvKeyFile, "/path/to/key.pem")
	t.Setenv(EnvCAFile, "/path/to/ca.pem")

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), EnvCertFile)
}
