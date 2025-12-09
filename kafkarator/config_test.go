package kafkarator

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigFromEnvVars_Success(t *testing.T) {
	// Set up environment variables
	t.Setenv(envBrokers, "broker1:9092,broker2:9092,broker3:9092")
	t.Setenv(envCertFile, "/path/to/cert.pem")
	t.Setenv(envKeyFile, "/path/to/key.pem")
	t.Setenv(envCAFile, "/path/to/ca.pem")

	config, err := ConfigFromEnvVars()

	require.NoError(t, err)
	require.Equal(t, []string{"broker1:9092", "broker2:9092", "broker3:9092"}, config.Brokers)
	require.Equal(t, "/path/to/cert.pem", config.CertFile)
	require.Equal(t, "/path/to/key.pem", config.KeyFile)
	require.Equal(t, "/path/to/ca.pem", config.CAFile)
}

func TestConfigFromEnvVars_SingleBroker(t *testing.T) {
	// Set up environment variables with single broker
	t.Setenv(envBrokers, "localhost:9092")
	t.Setenv(envCertFile, "/cert.pem")
	t.Setenv(envKeyFile, "/key.pem")
	t.Setenv(envCAFile, "/ca.pem")

	config, err := ConfigFromEnvVars()

	require.NoError(t, err)
	require.Equal(t, []string{"localhost:9092"}, config.Brokers)
}

func TestConfigFromEnvVars_BrokersWithWhitespace(t *testing.T) {
	// Test that whitespace around broker names is trimmed
	t.Setenv(envBrokers, " broker1:9092 , broker2:9092 ,  broker3:9092  ")
	t.Setenv(envCertFile, "/path/to/cert.pem")
	t.Setenv(envKeyFile, "/path/to/key.pem")
	t.Setenv(envCAFile, "/path/to/ca.pem")

	config, err := ConfigFromEnvVars()

	require.NoError(t, err)
	require.Equal(t, []string{"broker1:9092", "broker2:9092", "broker3:9092"}, config.Brokers)
}

func TestConfigFromEnvVars_MissingBrokers(t *testing.T) {
	// Ensure brokers is not set
	os.Unsetenv(envBrokers)
	t.Setenv(envCertFile, "/path/to/cert.pem")
	t.Setenv(envKeyFile, "/path/to/key.pem")
	t.Setenv(envCAFile, "/path/to/ca.pem")

	config, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), envBrokers)
	require.Empty(t, config.Brokers)
}

func TestConfigFromEnvVars_MissingCertFile(t *testing.T) {
	t.Setenv(envBrokers, "broker1:9092")
	os.Unsetenv(envCertFile)
	t.Setenv(envKeyFile, "/path/to/key.pem")
	t.Setenv(envCAFile, "/path/to/ca.pem")

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), envCertFile)
}

func TestConfigFromEnvVars_MissingKeyFile(t *testing.T) {
	t.Setenv(envBrokers, "broker1:9092")
	t.Setenv(envCertFile, "/path/to/cert.pem")
	os.Unsetenv(envKeyFile)
	t.Setenv(envCAFile, "/path/to/ca.pem")

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), envKeyFile)
}

func TestConfigFromEnvVars_MissingCAFile(t *testing.T) {
	t.Setenv(envBrokers, "broker1:9092")
	t.Setenv(envCertFile, "/path/to/cert.pem")
	t.Setenv(envKeyFile, "/path/to/key.pem")
	os.Unsetenv(envCAFile)

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), envCAFile)
}

func TestConfigFromEnvVars_AllMissing(t *testing.T) {
	// Ensure all env vars are not set
	os.Unsetenv(envBrokers)
	os.Unsetenv(envCertFile)
	os.Unsetenv(envKeyFile)
	os.Unsetenv(envCAFile)

	config, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), envBrokers)
	require.Empty(t, config.Brokers)
}

func TestConfigFromEnvVars_EmptyBrokers(t *testing.T) {
	// Test empty string vs unset
	t.Setenv(envBrokers, "")
	t.Setenv(envCertFile, "/path/to/cert.pem")
	t.Setenv(envKeyFile, "/path/to/key.pem")
	t.Setenv(envCAFile, "/path/to/ca.pem")

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), envBrokers)
}

func TestConfigFromEnvVars_EmptyPaths(t *testing.T) {
	t.Setenv(envBrokers, "broker1:9092")
	t.Setenv(envCertFile, "")
	t.Setenv(envKeyFile, "/path/to/key.pem")
	t.Setenv(envCAFile, "/path/to/ca.pem")

	_, err := ConfigFromEnvVars()

	require.Error(t, err)
	require.Contains(t, err.Error(), envCertFile)
}
