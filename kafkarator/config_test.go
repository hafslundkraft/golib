package kafkarator

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withEnv(t *testing.T, env map[string]string, fn func()) {
	t.Helper()

	orig := map[string]string{}
	for k := range env {
		orig[k] = os.Getenv(k)
	}

	for k, v := range env {
		if v == "" {
			_ = os.Unsetenv(k)
		} else {
			_ = os.Setenv(k, v)
		}
	}

	defer func() {
		for k, v := range orig {
			if v == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, v)
			}
		}
	}()
	fn()
}

func TestGetTLSConfig_Success(t *testing.T) {
	withEnv(t, map[string]string{
		envCertFile: "cert.pem",
		envKeyFile:  "key.pem",
		envCACert:   "ca.pem",
	}, func() {
		cfg, err := getTLSConfig()

		require.NoError(t, err)
		assert.Equal(t, "cert.pem", cfg.CertFile)
		assert.Equal(t, "key.pem", cfg.KeyFile)
		assert.Equal(t, "ca.pem", cfg.CACert)
	})
}

func TestGetTLSConfig_MissingCert(t *testing.T) {
	withEnv(t, map[string]string{
		envKeyFile: "key.pem",
		envCACert:  "ca.pem",
	}, func() {
		_, err := getTLSConfig()

		require.Error(t, err)
		assert.Contains(t, err.Error(), envCertFile)
	})
}

func TestGetSASLConfig_Success(t *testing.T) {
	withEnv(t, map[string]string{
		envAzureScope: "api://scope",
	}, func() {
		cfg, err := getSASLConfig()

		require.NoError(t, err)
		assert.Equal(t, "api://scope", cfg.Scope)
	})
}

func TestGetSASLConfig_MissingScope(t *testing.T) {
	withEnv(t, map[string]string{
		envAzureScope: "",
	}, func() {
		_, err := getSASLConfig()

		require.Error(t, err)
		assert.Contains(t, err.Error(), envAzureScope)
	})
}

func TestGetSRConfig_Success(t *testing.T) {
	withEnv(t, map[string]string{
		envKafkaUser:         "user",
		envSchemaRegistryURL: "url.com",
		envKafkaPassword:     "secret",
	}, func() {
		cfg, err := getSRConfig()

		require.NoError(t, err)
		assert.Equal(t, "secret", cfg.SchemaRegistryPassword)
		assert.NotEmpty(t, cfg.SchemaRegistryURL)
	})
}

func TestGetSRConfig_MissingPassword(t *testing.T) {
	withEnv(t, map[string]string{
		envKafkaUser:         "user",
		envSchemaRegistryURL: "url.com",
		envKafkaPassword:     "",
	}, func() {
		_, err := getSRConfig()

		require.Error(t, err)
		assert.Contains(t, err.Error(), envKafkaPassword)
	})
}

func TestConfigFromEnvVars_TLS(t *testing.T) {
	withEnv(t, map[string]string{
		envEnv:      "test",
		envAuthType: "tls",
		envCertFile: "cert.pem",
		envKeyFile:  "key.pem",
		envCACert:   "ca.pem",
		envBroker:   "broker:9090",
	}, func() {
		cfg, err := ConfigFromEnvVars()

		require.NoError(t, err)
		assert.Equal(t, "test", cfg.Env)
		assert.Equal(t, "tls", cfg.AuthMode)
		assert.NotEmpty(t, cfg.Broker)
	})
}
