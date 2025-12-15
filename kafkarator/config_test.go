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

func TestSplitAndCleanBrokers(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		{"single", "broker1:9092", []string{"broker1:9092"}, false},
		{"multiple", "a:1, b:2 ,c:3", []string{"a:1", "b:2", "c:3"}, false},
		{"empty parts", " , , ", nil, true},
		{"empty string", "", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitAndCleanBrokers(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetBrokers_InvalidAuthMode(t *testing.T) {
	withEnv(t, nil, func() {
		_, err := getBrokers("prod", "invalid")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported auth mode")
	})
}

func TestGetTLSConfig_Success(t *testing.T) {
	withEnv(t, map[string]string{
		envCertFile: "cert.pem",
		envKeyFile:  "key.pem",
		envCAFile:   "ca.pem",
	}, func() {
		cfg, err := getTLSConfig()

		require.NoError(t, err)
		assert.Equal(t, "cert.pem", cfg.CertFile)
		assert.Equal(t, "key.pem", cfg.KeyFile)
		assert.Equal(t, "ca.pem", cfg.CAFile)
	})
}

func TestGetTLSConfig_MissingCert(t *testing.T) {
	withEnv(t, map[string]string{
		envKeyFile: "key.pem",
		envCAFile:  "ca.pem",
	}, func() {
		_, err := getTLSConfig()

		require.Error(t, err)
		assert.Contains(t, err.Error(), envCertFile)
	})
}

func TestGetSASLConfig_Success(t *testing.T) {
	withEnv(t, map[string]string{
		envAzureOID: "api://scope",
	}, func() {
		cfg, err := getSASLConfig()

		require.NoError(t, err)
		assert.Equal(t, "api://scope", cfg.OID)
	})
}

func TestGetSASLConfig_MissingOID(t *testing.T) {
	withEnv(t, map[string]string{
		envAzureOID: "",
	}, func() {
		_, err := getSASLConfig()

		require.Error(t, err)
		assert.Contains(t, err.Error(), envAzureOID)
	})
}

func TestGetSRConfig_Success(t *testing.T) {
	withEnv(t, map[string]string{
		envSchemaRegistryPassword: "secret",
	}, func() {
		cfg, err := getSRConfig("test")

		require.NoError(t, err)
		assert.Equal(t, "secret", cfg.SchemaRegistryPassword)
		assert.NotEmpty(t, cfg.SchemaRegistryURL)
	})
}

func TestGetSRConfig_MissingPassword(t *testing.T) {
	withEnv(t, map[string]string{
		envSchemaRegistryPassword: "",
	}, func() {
		_, err := getSRConfig("test")

		require.Error(t, err)
		assert.Contains(t, err.Error(), envSchemaRegistryPassword)
	})
}

func TestConfigFromEnvVars_TLS(t *testing.T) {
	withEnv(t, map[string]string{
		envEnv:      "test",
		envAuthMode: "tls",
		envCertFile: "cert.pem",
		envKeyFile:  "key.pem",
		envCAFile:   "ca.pem",
	}, func() {
		cfg, err := ConfigFromEnvVars()

		require.NoError(t, err)
		assert.Equal(t, "test", cfg.Env)
		assert.Equal(t, "tls", cfg.AuthMode)
		assert.NotEmpty(t, cfg.Brokers)
	})
}
