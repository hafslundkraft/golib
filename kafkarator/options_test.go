package kafkarator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWithReaderAutoOffsetReset_InvalidReturnsError verifies that passing an
// invalid AutoOffsetReset returns a non-nil option that yields an error when
// applied, rather than returning a nil option (which would panic when invoked).
func TestWithReaderAutoOffsetReset_InvalidReturnsError(t *testing.T) {
	opt := WithReaderAutoOffsetReset(AutoOffsetReset("bogus"))
	require.NotNil(t, opt, "option must not be nil (would cause nil-func panic)")

	ro := defaultReaderOptions()
	err := opt(&ro)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithReaderAutoOffsetReset")
}

func TestWithReaderAutoOffsetReset_ValidSucceeds(t *testing.T) {
	opt := WithReaderAutoOffsetReset(OffsetLatest)
	require.NotNil(t, opt)

	ro := defaultReaderOptions()
	require.NoError(t, opt(&ro))
	assert.Equal(t, OffsetLatest, ro.autoOffsetReset)
}

func TestWithProcessorAutoOffsetReset_InvalidReturnsError(t *testing.T) {
	opt := WithProcessorAutoOffsetReset(AutoOffsetReset("bogus"))
	require.NotNil(t, opt, "option must not be nil (would cause nil-func panic)")

	cfg := defaultProcessorConfig()
	err := opt(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithProcessorAutoOffsetReset")
}

func TestWithProcessorAutoOffsetReset_ValidSucceeds(t *testing.T) {
	opt := WithProcessorAutoOffsetReset(OffsetLatest)
	require.NotNil(t, opt)

	cfg := defaultProcessorConfig()
	require.NoError(t, opt(&cfg))
	assert.Equal(t, OffsetLatest, cfg.autoOffsetReset)
}

func TestWithProcessorReadTimeout_NegativeReturnsError(t *testing.T) {
	opt := WithProcessorReadTimeout(-1 * time.Second)
	cfg := defaultProcessorConfig()

	err := opt(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithProcessorReadTimeout")
	// The default must remain untouched on error.
	assert.Equal(t, 10*time.Second, cfg.readTimeout)
}

func TestWithProcessorReadTimeout_ZeroAndPositiveOK(t *testing.T) {
	cfg := defaultProcessorConfig()
	require.NoError(t, WithProcessorReadTimeout(0)(&cfg))
	assert.Equal(t, time.Duration(0), cfg.readTimeout)

	require.NoError(t, WithProcessorReadTimeout(3*time.Second)(&cfg))
	assert.Equal(t, 3*time.Second, cfg.readTimeout)
}

func TestWithProcessorMaxMessages_InvalidReturnsError(t *testing.T) {
	cfg := defaultProcessorConfig()
	err := WithProcessorMaxMessages(0)(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithProcessorMaxMessages")

	err = WithProcessorMaxMessages(-5)(&cfg)
	require.Error(t, err)
	// The default must remain untouched on error.
	assert.Equal(t, 10, cfg.maxMessages)
}

func TestWithProcessorMaxMessages_ValidSucceeds(t *testing.T) {
	cfg := defaultProcessorConfig()
	require.NoError(t, WithProcessorMaxMessages(42)(&cfg))
	assert.Equal(t, 42, cfg.maxMessages)
}

// TestSASLScopeErrorMessage verifies that the error surfaced when SASL is
// selected but no scope is provided references the correct env variable name
// (AZURE_KAFKA_SCOPE), not the non-existent KAFKA_SASL_SCOPE.
func TestSASLScopeErrorMessage(t *testing.T) {
	cfg := &Config{
		AuthMode: AuthSASL,
		Broker:   "broker:9092",
		CACert:   "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
		SASL:     SASLConfig{Scope: ""},
	}

	_, err := NewConnection(cfg, newMockTelemetry())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AZURE_KAFKA_SCOPE")
	assert.NotContains(t, err.Error(), "KAFKA_SASL_SCOPE")
}
