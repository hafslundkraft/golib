package kafkarator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithReaderAutoOffsetReset_Valid(t *testing.T) {
	ro := defaultReaderOptions()
	opt := WithReaderAutoOffsetReset(OffsetLatest)

	err := opt(&ro)
	require.NoError(t, err)
	assert.Equal(t, OffsetLatest, ro.autoOffsetReset)
}

func TestWithReaderAutoOffsetReset_Invalid(t *testing.T) {
	ro := defaultReaderOptions()
	opt := WithReaderAutoOffsetReset(AutoOffsetReset("invalid"))

	err := opt(&ro)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid AutoOffsetReset")
}

func TestWithProcessorAutoOffsetReset_Valid(t *testing.T) {
	cfg := defaultProcessorConfig()
	opt := WithProcessorAutoOffsetReset(OffsetLatest)

	err := opt(&cfg)
	require.NoError(t, err)
	assert.Equal(t, OffsetLatest, cfg.autoOffsetReset)
}

func TestWithProcessorAutoOffsetReset_Invalid(t *testing.T) {
	cfg := defaultProcessorConfig()
	opt := WithProcessorAutoOffsetReset(AutoOffsetReset("invalid"))

	err := opt(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid AutoOffsetReset")
}
