package claimcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTokenExchanger_RejectsHTTPURL(t *testing.T) {
	t.Setenv(envIDPIssuerURL, "http://idp.example.com")
	_, err := newTokenExchanger()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https://")
}

func TestNewTokenExchanger_RejectsNonURL(t *testing.T) {
	t.Setenv(envIDPIssuerURL, "not-a-url")
	_, err := newTokenExchanger()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https://")
}

func TestNewTokenExchanger_AcceptsHTTPSURL(t *testing.T) {
	t.Setenv(envIDPIssuerURL, "https://idp.example.com")
	_, err := newTokenExchanger()
	require.NoError(t, err)
}
