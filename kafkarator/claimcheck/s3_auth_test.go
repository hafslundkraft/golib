package claimcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIdentityTokenRetriever_RejectsHTTPURL(t *testing.T) {
	t.Setenv(envIDPIssuerURL, "http://idp.example.com")
	_, err := newIdentityTokenRetriever()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https://")
}

func TestNewIdentityTokenRetriever_RejectsNonURL(t *testing.T) {
	t.Setenv(envIDPIssuerURL, "not-a-url")
	_, err := newIdentityTokenRetriever()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https://")
}

func TestNewIdentityTokenRetriever_AcceptsHTTPSURL(t *testing.T) {
	t.Setenv(envIDPIssuerURL, "https://idp.example.com")
	_, err := newIdentityTokenRetriever()
	require.NoError(t, err)
}
