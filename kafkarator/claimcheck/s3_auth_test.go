package claimcheck

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/identity"
)

func TestGetIdentityToken_ReturnsIDPToken(t *testing.T) {
	keycloak := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"access_token":"s3-token","token_type":"Bearer","expires_in":300}`)
	}))
	t.Cleanup(keycloak.Close)

	// identity reads exp from the service account token, so it must be a well-formed JWT.
	payload := fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())
	idpJWT := "e30." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".sig"
	idpFile := filepath.Join(t.TempDir(), "idp-token")
	require.NoError(t, os.WriteFile(idpFile, []byte(idpJWT), 0o600))

	cred := &identity.WorkloadCredential{TokenFile: idpFile, TokenURL: keycloak.URL + "/token"}
	r := tokenSourceRetriever{src: newIDPTokenSource(cred, "ceph_rgw", keycloak.Client())}

	tok, err := r.GetIdentityToken()
	require.NoError(t, err)
	assert.Equal(t, "s3-token", string(tok))
}

func TestNewIdentityTokenRetriever_IssuerURL(t *testing.T) {
	tests := []struct {
		name, url string
		wantErr   bool
	}{
		{"https accepted", "https://idp.example.com", false},
		{"http rejected", "http://idp.example.com", true},
		{"non-URL rejected", "not-a-url", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envIDPIssuerURL, tt.url)
			_, err := newIdentityTokenRetriever()
			if tt.wantErr {
				require.ErrorContains(t, err, "https://")
				return
			}
			require.NoError(t, err)
		})
	}
}
