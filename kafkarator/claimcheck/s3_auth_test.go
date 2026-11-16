package claimcheck

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeKeycloak issues tok-1, tok-2, ... and records what it was sent.
type fakeKeycloak struct {
	expiresIn int
	status    int
	requests  atomic.Int32
	scope     atomic.Value
	assertion atomic.Value
}

func (f *fakeKeycloak) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := f.requests.Add(1)
	_ = r.ParseForm()
	f.scope.Store(r.PostForm.Get("scope"))
	f.assertion.Store(r.PostForm.Get("client_assertion"))
	if f.status != 0 {
		w.WriteHeader(f.status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"access_token":"tok-%d","token_type":"Bearer","expires_in":%d}`, n, f.expiresIn)
}

// newTestExchanger wires a tokenExchanger to kc. identity parses exp from the
// service account token, so the IDP token file must hold a well-formed JWT.
func newTestExchanger(t *testing.T, kc *fakeKeycloak) (e *tokenExchanger, idpJWT string) {
	t.Helper()
	srv := httptest.NewServer(kc)
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	payload := fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())
	idpJWT = "e30." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".sig"
	idpFile := filepath.Join(dir, "idp-token")
	require.NoError(t, os.WriteFile(idpFile, []byte(idpJWT), 0o600))

	return &tokenExchanger{
		awsTokenFile: filepath.Join(dir, "ceph_token"),
		tokenSource:  newIDPTokenSource(srv.URL, idpFile, "ceph_rgw", srv.Client()),
	}, idpJWT
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

func TestEnsureFreshToken_WritesTokenFile(t *testing.T) {
	kc := &fakeKeycloak{expiresIn: 300}
	e, idpJWT := newTestExchanger(t, kc)

	require.NoError(t, e.ensureFreshToken(context.Background()))

	assert.Equal(t, "tok-1", readFile(t, e.awsTokenFile))
	assert.Equal(t, "ceph_rgw", kc.scope.Load())
	assert.Equal(t, idpJWT, kc.assertion.Load())
}

func TestEnsureFreshToken_ReusesCachedToken(t *testing.T) {
	kc := &fakeKeycloak{expiresIn: 300}
	e, _ := newTestExchanger(t, kc)

	require.NoError(t, e.ensureFreshToken(context.Background()))
	require.NoError(t, e.ensureFreshToken(context.Background()))

	assert.Equal(t, int32(1), kc.requests.Load())
}

func TestEnsureFreshToken_WritesNewTokenWhenExpired(t *testing.T) {
	// oauth2 treats tokens within 10s of expiry as expired, so 1s forces a refresh.
	kc := &fakeKeycloak{expiresIn: 1}
	e, _ := newTestExchanger(t, kc)

	require.NoError(t, e.ensureFreshToken(context.Background()))
	require.NoError(t, e.ensureFreshToken(context.Background()))

	assert.Equal(t, int32(2), kc.requests.Load())
	assert.Equal(t, "tok-2", readFile(t, e.awsTokenFile))
}

func TestEnsureFreshToken_ErrorKeepsOldFile(t *testing.T) {
	kc := &fakeKeycloak{status: http.StatusInternalServerError}
	e, _ := newTestExchanger(t, kc)
	require.NoError(t, os.WriteFile(e.awsTokenFile, []byte("old"), 0o600))

	require.Error(t, e.ensureFreshToken(context.Background()))

	assert.Equal(t, "old", readFile(t, e.awsTokenFile))
}

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

func TestIDPTokenFileFromEnv(t *testing.T) {
	tests := []struct {
		name, current, legacy, want string
	}{
		{"none set", "", "", defaultIDPTokenFile},
		{"only legacy", "", "/legacy", "/legacy"},
		{"both set, current wins", "/current", "/legacy", "/current"},
		{"blank current falls back to legacy", "  ", "/legacy", "/legacy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envServiceAccountTokenPath, tt.current)
			t.Setenv(envIDPTokenFile, tt.legacy)
			assert.Equal(t, tt.want, idpTokenFileFromEnv())
		})
	}
}

func TestNewTokenExchanger_AcceptsHTTPSURL(t *testing.T) {
	t.Setenv(envIDPIssuerURL, "https://idp.example.com")
	e, err := newTokenExchanger()
	require.NoError(t, err)
	assert.NotNil(t, e.tokenSource)
}
