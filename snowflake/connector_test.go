package snowflake

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/oauth2"

	"github.com/hafslundkraft/golib/identity"
)

type failingTokenSource struct{}

func (failingTokenSource) Token() (*oauth2.Token, error) {
	return nil, errors.New("idp unreachable")
}

func TestNewConnector(t *testing.T) {
	tests := map[string]struct {
		warehouse     string
		wantWarehouse string
	}{
		"warehouse set":   {warehouse: "WH_BIG", wantWarehouse: "WH_BIG"},
		"warehouse empty": {warehouse: "", wantWarehouse: ""},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := newConnector(context.Background(), &Config{
				Account:    "acme",
				Warehouse:  tc.warehouse,
				Credential: fakeCredential{token: "token-1"},
			})

			if c.cfg.Account != "acme" {
				t.Errorf("Account = %q, want %q", c.cfg.Account, "acme")
			}
			if c.cfg.Warehouse != tc.wantWarehouse {
				t.Errorf("Warehouse = %q, want %q", c.cfg.Warehouse, tc.wantWarehouse)
			}
			if c.cfg.Authenticator != gosnowflake.AuthTypeWorkloadIdentityFederation {
				t.Errorf("Authenticator = %v, want workload identity federation", c.cfg.Authenticator)
			}
			if c.cfg.WorkloadIdentityProvider != "OIDC" {
				t.Errorf("WorkloadIdentityProvider = %q, want %q", c.cfg.WorkloadIdentityProvider, "OIDC")
			}
		})
	}
}

func TestConfigWithTokenLeavesTemplateAlone(t *testing.T) {
	c := newConnector(context.Background(), &Config{
		Account:    "acme",
		Credential: fakeCredential{token: "token-1"},
	})

	cfg, err := c.configWithToken()
	if err != nil {
		t.Fatalf("configWithToken: %v", err)
	}
	if cfg.Token != "token-1" {
		t.Errorf("Token = %q, want %q", cfg.Token, "token-1")
	}

	// The template must come back untouched, or concurrent Connect calls would be
	// writing their tokens into shared state.
	if c.cfg.Token != "" {
		t.Errorf("template Token = %q, want it left empty", c.cfg.Token)
	}
}

// TestConnectorRequestsAccountAudience goes through a real WorkloadCredential
// against a fake IdP, because the resource option is opaque outside identity.
// Without it the token gets Snowflake's shared default audience, which every
// account accepts.
func TestConnectorRequestsAccountAudience(t *testing.T) {
	gotResource := make(chan string, 1)
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		gotResource <- r.PostForm.Get("resource")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"access_token":"t","token_type":"Bearer","expires_in":3600}`)
	}))
	defer idp.Close()

	tokenFile := filepath.Join(t.TempDir(), "idp-token")
	if err := os.WriteFile(tokenFile, []byte(fakeJWT(time.Now().Add(time.Hour))), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newConnector(context.Background(), &Config{
		Account:    "acme",
		Credential: &identity.WorkloadCredential{TokenFile: tokenFile, TokenURL: idp.URL},
	})
	if _, err := c.configWithToken(); err != nil {
		t.Fatalf("configWithToken: %v", err)
	}

	if got, want := <-gotResource, "acme.snowflakecomputing.com"; got != want {
		t.Errorf("resource = %q, want %q", got, want)
	}
}

// fakeJWT builds an unsigned JWT carrying only an exp claim, which is all
// identity reads from the projected Kubernetes token.
func fakeJWT(exp time.Time) string {
	enc := base64.RawURLEncoding
	payload := fmt.Sprintf(`{"exp":%d}`, exp.Unix())
	return enc.EncodeToString([]byte(`{"alg":"none"}`)) + "." + enc.EncodeToString([]byte(payload)) + ".sig"
}

func TestConfigWithTokenPropagatesFailure(t *testing.T) {
	c := &connector{tokenSource: failingTokenSource{}}

	if _, err := c.configWithToken(); err == nil {
		t.Fatal("expected an error when the token exchange fails, got none")
	}
}
