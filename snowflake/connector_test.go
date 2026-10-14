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

	"github.com/hafslundkraft/golib/identity"
)

func TestNewConnector(t *testing.T) {
	c := newConnector(&Config{
		Account:    "acme",
		Warehouse:  "WH_BIG",
		Database:   "INGEST",
		Schema:     "MY_SCHEMA",
		Credential: fakeCredential{token: "token-1"},
	})

	if c.cfg.Account != "acme" {
		t.Errorf("Account = %q, want %q", c.cfg.Account, "acme")
	}
	if c.cfg.Warehouse != "WH_BIG" {
		t.Errorf("Warehouse = %q, want %q", c.cfg.Warehouse, "WH_BIG")
	}
	if c.cfg.Database != "INGEST" {
		t.Errorf("Database = %q, want %q", c.cfg.Database, "INGEST")
	}
	if c.cfg.Schema != "MY_SCHEMA" {
		t.Errorf("Schema = %q, want %q", c.cfg.Schema, "MY_SCHEMA")
	}
	if c.cfg.Authenticator != gosnowflake.AuthTypeWorkloadIdentityFederation {
		t.Errorf("Authenticator = %v, want workload identity federation", c.cfg.Authenticator)
	}
	if c.cfg.WorkloadIdentityProvider != "OIDC" {
		t.Errorf("WorkloadIdentityProvider = %q, want %q", c.cfg.WorkloadIdentityProvider, "OIDC")
	}
}

func TestConfigWithTokenLeavesTemplateAlone(t *testing.T) {
	c := newConnector(&Config{
		Account:    "acme",
		Credential: fakeCredential{token: "token-1"},
	})

	cfg, err := c.configWithToken(context.Background())
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

	c := newConnector(&Config{
		Account:    "acme",
		Credential: &identity.WorkloadCredential{TokenFile: tokenFile, TokenURL: idp.URL},
	})
	if _, err := c.configWithToken(context.Background()); err != nil {
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

// TestConfigWithTokenHonorsDialContext covers why the token source is built per
// call: a canceled dial must stop the exchange instead of it running under some
// context captured when the pool was created.
func TestConfigWithTokenHonorsDialContext(t *testing.T) {
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"access_token":"t","token_type":"Bearer","expires_in":3600}`)
	}))
	defer idp.Close()

	tokenFile := filepath.Join(t.TempDir(), "idp-token")
	if err := os.WriteFile(tokenFile, []byte(fakeJWT(time.Now().Add(time.Hour))), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newConnector(&Config{
		Account:    "acme",
		Credential: &identity.WorkloadCredential{TokenFile: tokenFile, TokenURL: idp.URL},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.configWithToken(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}

	// A canceled earlier dial must not poison later ones.
	if _, err := c.configWithToken(context.Background()); err != nil {
		t.Errorf("configWithToken after a canceled dial: %v", err)
	}
}
