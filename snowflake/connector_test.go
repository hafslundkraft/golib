package snowflake

import (
	"context"
	"errors"
	"testing"

	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/oauth2"
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

func TestConfigWithTokenPropagatesFailure(t *testing.T) {
	c := &connector{tokenSource: failingTokenSource{}}

	if _, err := c.configWithToken(); err == nil {
		t.Fatal("expected an error when the token exchange fails, got none")
	}
}
