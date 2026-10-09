package snowflake

import (
	"context"
	"testing"

	"golang.org/x/oauth2"

	"github.com/hafslundkraft/golib/identity"
)

// fakeCredential hands out a fixed token, so the tests never reach the IDP.
type fakeCredential struct {
	token string
}

func (f fakeCredential) TokenSource(_ context.Context, _ ...func(*identity.TokenOptions)) oauth2.TokenSource {
	return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: f.token})
}

func envFrom(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

func TestNewConfig(t *testing.T) {
	tests := map[string]struct {
		env           map[string]string
		opts          []Option
		wantErr       bool
		wantAccount   string
		wantWarehouse string
	}{
		"account and warehouse injected": {
			env:           map[string]string{envAccount: "acme", envWarehouse: "WH_SMALL"},
			wantAccount:   "acme",
			wantWarehouse: "WH_SMALL",
		},
		"warehouse left to the snowflake user default": {
			env:           map[string]string{envAccount: "acme"},
			wantAccount:   "acme",
			wantWarehouse: "",
		},
		"option overrides the injected warehouse": {
			env:           map[string]string{envAccount: "acme", envWarehouse: "WH_SMALL"},
			opts:          []Option{WithWarehouse("WH_BIG")},
			wantAccount:   "acme",
			wantWarehouse: "WH_BIG",
		},
		"missing account": {
			env:     map[string]string{envWarehouse: "WH_SMALL"},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, err := NewConfig(envFrom(tc.env), fakeCredential{token: "t"}, tc.opts...)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewConfig: %v", err)
			}
			if cfg.Account != tc.wantAccount {
				t.Errorf("Account = %q, want %q", cfg.Account, tc.wantAccount)
			}
			if cfg.Warehouse != tc.wantWarehouse {
				t.Errorf("Warehouse = %q, want %q", cfg.Warehouse, tc.wantWarehouse)
			}
		})
	}
}

func TestConfigAudience(t *testing.T) {
	cfg := &Config{Account: "acme"}

	// Snowflake's default audience is the shared snowflakecomputing.com, which every
	// account accepts. Scoping it to this account is what keeps a token minted here
	// from authenticating against someone else's.
	if got, want := cfg.audience(), "acme.snowflakecomputing.com"; got != want {
		t.Errorf("audience() = %q, want %q", got, want)
	}
}
