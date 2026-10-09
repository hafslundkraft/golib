// Package snowflake connects a Happi workload to Snowflake using the workload's
// own platform identity. The Kubernetes token the Happi operator projects into
// the pod is exchanged for one scoped to this Snowflake account, which Snowflake
// accepts as a workload identity credential. Nothing is stored, and no secret is
// configured.
package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/hafslundkraft/golib/identity"
)

const (
	envAccount   = "SNOWFLAKE_ACCOUNT"
	envWarehouse = "SNOWFLAKE_WAREHOUSE"
)

// Connections are recycled regularly, so a long-lived pool keeps reconnecting with
// current tokens instead of relying on sessions opened long ago. Retiring idle
// connections also stops a quiet pool from holding the warehouse awake, which
// costs credits.
const (
	connMaxLifetime = 30 * time.Minute
	connMaxIdleTime = 5 * time.Minute
)

// Config holds the parameters needed to connect to Snowflake.
type Config struct {
	Account    string
	Warehouse  string
	Credential identity.Credential
}

// Option configures a [Config].
type Option func(*Config)

// WithWarehouse overrides the warehouse the operator injected. A HappiJob has no
// manifest field for the warehouse, so this is the only way for a job to ask for
// more compute than the platform default.
func WithWarehouse(warehouse string) Option {
	return func(cfg *Config) {
		cfg.Warehouse = warehouse
	}
}

// NewConfig reads connection parameters from environment variables that are
// automatically set on pods with Snowflake access provisioned through the platform.
func NewConfig(env func(string) string, cred identity.Credential, opts ...Option) (*Config, error) {
	account := env(envAccount)
	if account == "" {
		return nil, fmt.Errorf("missing %s environment variable", envAccount)
	}

	cfg := &Config{
		Account: account,
		// An unset warehouse is left empty rather than defaulted here: the operator
		// owns that default, and repeating it would let the two drift without any
		// error to notice it by.
		Warehouse:  env(envWarehouse),
		Credential: cred,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg, nil
}

// New opens a connection pool to Snowflake and verifies it with a ping. Every
// connection the pool opens authenticates with a current token, so the
// returned pool can be set up once at startup and used for the process's lifetime.
//
// The context is retained by the pool for token refreshes and must not be canceled
// while the pool is still in use.
func New(ctx context.Context, cfg *Config) (*sql.DB, error) {
	db := sql.OpenDB(newConnector(ctx, cfg))
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping snowflake: %w", err)
	}
	return db, nil
}

// audience is the value Snowflake requires in the token's aud claim, sent as the
// RFC 8707 resource parameter when the Kubernetes token is exchanged. It is derived
// rather than configured because the operator writes the same string into the
// service user's OIDC_AUDIENCE_LIST. Snowflake's default audience is the shared
// snowflakecomputing.com, which every Snowflake account accepts, so a token minted
// without it would authenticate against someone else's account.
func (c *Config) audience() string {
	return c.Account + ".snowflakecomputing.com"
}
