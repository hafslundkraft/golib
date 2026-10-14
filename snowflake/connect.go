// Package snowflake connects a Happi workload to Snowflake using the workload's
// own platform identity. The Kubernetes token the Happi operator projects into
// the pod is exchanged for one scoped to this Snowflake account, which Snowflake
// accepts as a workload identity credential. Nothing is stored, and no secret is
// configured.
package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/hafslundkraft/golib/identity"
)

const (
	envAccount   = "SNOWFLAKE_ACCOUNT"
	envWarehouse = "SNOWFLAKE_WAREHOUSE"
)

// A Snowflake session can be renewed only until its master token expires (about
// four hours); after that the driver must log in again, and an open connection
// holds only the token it was opened with. An hour keeps every login on a fresh
// token with room for long queries and transactions.
const connMaxLifetime = time.Hour

// Config holds the parameters needed to connect to Snowflake.
type Config struct {
	Account    string
	Warehouse  string
	Database   string
	Schema     string
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

// WithDatabase sets the session's default database, so queries can name tables
// without the database prefix. Without it, tables must be fully qualified.
func WithDatabase(database string) Option {
	return func(cfg *Config) {
		cfg.Database = database
	}
}

// WithSchema sets the session's default schema, so queries can name tables
// without the schema prefix. It requires [WithDatabase], since a schema resolves
// within a database; [NewConfig] returns an error otherwise.
func WithSchema(schema string) Option {
	return func(cfg *Config) {
		cfg.Schema = schema
	}
}

// NewConfig reads connection parameters from environment variables that are
// automatically set on pods with Snowflake access provisioned through the platform.
// SNOWFLAKE_ACCOUNT is required; SNOWFLAKE_WAREHOUSE is optional.
func NewConfig(env func(string) string, cred identity.Credential, opts ...Option) (*Config, error) {
	account := env(envAccount)
	if account == "" {
		return nil, fmt.Errorf("missing %s environment variable", envAccount)
	}

	cfg := &Config{
		Account: account,
		// The operator injects its default warehouse when the manifest sets none.
		Warehouse:  env(envWarehouse),
		Credential: cred,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.Schema != "" && cfg.Database == "" {
		return nil, errors.New("WithSchema requires WithDatabase")
	}
	return cfg, nil
}

// Open opens a connection pool to Snowflake and logs in once with a ping, so a
// failing token exchange or login is reported here rather than on the first query.
// Every connection the pool opens authenticates with a fresh token, so the returned
// pool can be set up once at startup and used for the process's lifetime.
//
// ctx bounds only the initial ping; canceling it later does not affect the pool.
func Open(ctx context.Context, cfg *Config) (*sql.DB, error) {
	db := sql.OpenDB(newConnector(cfg))
	db.SetConnMaxLifetime(connMaxLifetime)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping snowflake: %w", err)
	}
	return db, nil
}

// audience is the aud claim Snowflake requires, requested as the RFC 8707 resource
// when the Kubernetes token is exchanged. It must match the OIDC_AUDIENCE_LIST the
// operator sets on the service user, which it derives from the account the same way.
func (c *Config) audience() string {
	return c.Account + ".snowflakecomputing.com"
}
