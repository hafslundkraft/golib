package snowflake

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/snowflakedb/gosnowflake"

	"github.com/hafslundkraft/golib/identity"
)

// connector hands database/sql a current token every time the pool opens
// a physical connection. gosnowflake takes the token as a plain Config field rather
// than as a callback the way pgx does, so this is the only place left to refresh it.
type connector struct {
	cfg        gosnowflake.Config
	credential identity.Credential
	resource   string
}

var _ driver.Connector = (*connector)(nil)

// newConnector builds the config template every connection is stamped from.
func newConnector(cfg *Config) *connector {
	sfCfg := gosnowflake.Config{
		Account: cfg.Account,
		// Empty leaves the Snowflake user's default warehouse in effect.
		Warehouse:                cfg.Warehouse,
		Authenticator:            gosnowflake.AuthTypeWorkloadIdentityFederation,
		WorkloadIdentityProvider: "OIDC",
	}

	return &connector{
		cfg:        sfCfg,
		credential: cfg.Credential,
		resource:   cfg.audience(),
	}
}

// Connect opens one physical connection with a fresh token. database/sql calls
// this whenever the pool grows, which is what lets a long-lived *sql.DB keep
// working: no connection is ever opened with an expired token.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg, err := c.configWithToken(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, cfg).Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("open snowflake connection: %w", err)
	}
	return conn, nil
}

// Driver returns the driver behind this connector, as database/sql requires.
func (c *connector) Driver() driver.Driver {
	return gosnowflake.SnowflakeDriver{}
}

// configWithToken copies the template and stamps a fresh token onto the copy.
// The copy matters: database/sql calls Connect concurrently as the pool grows, and
// a shared config would have those calls overwriting each other's token.
//
// The token source is built per call so the exchange runs under the dial context:
// a canceled dial stops it, and no context has to outlive the pool. That gives up
// token caching, which costs one exchange per physical connection; with
// connections recycled every connMaxLifetime that is rare.
func (c *connector) configWithToken(ctx context.Context) (gosnowflake.Config, error) {
	token, err := c.credential.TokenSource(ctx, identity.WithResource(c.resource)).Token()
	if err != nil {
		return gosnowflake.Config{}, fmt.Errorf("exchange token for snowflake: %w", err)
	}

	cfg := c.cfg
	cfg.Token = token.AccessToken
	return cfg, nil
}
