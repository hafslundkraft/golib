package snowflake

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/snowflakedb/gosnowflake"

	"github.com/hafslundkraft/golib/identity"
)

// connector gives every new connection a fresh token. gosnowflake takes the token
// as a plain Config field, not a callback, so this is where it gets refreshed.
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
		// Empty leaves the default warehouse of the workload's Snowflake user in effect.
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
// it whenever the pool needs a new connection.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg, err := c.configWithToken(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, cfg).Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return conn, nil
}

// Driver returns the driver behind this connector, as database/sql requires.
func (c *connector) Driver() driver.Driver {
	return gosnowflake.SnowflakeDriver{}
}

// configWithToken returns a copy of c.cfg with a fresh token. It copies because
// database/sql calls Connect concurrently. The token source is built per call so
// a canceled dial stops the exchange, at the cost of one exchange per connection.
func (c *connector) configWithToken(ctx context.Context) (gosnowflake.Config, error) {
	token, err := c.credential.TokenSource(ctx, identity.WithResource(c.resource)).Token()
	if err != nil {
		return gosnowflake.Config{}, fmt.Errorf("exchange token: %w", err)
	}

	cfg := c.cfg
	cfg.Token = token.AccessToken
	return cfg, nil
}
