package snowflake

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/oauth2"

	"github.com/hafslundkraft/golib/identity"
)

// connector hands database/sql a freshly exchanged token every time the pool opens
// a physical connection. gosnowflake takes the token as a plain Config field rather
// than as a callback the way pgx does, so this is the only place left to refresh it.
type connector struct {
	cfg         gosnowflake.Config
	tokenSource oauth2.TokenSource
}

var _ driver.Connector = (*connector)(nil)

// newConnector builds the config template every connection is stamped from. The
// context is retained by the token source and used for its HTTP requests, so it
// must outlive the pool.
func newConnector(ctx context.Context, cfg *Config) *connector {
	sfCfg := gosnowflake.Config{
		Account:                  cfg.Account,
		Authenticator:            gosnowflake.AuthTypeWorkloadIdentityFederation,
		WorkloadIdentityProvider: "OIDC",
	}
	// An empty warehouse is left unset rather than assigned, so the Snowflake
	// user's own default applies when the platform injected none. Assigning ""
	// would instead run the session with no warehouse at all.
	if cfg.Warehouse != "" {
		sfCfg.Warehouse = cfg.Warehouse
	}

	return &connector{
		cfg:         sfCfg,
		tokenSource: cfg.Credential.TokenSource(ctx, identity.WithResource(cfg.audience())),
	}
}

// Connect opens one physical connection with a freshly exchanged token. database/sql
// calls this whenever the pool grows, which is what lets a long-lived *sql.DB keep
// working: no connection is ever opened with a token older than itself.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg, err := c.configWithToken()
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

// configWithToken copies the template and stamps a current token onto the copy.
// The copy matters: database/sql calls Connect concurrently as the pool grows, and
// a shared config would have those calls overwriting each other's token.
func (c *connector) configWithToken() (gosnowflake.Config, error) {
	token, err := c.tokenSource.Token()
	if err != nil {
		return gosnowflake.Config{}, fmt.Errorf("exchange token for snowflake: %w", err)
	}

	cfg := c.cfg
	cfg.Token = token.AccessToken
	return cfg, nil
}
