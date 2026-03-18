package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hafslundkraft/golib/identity"
)

// NewConfig reads connection parameters from environment variables that are
// automatically set on pods with a database provisioned through the platform.
func NewConfig(env func(string) string, cred identity.Credential, opts ...Option) (*Config, error) {
	user := env("POSTGRES_USER")
	if user == "" {
		return nil, fmt.Errorf("missing POSTGRES_USER environment variable")
	}
	db := env("POSTGRES_DB")
	if db == "" {
		return nil, fmt.Errorf("missing POSTGRES_DB environment variable")
	}
	rwHost := env("POSTGRES_HOST_RW")
	if rwHost == "" {
		return nil, fmt.Errorf("missing POSTGRES_HOST_RW environment variable")
	}
	roHost := env("POSTGRES_HOST_RO")
	if roHost == "" {
		return nil, fmt.Errorf("missing POSTGRES_HOST_RO environment variable")
	}
	cfg := &Config{
		User:       user,
		Database:   db,
		RWHost:     rwHost,
		ROHost:     roHost,
		Credential: cred,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg, nil
}

// Option configures a [Config].
type Option func(*Config)

// WithTracer sets a [pgx.QueryTracer] to instrument queries, e.g. with otelpgx.
func WithTracer(tracer pgx.QueryTracer) Option {
	return func(cfg *Config) {
		cfg.tracer = tracer
	}
}

// Config holds the parameters needed to connect to a PostgreSQL database.
type Config struct {
	User       string
	Database   string
	RWHost     string
	ROHost     string
	Credential identity.Credential
	tracer     pgx.QueryTracer
}

// New creates a connection pool to the read-write host and verifies
// connectivity with a ping.
func New(ctx context.Context, cfg *Config) (*pgxpool.Pool, error) {
	return newPool(ctx, cfg, cfg.RWHost)
}

// NewReadOnly creates a connection pool to the read-only host and verifies
// connectivity with a ping.
func NewReadOnly(ctx context.Context, cfg *Config) (*pgxpool.Pool, error) {
	return newPool(ctx, cfg, cfg.ROHost)
}

// Connect opens a single connection to the read-write host and verifies
// connectivity with a ping. Unlike [New], this connection will not
// automatically recover from interruptions. Prefer this over a pool when
// you rely on session-scoped state such as temporary tables or SET variables.
func Connect(ctx context.Context, cfg *Config) (*pgx.Conn, error) {
	return newConnection(ctx, cfg, cfg.RWHost)
}

// ConnectReadOnly opens a single connection to the read-only host and verifies
// connectivity with a ping. See [Connect] for guidance on when to prefer a
// single connection over a pool.
func ConnectReadOnly(ctx context.Context, cfg *Config) (*pgx.Conn, error) {
	return newConnection(ctx, cfg, cfg.ROHost)
}

func newConnection(ctx context.Context, cfg *Config, host string) (*pgx.Conn, error) {
	pgCfg, err := pgx.ParseConfig(connectionString(cfg, host))
	if err != nil {
		return nil, fmt.Errorf("create config: %w", err)
	}

	pgCfg.OAuthTokenProvider = tokenProvider(ctx, cfg.Credential)
	pgCfg.Tracer = cfg.tracer

	db, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	if err := db.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return db, nil
}

func newPool(ctx context.Context, cfg *Config, host string) (*pgxpool.Pool, error) {
	pgCfg, err := pgxpool.ParseConfig(connectionString(cfg, host))
	if err != nil {
		return nil, fmt.Errorf("create config: %w", err)
	}

	pgCfg.ConnConfig.OAuthTokenProvider = tokenProvider(ctx, cfg.Credential)
	pgCfg.ConnConfig.Tracer = cfg.tracer

	pool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping pool: %w", err)
	}

	return pool, nil
}

func connectionString(cfg *Config, host string) string {
	return fmt.Sprintf("postgres://%s@%s:5432/%s?sslmode=disable", cfg.User, host, cfg.Database)
}

func tokenProvider(ctx context.Context, cred identity.Credential) func(context.Context) (string, error) {
	tokenSource := cred.TokenSource(ctx, identity.WithScopes(writeScope))

	return func(_ context.Context) (string, error) {
		token, err := tokenSource.Token()
		if err != nil {
			return "", fmt.Errorf("get token: %w", err)
		}
		return token.AccessToken, nil
	}
}

const writeScope = "postgres_write"
