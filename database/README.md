# database/postgres

Library for connecting to PostgreSQL databases on the Happi Platform
using OAuth token-based authentication. Supports both connection pools
and single connections, with read-write and read-only host separation.

Includes helpers for retrying transactions on transient errors
(connection exceptions, serialization failures) and for classifying
common PostgreSQL error codes.

## Minimal example

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/hafslundkraft/golib/database/postgres"
	"github.com/hafslundkraft/golib/identity"
)

func main() {
	ctx := context.Background()

	cred, err := identity.NewWorkloadCredential()
	if err != nil {
		log.Fatal(err)
	}

	pgCfg, err := postgres.NewConfig(os.Getenv, cred)
	if err != nil {
		log.Fatal(err)
	}

	db, err := postgres.New(ctx, pgCfg)
	if err != nil {
		log.Fatal(err)
	}

	var result int
	if err := db.QueryRow(ctx, "SELECT 42").Scan(&result); err != nil {
		log.Fatal(err)
	}
	log.Println(result)
}
```

## Retrying transactions

`TryTxFunc` works exactly like `pgx.BeginTxFunc`, but retries the
transaction on transient errors (connection exceptions, serialization
failures) with exponential backoff.

```go
err := postgres.TryTxFunc(ctx, db, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance - @amount WHERE id = @from",
		pgx.NamedArgs{"amount": 100, "from": 1}); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance + @amount WHERE id = @to",
		pgx.NamedArgs{"amount": 100, "to": 2}); err != nil {
		return err
	}
	return nil
})
```

## Tracing

Use the `WithTracer` option to instrument queries with any `pgx.QueryTracer`
implementation. For example, with `otelpgx` for OpenTelemetry:

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/exaring/otelpgx"

	"github.com/hafslundkraft/golib/database/postgres"
	"github.com/hafslundkraft/golib/identity"
	"github.com/hafslundkraft/golib/telemetry"
)

func main() {
	ctx := context.Background()

	provider, shutdown := telemetry.New(ctx, "my-service")
	defer shutdown(ctx)

	cred, err := identity.NewWorkloadCredential()
	if err != nil {
		log.Fatal(err)
	}

	pgCfg, err := postgres.NewConfig(os.Getenv, cred,
		postgres.WithTracer(otelpgx.NewTracer(otelpgx.WithTracerProvider(provider.TracerProvider()))),
	)
	if err != nil {
		log.Fatal(err)
	}

	db, err := postgres.New(ctx, pgCfg)
	if err != nil {
		log.Fatal(err)
	}
	_ = db
}
```
