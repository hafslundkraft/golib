# snowflake

![Version](https://img.shields.io/github/v/tag/hafslundkraft/golib?filter=snowflake/v*&label=version)

Connect to Snowflake from a Happi workload. There are no secrets to configure: the
connection uses the identity the Happi operator gives your pod.

## Installation

```bash
go get github.com/hafslundkraft/golib/snowflake
```

## Prerequisites

Your manifest must request Snowflake access:

```yaml
snowflake: true
```

The operator then creates your Snowflake service user, binds it to your service
account, and injects the environment variables the package needs: `SNOWFLAKE_ACCOUNT`
(required) and `SNOWFLAKE_WAREHOUSE` (optional).

You also need read access to the tables you query. Add your workload to the access
list (ACL) in the producer's data product manifest.

## Minimal example

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/hafslundkraft/golib/identity"
	"github.com/hafslundkraft/golib/snowflake"
)

func main() {
	ctx := context.Background()

	cred, err := identity.NewWorkloadCredential()
	if err != nil {
		log.Fatal(err)
	}

	cfg, err := snowflake.NewConfig(os.Getenv, cred)
	if err != nil {
		log.Fatal(err)
	}

	db, err := snowflake.Open(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM INGEST.MY_SCHEMA.MY_TABLE").Scan(&n); err != nil {
		log.Fatal(err)
	}
	log.Println(n)
}
```

`Open` returns an ordinary `*sql.DB`, so everything after it is standard
`database/sql`.

Tables are addressed as `DATABASE.SCHEMA.TABLE`, unless you set a default
database and schema (see below).

## Connection lifetime

Set up the `*sql.DB` once at startup and keep it for the lifetime of the process.
Every connection the pool opens gets a fresh token. After an hour, `database/sql`
retires a connection when it is returned to the pool rather than reusing it; an
in-flight query or transaction may keep the connection longer.

If you run many queries in parallel, raise `db.SetMaxIdleConns`: `database/sql`
keeps only two idle connections by default, and every new connection costs a
token exchange and a Snowflake login.

## Choosing a warehouse

The operator injects a default warehouse, so most workloads do not set one. An
`Application` can override it in its manifest:

```yaml
snowflake: true
snowflakeWarehouse: WH_SOMETHING_BIGGER
```

A `HappiJob` has no such manifest field, so pass it to `NewConfig` instead:

```go
cfg, err := snowflake.NewConfig(os.Getenv, cred, snowflake.WithWarehouse("WH_SOMETHING_BIGGER"))
```

## Default database and schema

Set a default database and schema to query tables by name alone:

```go
cfg, err := snowflake.NewConfig(os.Getenv, cred,
	snowflake.WithDatabase("INGEST"),
	snowflake.WithSchema("MY_SCHEMA"),
)
// ...
db.QueryRowContext(ctx, "SELECT COUNT(*) FROM MY_TABLE")
```

You can set the database alone and query `SCHEMA.TABLE`. `WithSchema` requires
`WithDatabase`; `NewConfig` returns an error otherwise.

Without them, tables must be fully qualified. Setting a default does not grant
access: your workload still needs read access to the tables, as above.
