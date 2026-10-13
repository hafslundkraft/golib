# snowflake

![Version](https://img.shields.io/github/v/tag/hafslundkraft/golib?filter=snowflake/v*&label=version)

Connect to Snowflake from a Happi workload. There are no secrets to configure: the
connection uses the identity the Happi operator gives your pod.

## Prerequisites

Your manifest must request Snowflake access:

```yaml
snowflake: true
```

The operator then creates your Snowflake service user, binds it to your service
account, and injects the environment variables the package needs.

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

	db, err := snowflake.New(ctx, cfg)
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

`New` returns an ordinary `*sql.DB`, so everything after it is standard
`database/sql`. Set it up once at startup and keep it for the lifetime of the
process: every connection the pool opens gets a current token, and connections
are retired regularly so the pool keeps reconnecting with current tokens.

Tables are addressed as `DATABASE.SCHEMA.TABLE`, so a session database and schema
are rarely needed.

## Choosing a warehouse

The operator injects a warehouse, so most workloads do not set one. An
`Application` can override it in its manifest:

```yaml
snowflake: true
snowflakeWarehouse: WH_SOMETHING_BIGGER
```

A `HappiJob` has no such manifest field, so pass it to `NewConfig` instead:

```go
cfg, err := snowflake.NewConfig(os.Getenv, cred, snowflake.WithWarehouse("WH_SOMETHING_BIGGER"))
```
