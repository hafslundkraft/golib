# snowflake

![Version](https://img.shields.io/github/v/tag/hafslundkraft/golib?filter=snowflake/v*&label=version)

Connect to Snowflake from a Happi workload using the workload's own platform
identity. 

The Kubernetes token the Happi operator projects into your pod is exchanged at the
platform IdP for a token scoped to your Snowflake account, which Snowflake accepts
as a workload identity credential.

## Prerequisites

Your manifest must request Snowflake access:

```yaml
snowflake: true
```

The operator then creates your Snowflake service user, binds it to your service
account, and injects the environment variables below.

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
process: the pool exchanges a fresh token for every connection it opens, and
retires connections well inside that token's lifetime.

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

## Environment variables

| Config field        | Environment variable   | Required / When                 | Description                                   |
|---------------------|------------------------|---------------------------------|-----------------------------------------------|
| `Config.Account`    | `SNOWFLAKE_ACCOUNT`    | Required (injected by operator) | Snowflake account identifier                  |
| `Config.Warehouse`  | `SNOWFLAKE_WAREHOUSE`  | Optional (injected by operator) | Warehouse the session runs against            |

The token exchange itself is handled by
[identity](../identity), which reads `HAPPI_IDP_ISSUER_URL` and the projected
Kubernetes token. `NewConfig` takes `os.Getenv` as a parameter rather than calling
it directly, so the configuration can be tested without touching the environment.

## Troubleshooting

**`missing SNOWFLAKE_ACCOUNT environment variable`**
You are running outside a Happi pod, or the manifest does not request Snowflake.
Set the variables yourself for local development.

**`HAPPI_IDP_ISSUER_URL not set` from `NewWorkloadCredential`**
Same cause — the operator injects this alongside the Snowflake variables.

**`exchange token for snowflake: ... 401`**
The IdP did not recognise the workload. Usually means the service account differs
from the one the Snowflake service user is bound to.

**`ping snowflake: ...` with an authentication error**
The token reached Snowflake but was rejected. Check that the service user's
`OIDC_AUDIENCE_LIST` contains `<account>.snowflakecomputing.com`.
