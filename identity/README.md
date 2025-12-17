# identity

Library for fetching OAuth tokens based on K8s service account identity,
using the RFC 7521 client assertion flow. Assumes to be running on the
Happi Platform (where it will find the service account token on
`/happi/idp-token`), but the path can be configured by initializing the
`WorkloadCredential` struct directly.

Note: This implementation reads the service account token's expiry from
the `exp` claim, meaning tokens using the transitional 3607-second
expiry are not supported.

## Minimal example

```go
package main

import (
	"context"
	"os"

	"github.com/hafslundkraft/golib/identity"
)

func main() {
	ctx := context.Background()

	cred, err := identity.NewWorkloadCredential()
	if err != nil {
		os.Exit(1)
	}

	scope := "foo"
	ts := cred.TokenSource(ctx, scope)
	_ = ts.Token()
}
```
