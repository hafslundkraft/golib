package identity

import (
	"context"

	"golang.org/x/oauth2"
)

// A Credential is anything that can be used to produce an oauth2.TokenSource
type Credential interface {
	TokenSource(ctx context.Context, scopes ...string) oauth2.TokenSource
}
