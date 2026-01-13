package identity

import (
	"context"

	"golang.org/x/oauth2"
)

// A Credential is anything that can be used to produce an oauth2.TokenSource
type Credential interface {
	TokenSource(ctx context.Context, opts ...func(*TokenOptions)) oauth2.TokenSource
}

// TokenOptions defines options for configuring the token requests.
type TokenOptions struct {
	scopes   []string
	resource string
}

// WithResource sets the target resource for the token.
func WithResource(resource string) func(*TokenOptions) {
	return func(o *TokenOptions) {
		o.resource = resource
	}
}

// WithScopes specifies scopes to request in the token.
func WithScopes(scopes ...string) func(*TokenOptions) {
	return func(o *TokenOptions) {
		o.scopes = scopes
	}
}
