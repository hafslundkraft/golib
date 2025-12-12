package auth

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// AccessTokenProvider provides OAuth access tokens for Kafka SASL/OAUTHBEARER authentication.
type AccessTokenProvider interface {
	// GetAccessToken returns a valid OAuth access token or an error.
	GetAccessToken(ctx context.Context) (string, error)
}

// TokenProvider implements AccessTokenProvider using Azure AD credentials.
type TokenProvider struct {
	cred  azcore.TokenCredential
	scope string
}

// NewDefaultTokenProvider creates a TokenProvider using Azure DefaultAzureCredential
// and the provided OAuth scope.
func NewDefaultTokenProvider(scope string) (*TokenProvider, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DefaultAzureCredential: %w", err)
	}

	return &TokenProvider{
		cred:  cred,
		scope: scope,
	}, nil
}

// GetAccessToken retrieves an OAuth access token from Azure AD using the configured scope.
func (p *TokenProvider) GetAccessToken(ctx context.Context) (string, error) {
	token, err := p.cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{p.scope},
	})
	if err != nil {
		return "", fmt.Errorf("failed to get Azure AD token: %w", err)
	}

	return token.Token, nil
}

// TelemetryWrappedTokenProvider wraps an AccessTokenProvider with OpenTelemetry tracing
// and returns a function compatible with Kafka OAuth token refresh callbacks.
func TelemetryWrappedTokenProvider(
	tp AccessTokenProvider,
	tracer trace.Tracer,
) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		ctx, span := tracer.Start(ctx, "kafka.refresh_oauth_token")
		defer span.End()

		token, err := tp.GetAccessToken(ctx)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return "", fmt.Errorf("failed to get oauth token: %w", err)
		}

		span.SetStatus(codes.Ok, "OAuth token refreshed successfully")
		return token, nil
	}
}
