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

type AccessTokenProvider interface {
	GetAccessToken(ctx context.Context) (string, error)
}

type TokenProvider struct {
	cred  azcore.TokenCredential
	scope string
}

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

func (p *TokenProvider) GetAccessToken(ctx context.Context) (string, error) {
	token, err := p.cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{p.scope},
	})
	if err != nil {
		return "", fmt.Errorf("failed to get Azure AD token: %w", err)
	}
	return token.Token, nil
}

func TelemetryWrappedTokenProvider(tp AccessTokenProvider, tracer trace.Tracer) func(context.Context) (string, error) {
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
