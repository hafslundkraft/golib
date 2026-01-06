package auth

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang-jwt/jwt/v5"
)

// AccessTokenProvider provides OAuth access tokens for Kafka SASL/OAUTHBEARER authentication.
type AccessTokenProvider interface {
	// GetAccessToken returns a valid OAuth access token or an error.
	GetAccessToken(ctx context.Context) (kafka.OAuthBearerToken, error)
}

// TokenProvider implements AccessTokenProvider using Azure credentials.
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
func (p *TokenProvider) GetAccessToken(ctx context.Context) (kafka.OAuthBearerToken, error) {
	token, err := p.cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{p.scope},
	})
	if err != nil {
		return kafka.OAuthBearerToken{}, fmt.Errorf("%w", err)
	}

	parsedToken, _, err := new(jwt.Parser).ParseUnverified(token.Token, jwt.MapClaims{})
	if err != nil {
		return kafka.OAuthBearerToken{}, fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := parsedToken.Claims.(jwt.MapClaims)
	if !ok {
		return kafka.OAuthBearerToken{}, fmt.Errorf("failed to cast claims")
	}

	oid, ok := claims["oid"].(string)
	if !ok || oid == "" {
		return kafka.OAuthBearerToken{}, fmt.Errorf("oid not found in token claims")
	}

	return kafka.OAuthBearerToken{
		TokenValue: token.Token,
		Expiration: token.ExpiresOn,
		Principal:  oid,
		Extensions: map[string]string{},
	}, nil
}
