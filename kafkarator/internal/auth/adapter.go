package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"golang.org/x/oauth2"
)

// oauth2TokenSourceAdapter adapts an oauth2.TokenSource to an AccessTokenProvider.
// This is INTERNAL and should not be exposed to users.
type oauth2TokenSourceAdapter struct {
	source oauth2.TokenSource
}

// NewOAuth2TokenSourceAdapter wraps an oauth2.TokenSource so it can be used
// by Kafka SASL/OAUTHBEARER logic.
func NewOAuth2TokenSourceAdapter(
	src oauth2.TokenSource,
) AccessTokenProvider {
	return &oauth2TokenSourceAdapter{
		source: src,
	}
}

// GetAccessToken implements AccessTokenProvider.
func (a *oauth2TokenSourceAdapter) GetAccessToken(
	ctx context.Context,
) (kafka.OAuthBearerToken, error) {
	tok, err := a.source.Token()
	if err != nil {
		return kafka.OAuthBearerToken{}, fmt.Errorf("get oauth2 token: %w", err)
	}

	if tok.AccessToken == "" {
		return kafka.OAuthBearerToken{}, fmt.Errorf("empty access token")
	}

	// Kafka requires a non-zero expiry
	if tok.Expiry.IsZero() {
		return kafka.OAuthBearerToken{}, fmt.Errorf("oauth2 token has no expiry")
	}

	// Defensive check
	if time.Now().After(tok.Expiry) {
		return kafka.OAuthBearerToken{}, fmt.Errorf("oauth2 token already expired")
	}

	return kafka.OAuthBearerToken{
		TokenValue: tok.AccessToken,
		Expiration: tok.Expiry,
		Principal:  "",
		Extensions: map[string]string{},
	}, nil
}
