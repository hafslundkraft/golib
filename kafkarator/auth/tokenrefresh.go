package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TokenReceiver receives OAuth bearer tokens and forwards them to the Kafka client.
// It is typically implemented by a Kafka producer or consumer.
type TokenReceiver interface {
	// SetOAuthBearerToken provides a valid OAuth bearer token to Kafka.
	SetOAuthBearerToken(token kafka.OAuthBearerToken) error

	// SetOAuthBearerTokenFailure reports a token refresh failure to Kafka.
	SetOAuthBearerTokenFailure(errStr string) error
}

// StartOAuthRefreshLoop performs an initial OAuth token refresh and starts a
// background goroutine that periodically refreshes the token until the context
// is canceled.
//
// The refresh interval is derived from the token expiration time and includes
// exponential backoff on failures.
func StartOAuthRefreshLoop(
	ctx context.Context,
	tp AccessTokenProvider,
	tr TokenReceiver,
	tracer trace.Tracer,
) error {
	// Initial token refresh
	token, err := refreshOAuthToken(ctx, tp, tr, tracer)
	if err != nil {
		return err
	}

	refreshDelay := refreshInterval(token)
	backoffDelay := 1 * time.Second

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case <-time.After(refreshDelay):
				tkn, err := refreshOAuthToken(ctx, tp, tr, tracer)
				if err != nil {
					backoffDelay = backoff(backoffDelay)
					refreshDelay = backoffDelay
				} else {
					backoffDelay = 1 * time.Second
					refreshDelay = refreshInterval(tkn)
				}
			}
		}
	}()

	return nil
}

// refreshOAuthToken fetches a new OAuth access token and delivers it to Kafka
// using the provided TokenReceiver.
//
// On failure, the error is reported to Kafka and returned to the caller.
func refreshOAuthToken(
	ctx context.Context,
	tp AccessTokenProvider,
	tr TokenReceiver,
	tracer trace.Tracer,
) (kafka.OAuthBearerToken, error) {
	ctx, span := tracer.Start(ctx, "kafka.refresh_oauth_token")
	defer span.End()

	tokenStr, err := tp.GetAccessToken(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		_ = tr.SetOAuthBearerTokenFailure(err.Error())
		return kafka.OAuthBearerToken{}, fmt.Errorf("failed to get oauth token: %w", err)
	}

	token := kafka.OAuthBearerToken{
		TokenValue: tokenStr,
		Expiration: time.Now().Add(1 * time.Hour),
	}

	if err := tr.SetOAuthBearerToken(token); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		_ = tr.SetOAuthBearerTokenFailure(err.Error())
		return kafka.OAuthBearerToken{}, fmt.Errorf("failed to set oauth token: %w", err)
	}

	span.SetStatus(codes.Ok, "OAuth token refreshed successfully")
	return token, nil
}

// refreshInterval calculates the next refresh interval based on the token
// expiration time, ensuring a minimum refresh interval of one minute.
func refreshInterval(t kafka.OAuthBearerToken) time.Duration {
	d := time.Until(t.Expiration) - 2*time.Minute
	if d < time.Minute {
		d = time.Minute
	}
	return d
}

// backoff applies exponential backoff with an upper bound of 30 seconds.
func backoff(current time.Duration) time.Duration {
	if current < 30*time.Second {
		return current * 2
	}
	return current
}
