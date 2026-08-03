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
// background goroutine that periodically refreshes the token.
//
// The refresh interval is derived from the token expiration time and includes
// exponential backoff on failures.
//
// ctx bounds the initial synchronous refresh and every refresh performed by the
// loop; canceling it also stops the loop. The returned stop function stops the
// loop and blocks until its goroutine has returned, so that callers may tear
// down tr as soon as stop returns — an in-flight SetOAuthBearerToken on a
// destroyed Kafka handle is a use-after-free, not a returned error. stop is
// idempotent and safe to call concurrently.
func StartOAuthRefreshLoop(
	ctx context.Context,
	tp AccessTokenProvider,
	tr TokenReceiver,
	tracer trace.Tracer,
) (stop func(), err error) {
	// Initial token refresh
	token, err := refreshOAuthToken(ctx, tp, tr, tracer)
	if err != nil {
		return nil, fmt.Errorf("refresh oauth token: %w", err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		refreshDelay := refreshInterval(token)
		backoffDelay := 1 * time.Second

		for {
			select {
			case <-loopCtx.Done():
				return

			case <-time.After(refreshDelay):
				tkn, err := refreshOAuthToken(loopCtx, tp, tr, tracer)
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

	// Both operations are idempotent, so stop needs no additional guard:
	// canceling twice is a no-op and a receive on a closed channel never
	// blocks.
	return func() {
		cancel()
		<-done
	}, nil
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
	ctx, span := tracer.Start(ctx, "kafkarator.refresh_oauth_token")
	defer span.End()

	token, err := tp.GetAccessToken(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		_ = tr.SetOAuthBearerTokenFailure(err.Error())
		return kafka.OAuthBearerToken{}, fmt.Errorf("failed to get oauth token: %w", err)
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
