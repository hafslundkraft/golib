package auth

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"time"
)

type TokenReceiver interface {
	SetOAuthBearerToken(token kafka.OAuthBearerToken) error
	SetOAuthBearerTokenFailure(errStr string) error
}

func StartOAuthRefreshLoop(
	ctx context.Context,
	tp AccessTokenProvider,
	tr TokenReceiver,
	tracer trace.Tracer,
) error {
	// Initial token
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
				// Refresh
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

func refreshInterval(t kafka.OAuthBearerToken) time.Duration {
	d := time.Until(t.Expiration) - 2*time.Minute
	if d < time.Minute {
		d = time.Minute
	}
	return d
}

func backoff(current time.Duration) time.Duration {
	if current < 30*time.Second {
		return current * 2
	}
	return current
}
