package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type fakeAccessTokenProvider struct {
	value string
	err   error
	calls int
}

func (f *fakeAccessTokenProvider) GetAccessToken(ctx context.Context) (kafka.OAuthBearerToken, error) {
	f.calls++
	if f.err != nil {
		return kafka.OAuthBearerToken{}, f.err
	}
	return kafka.OAuthBearerToken{
		TokenValue: f.value,
		Expiration: time.Now().Add(10 * time.Minute),
	}, nil
}

type fakeTokenReceiver struct {
	setCalls      int
	failureCalls  int
	lastToken     kafka.OAuthBearerToken
	lastFailure   string
	forceSetError error
}

func (r *fakeTokenReceiver) SetOAuthBearerToken(token kafka.OAuthBearerToken) error {
	r.setCalls++
	r.lastToken = token
	if r.forceSetError != nil {
		return r.forceSetError
	}
	return nil
}

func (r *fakeTokenReceiver) SetOAuthBearerTokenFailure(errStr string) error {
	r.failureCalls++
	r.lastFailure = errStr
	return nil
}

func TestRefreshOAuthToken_Success(t *testing.T) {
	ctx := context.Background()
	tp := &fakeAccessTokenProvider{value: "tok123"}
	tr := &fakeTokenReceiver{}
	tracer := sdktrace.NewTracerProvider().Tracer("test")

	token, err := refreshOAuthToken(ctx, tp, tr, tracer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if token.TokenValue != "tok123" {
		t.Fatalf("expected token value tok123, got %s", token.TokenValue)
	}

	if tp.calls != 1 {
		t.Fatalf("expected GetAccessToken to be called once, got %d", tp.calls)
	}
	if tr.setCalls != 1 {
		t.Fatalf("expected SetOAuthBearerToken to be called once, got %d", tr.setCalls)
	}
	if tr.failureCalls != 0 {
		t.Fatalf("expected no failures, got %d", tr.failureCalls)
	}
}

func TestRefreshOAuthToken_TokenProviderError(t *testing.T) {
	ctx := context.Background()
	tp := &fakeAccessTokenProvider{err: errors.New("token fail")}
	tr := &fakeTokenReceiver{}
	tracer := sdktrace.NewTracerProvider().Tracer("test")

	_, err := refreshOAuthToken(ctx, tp, tr, tracer)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if tp.calls != 1 {
		t.Fatalf("expected GetAccessToken calls=1, got %d", tp.calls)
	}
	if tr.setCalls != 0 {
		t.Fatalf("expected SetOAuthBearerToken calls=0, got %d", tr.setCalls)
	}
	if tr.failureCalls != 1 {
		t.Fatalf("expected SetOAuthBearerTokenFailure calls=1, got %d", tr.failureCalls)
	}
	if tr.lastFailure == "" {
		t.Fatal("expected lastFailure to be set")
	}
}

func TestRefreshOAuthToken_TokenReceiverError(t *testing.T) {
	ctx := context.Background()
	tp := &fakeAccessTokenProvider{value: "tok123"}
	tr := &fakeTokenReceiver{forceSetError: errors.New("set fail")}
	tracer := sdktrace.NewTracerProvider().Tracer("test")

	_, err := refreshOAuthToken(ctx, tp, tr, tracer)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if tp.calls != 1 {
		t.Fatalf("expected GetAccessToken calls=1, got %d", tp.calls)
	}
	if tr.setCalls != 1 {
		t.Fatalf("expected SetOAuthBearerToken calls=1, got %d", tr.setCalls)
	}
	if tr.failureCalls != 1 {
		t.Fatalf("expected SetOAuthBearerTokenFailure calls=1, got %d", tr.failureCalls)
	}
	if tr.lastFailure == "" {
		t.Fatal("expected lastFailure to be set")
	}
}

func TestRefreshInterval_NormalFuture(t *testing.T) {
	// Expiration 10 minutes from now => 8 minutes refresh interval (approx)
	exp := time.Now().Add(10 * time.Minute)
	token := kafka.OAuthBearerToken{Expiration: exp}

	d := refreshInterval(token)

	// Allow for small timing jitter
	if d < 7*time.Minute || d > 9*time.Minute {
		t.Fatalf("expected ~8m, got %v", d)
	}
}

func TestRefreshInterval_ClampedToOneMinute(t *testing.T) {
	// Expiration 30 seconds from now -> time.Until ~30s, minus 2m is negative
	exp := time.Now().Add(30 * time.Second)
	token := kafka.OAuthBearerToken{Expiration: exp}

	d := refreshInterval(token)

	if d != time.Minute {
		t.Fatalf("expected 1m due to clamp, got %v", d)
	}
}

func TestBackoff_DoublesUntil30s(t *testing.T) {
	d := 1 * time.Second
	d = backoff(d)
	if d != 2*time.Second {
		t.Fatalf("expected 2s, got %v", d)
	}
	d = backoff(d)
	if d != 4*time.Second {
		t.Fatalf("expected 4s, got %v", d)
	}
	d = 20 * time.Second
	d = backoff(d)
	if d != 40*time.Second {
		t.Fatalf("expected 40s, got %v", d)
	}
}

func TestBackoff_MaxedAt30s(t *testing.T) {
	d := 30 * time.Second
	d2 := backoff(d)
	if d2 != d {
		t.Fatalf("expected %v to stay %v, got %v", d, d, d2)
	}

	d = 45 * time.Second
	d2 = backoff(d)
	if d2 != d {
		t.Fatalf("expected %v to stay %v, got %v", d, d, d2)
	}
}

func TestStartOAuthRefreshLoop_InitialError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := &fakeAccessTokenProvider{err: errors.New("initial fail")}
	tr := &fakeTokenReceiver{}
	tracer := sdktrace.NewTracerProvider().Tracer("test")

	err := StartOAuthRefreshLoop(ctx, tp, tr, tracer)
	if err == nil {
		t.Fatal("expected error from initial refresh, got nil")
	}

	if tp.calls != 1 {
		t.Fatalf("expected 1 GetAccessToken call, got %d", tp.calls)
	}
	if tr.failureCalls != 1 {
		t.Fatalf("expected 1 SetOAuthBearerTokenFailure call, got %d", tr.failureCalls)
	}
}

func TestStartOAuthRefreshLoop_InitialSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := &fakeAccessTokenProvider{value: "tok123"}
	tr := &fakeTokenReceiver{}
	tracer := sdktrace.NewTracerProvider().Tracer("test")

	err := StartOAuthRefreshLoop(ctx, tp, tr, tracer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// StartOAuthRefreshLoop should call refreshOAuthToken once synchronously
	if tp.calls != 1 {
		t.Fatalf("expected 1 GetAccessToken call, got %d", tp.calls)
	}
	if tr.setCalls != 1 {
		t.Fatalf("expected 1 SetOAuthBearerToken call, got %d", tr.setCalls)
	}
}
