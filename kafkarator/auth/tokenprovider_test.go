package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type mockCredential struct {
	value string
	err   error
	calls int
}

func (m *mockCredential) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	m.calls++
	if m.err != nil {
		return azcore.AccessToken{}, m.err
	}
	return azcore.AccessToken{
		Token:     m.value,
		ExpiresOn: time.Now().Add(time.Hour),
	}, nil
}

func TestTokenProvider_GetAccessToken_Success(t *testing.T) {
	mockCred := &mockCredential{value: "abc123"}

	p := &TokenProvider{
		cred:  mockCred,
		scope: "https://example/.default",
	}

	token, err := p.GetAccessToken(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if token.TokenValue != "abc123" {
		t.Fatalf("expected token abc123, got %s", token)
	}

	if mockCred.calls != 1 {
		t.Fatalf("expected 1 GetToken call, got %d", mockCred.calls)
	}
}

func TestTokenProvider_GetAccessToken_Error(t *testing.T) {
	mockErr := errors.New("boom")
	mockCred := &mockCredential{err: mockErr}

	p := &TokenProvider{
		cred:  mockCred,
		scope: "https://example/.default",
	}

	_, err := p.GetAccessToken(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if mockCred.calls != 1 {
		t.Fatalf("expected 1 GetToken call, got %d", mockCred.calls)
	}
}
