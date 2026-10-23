package kafkarator

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestBearerTransport_SetsAuthorizationHeader(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ts := oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: "happi-token",
		Expiry:      time.Now().Add(5 * time.Minute),
	})
	client := withBearerTokenSource(http.DefaultClient, ts)

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, "Bearer happi-token", got)
}

type refreshingTokenSource struct {
	tokens []string
	i      int
}

func (r *refreshingTokenSource) Token() (*oauth2.Token, error) {
	tok := r.tokens[r.i]
	if r.i < len(r.tokens)-1 {
		r.i++
	}
	return &oauth2.Token{AccessToken: tok, Expiry: time.Now().Add(time.Minute)}, nil
}

func TestBearerTransport_RefreshesPerRequest(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ts := &refreshingTokenSource{tokens: []string{"t1", "t2"}}
	client := withBearerTokenSource(http.DefaultClient, ts)

	for range 2 {
		resp, err := client.Get(srv.URL)
		require.NoError(t, err)
		resp.Body.Close()
	}

	assert.Equal(t, []string{"Bearer t1", "Bearer t2"}, seen)
}

func TestNewSchemaRegistryClient_BearerPrecedenceOverBasicAuth(t *testing.T) {
	cfg := &SchemaRegistryConfig{
		SchemaRegistryURL:      "http://localhost:8081",
		SchemaRegistryUser:     "user",
		SchemaRegistryPassword: "pass",
	}
	ts := oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: "tok",
		Expiry:      time.Now().Add(time.Minute),
	})

	client, err := newSchemaRegistryClient(cfg, newMockTelemetry(), ts)
	require.NoError(t, err)
	require.NotNil(t, client)
}
