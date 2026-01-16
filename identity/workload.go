package identity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// WorkloadCredential is a Credential for HAPPI workloads, reading a service
// account token and using it as a client assertion to fetch a HAPPI token.
type WorkloadCredential struct {
	TokenFile string
	TokenURL  string
}

var _ Credential = (*WorkloadCredential)(nil)

// NewWorkloadCredential creates a new WorkloadCredential based on configuration
// set by the HAPPI platform in the environment.
func NewWorkloadCredential() (WorkloadCredential, error) {
	clientTokenFilePath := os.Getenv("HAPPI_SERVICE_ACCOUNT_TOKEN_PATH")
	if clientTokenFilePath == "" {
		clientTokenFilePath = "/happi/idp-token" //nolint:gosec // This is not a secret
	}
	tokenURL := os.Getenv("HAPPI_TOKEN_URL")
	if tokenURL == "" {
		return WorkloadCredential{}, fmt.Errorf("HAPPI_TOKEN_URL not set")
	}

	return WorkloadCredential{
		TokenURL:  tokenURL,
		TokenFile: clientTokenFilePath,
	}, nil
}

// TokenSource constructs an oauth2.TokenSource that will return a cached or new HAPPI token.
func (w *WorkloadCredential) TokenSource(ctx context.Context, options ...func(*TokenOptions)) oauth2.TokenSource {
	var opts TokenOptions
	for _, opt := range options {
		opt(&opts)
	}

	cfg := clientcredentials.Config{
		Scopes:   opts.scopes,
		TokenURL: w.TokenURL,
		EndpointParams: url.Values{
			"client_assertion_type": []string{"urn:ietf:params:oauth:client-assertion-type:jwt-bearer"},
		},
		AuthStyle: oauth2.AuthStyleInParams,
	}

	if opts.resource != "" {
		cfg.EndpointParams.Set("resource", opts.resource)
	}

	return oauth2.ReuseTokenSource(nil, &TokenSource{
		cfg:       cfg,
		ctx:       ctx,
		mtx:       &sync.RWMutex{},
		tokenFile: w.TokenFile,
	})
}

// TokenSource holds the state for caching and producing tokens for a particular
// credential and a particular set of scopes.
type TokenSource struct {
	cfg       clientcredentials.Config
	ctx       context.Context
	mtx       *sync.RWMutex
	expires   time.Time
	tokenFile string
}

var _ oauth2.TokenSource = (*TokenSource)(nil)

// Token returns a new HAPPI token using client assertion
func (t *TokenSource) Token() (*oauth2.Token, error) {
	err := t.refreshAssertion()
	if err != nil {
		return nil, fmt.Errorf("getting assertion: %w", err)
	}
	token, err := t.cfg.Token(t.ctx)
	if err != nil {
		return nil, fmt.Errorf("getting token: %w", err)
	}
	return token, nil
}

func (w *TokenSource) refreshAssertion() error {
	// The code in this function is adapted from https://github.com/Azure/azure-sdk-for-go/blob/d1a1a45f72a0a35372ebfeaaf042bdef642365df/sdk/azidentity/workload_identity.go#L132
	// which is licensed under the MIT license included here:
	/*
		MIT License

		Copyright (c) Microsoft Corporation.

		Permission is hereby granted, free of charge, to any person obtaining a copy
		of this software and associated documentation files (the "Software"), to deal
		in the Software without restriction, including without limitation the rights
		to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
		copies of the Software, and to permit persons to whom the Software is
		furnished to do so, subject to the following conditions:

		The above copyright notice and this permission notice shall be included in all
		copies or substantial portions of the Software.

		THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
		IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
		FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
		AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
		LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
		OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
		SOFTWARE
	*/
	w.mtx.RLock()
	if w.expires.Before(time.Now()) {
		// ensure only one goroutine at a time updates the assertion
		w.mtx.RUnlock()
		w.mtx.Lock()
		defer w.mtx.Unlock()
		// double check because another goroutine may have acquired the write lock first and done the update
		if now := time.Now(); w.expires.Before(now) {
			content, err := os.ReadFile(w.tokenFile)
			if err != nil {
				return fmt.Errorf("reading service account token file: %w", err)
			}
			assertion := string(content)
			w.cfg.EndpointParams.Set("client_assertion", assertion)
			exp, err := readExpiry(assertion)
			if err != nil {
				return fmt.Errorf("parsing service account token: %w", err)
			}
			// Some margin for safety
			w.expires = exp.Add(-30 * time.Second)
		}
	} else {
		defer w.mtx.RUnlock()
	}
	return nil
}

// readExpiry reads the exp claim from a raw JWT input and returns the parsed
// time.Time without any further validation.
func readExpiry(rawToken string) (time.Time, error) {
	parts := strings.SplitN(rawToken, ".", 3)
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("expected 3 parts, got %d", len(parts))
	}

	token, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("base64 decoding token: %w", err)
	}

	var c struct {
		Expiry int64 `json:"exp"`
	}
	if err := json.Unmarshal(token, &c); err != nil {
		return time.Time{}, fmt.Errorf("parsing expiry: %w", err)
	}

	return time.Unix(c.Expiry, 0).UTC(), nil
}
