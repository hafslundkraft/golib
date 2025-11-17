package identity

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// WorkloadCredential is a Credential for HAPPI workloads, reading a service
// account token and using it as a client assertion to fetch a HAPPI token.
type WorkloadCredential struct {
	tokenFile string
	clientID  string
	tokenURL  string
}

var _ Credential = (*WorkloadCredential)(nil)

// NewWorkloadCredential creates a new WorkloadCredential based on configuration
// set by the HAPPI platform in the environment.
func NewWorkloadCredential() (WorkloadCredential, error) {
	clientID := os.Getenv("HAPPI_CLIENT_ID")
	if clientID == "" {
		return WorkloadCredential{}, fmt.Errorf("HAPPI_CLIENT_ID not set")
	}
	clientTokenFilePath := os.Getenv("HAPPI_SERVICE_ACCOUNT_TOKEN_PATH")
	if clientTokenFilePath == "" {
		return WorkloadCredential{}, fmt.Errorf("HAPPI_SERVICE_ACCOUNT_TOKEN_PATH not set")
	}
	tokenURL := os.Getenv("HAPPI_TOKEN_URL")
	if tokenURL == "" {
		return WorkloadCredential{}, fmt.Errorf("HAPPI_TOKEN_URL not set")
	}

	return WorkloadCredential{
		clientID:  clientID,
		tokenURL:  tokenURL,
		tokenFile: clientTokenFilePath,
	}, nil
}

// TokenSource constructs an oauth2.TokenSource that will return a cached or new HAPPI token.
func (w *WorkloadCredential) TokenSource(ctx context.Context, scopes ...string) oauth2.TokenSource {
	return oauth2.ReuseTokenSource(nil, &tokenSource{
		cfg: clientcredentials.Config{
			ClientID: w.clientID,
			TokenURL: w.tokenURL,
			Scopes:   scopes,
			EndpointParams: url.Values{
				"client_assertion_type": []string{"urn:ietf:params:oauth:client-assertion-type:jwt-bearer"},
			},
			AuthStyle: oauth2.AuthStyleInParams,
		},
		ctx:       ctx,
		mtx:       &sync.RWMutex{},
		tokenFile: w.tokenFile,
	})
}

type tokenSource struct {
	cfg       clientcredentials.Config
	ctx       context.Context
	mtx       *sync.RWMutex
	expires   time.Time
	tokenFile string
}

// Token returns a new HAPPI token using client assertion
func (t *tokenSource) Token() (*oauth2.Token, error) {
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

func (w *tokenSource) refreshAssertion() error {
	// The code in this function is copied from https://github.com/Azure/azure-sdk-for-go/blob/d1a1a45f72a0a35372ebfeaaf042bdef642365df/sdk/azidentity/workload_identity.go#L132
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
			w.cfg.EndpointParams.Set("client_assertion", string(content))
			// Kubernetes rotates service account tokens when they reach 80% of their total TTL. The shortest TTL
			// is 1 hour. That implies the token we just read is valid for at least 12 minutes (20% of 1 hour),
			// but we add some margin for safety.
			w.expires = now.Add(10 * time.Minute)
		}
	} else {
		defer w.mtx.RUnlock()
	}
	return nil
}
