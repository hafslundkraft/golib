package claimcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

const (
	envIDPIssuerURL     = "HAPPI_IDP_ISSUER_URL"
	envIDPTokenFile     = "HAPPI_IDP_TOKEN_FILE"        //nolint:gosec // environment variable name, not a credential
	envAWSTokenFile     = "AWS_WEB_IDENTITY_TOKEN_FILE" //nolint:gosec // environment variable name, not a credential
	defaultIDPTokenFile = "/happi/idp-token"            //nolint:gosec // well-known path on Happi pods, not a hardcoded credential
	defaultAWSTokenFile = "/tmp/ceph_token"             // #nosec G108 — well-known path on Happi pods
	defaultIDPS3Scope   = "ceph_rgw"

	envIDPS3Scope = "HAPPI_IDP_S3_SCOPE"

	tokenExpiryMargin   = 60 * time.Second
	tokenExpiryFallback = 3600 * time.Second
	tokenRequestTimeout = 10 * time.Second
)

// ClaimCheckRoleARN computes the Ceph IAM role ARN for a claim-check bucket.
//
//	arn:aws:iam:::role/happi/{system}/{env}/{bucket}/{system}.{env}.{bucket}.{access}
//
// access is "rw" for writers and "r" for readers.
func claimCheckRoleARN(system, env, bucket, access string) (string, error) {
	var missing []string
	for name, val := range map[string]string{
		"system": system, "env": env, "bucket": bucket, "access": access,
	} {
		if strings.TrimSpace(val) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf(
			"claimCheckRoleARN: required argument(s) empty or whitespace: %s",
			strings.Join(missing, ", "),
		)
	}
	return fmt.Sprintf("arn:aws:iam:::role/happi/%s/%s/%s/%s.%s.%s.%s",
		system, env, bucket, system, env, bucket, access), nil
}

// newTokenExchanger creates a tokenExchanger from environment variables.
// The exchanger is independent of bucket and role — it can be shared across
// multiple S3 clients to avoid concurrent token exchanges racing on the same
// awsTokenFile.
func newTokenExchanger() (*tokenExchanger, error) {
	idpIssuerURL := os.Getenv(envIDPIssuerURL)
	if strings.TrimSpace(idpIssuerURL) == "" {
		return nil, fmt.Errorf("claimcheck: %s env var is not set", envIDPIssuerURL)
	}

	idpTokenFile := defaultIDPTokenFile
	if v := os.Getenv(envIDPTokenFile); v != "" {
		idpTokenFile = v
	}

	awsTokenFile := defaultAWSTokenFile
	if v := os.Getenv(envAWSTokenFile); v != "" {
		awsTokenFile = v
	}

	idpScope := defaultIDPS3Scope
	if v := os.Getenv(envIDPS3Scope); v != "" {
		idpScope = v
	}

	return &tokenExchanger{
		idpIssuerURL: idpIssuerURL,
		idpTokenFile: idpTokenFile,
		awsTokenFile: awsTokenFile,
		idpScope:     idpScope,
		tracer:       nooptrace.NewTracerProvider().Tracer(""),
		httpClient:   &http.Client{Timeout: tokenRequestTimeout},
	}, nil
}

type tokenExchanger struct {
	idpIssuerURL string
	idpTokenFile string
	awsTokenFile string
	idpScope     string
	tracer       trace.Tracer
	httpClient   *http.Client

	mu        sync.Mutex
	expiresAt time.Time
}

// ensureFreshToken proactively refreshes the AWS token file if it is about
// to expire (within tokenExpiryMargin). Thread-safe.
func (e *tokenExchanger) ensureFreshToken(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if time.Now().Before(e.expiresAt.Add(-tokenExpiryMargin)) {
		return nil // still fresh
	}

	expiresIn, err := e.exchangeToken(ctx)
	if err != nil {
		return err
	}
	e.expiresAt = time.Now().Add(expiresIn)
	return nil
}

// exchangeToken performs the IDP token exchange and writes the AWS token file.
// Returns the token lifetime.
func (e *tokenExchanger) exchangeToken(ctx context.Context) (_ time.Duration, retErr error) {
	ctx, span := e.tracer.Start(ctx, "idp token exchange",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer func() {
		if retErr != nil {
			span.RecordError(retErr)
			span.SetStatus(codes.Error, retErr.Error())
		}
		span.End()
	}()

	assertionBytes, err := os.ReadFile(e.idpTokenFile)
	if err != nil {
		return 0, fmt.Errorf("claimcheck: read IDP token file %s: %w", e.idpTokenFile, err)
	}
	assertion := strings.TrimSpace(string(assertionBytes))

	tokenURL := strings.TrimSuffix(e.idpIssuerURL, "/") + "/protocol/openid-connect/token"
	formData := url.Values{
		"grant_type":            {"client_credentials"},
		"client_assertion_type": {"urn:ietf:params:oauth:client-assertion-type:jwt-bearer"},
		"client_assertion":      {assertion},
		"scope":                 {e.idpScope},
	}

	reqCtx, cancel := context.WithTimeout(ctx, tokenRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, tokenURL,
		strings.NewReader(formData.Encode()))
	if err != nil {
		return 0, fmt.Errorf("claimcheck: build token exchange request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("claimcheck: token exchange request to %s: %w", tokenURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("claimcheck: read token exchange response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("claimcheck: token exchange failed: HTTP %d %s from %s",
			resp.StatusCode, resp.Status, tokenURL)
	}

	var data struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return 0, fmt.Errorf("claimcheck: parse token exchange response: %w", err)
	}
	if data.AccessToken == "" {
		return 0, fmt.Errorf("claimcheck: token exchange response missing access_token")
	}

	// Write atomically: write to a temp file then rename.
	dir := "."
	if idx := strings.LastIndex(e.awsTokenFile, "/"); idx >= 0 {
		dir = e.awsTokenFile[:idx]
		if dir == "" {
			dir = "/"
		}
	}
	tmpFile, err := os.CreateTemp(dir, ".ceph_token.*")
	if err != nil {
		return 0, fmt.Errorf("claimcheck: create temp token file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }() // clean up temp on error

	if err := tmpFile.Chmod(0o600); err != nil {
		_ = tmpFile.Close()
		return 0, fmt.Errorf("claimcheck: chmod temp token file: %w", err)
	}
	if _, err := tmpFile.WriteString(data.AccessToken); err != nil {
		_ = tmpFile.Close()
		return 0, fmt.Errorf("claimcheck: write temp token file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return 0, fmt.Errorf("claimcheck: close temp token file: %w", err)
	}
	if err := os.Rename(tmpPath, e.awsTokenFile); err != nil {
		return 0, fmt.Errorf("claimcheck: rename token file: %w", err)
	}

	lifetime := time.Duration(data.ExpiresIn) * time.Second
	if lifetime == 0 {
		lifetime = tokenExpiryFallback
	}
	return lifetime, nil
}
