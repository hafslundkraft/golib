package claimcheck

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hafslundkraft/golib/identity"
	"golang.org/x/oauth2"
)

const (
	envIDPIssuerURL            = "HAPPI_IDP_ISSUER_URL"
	envServiceAccountTokenPath = "HAPPI_SERVICE_ACCOUNT_TOKEN_PATH" //nolint:gosec // environment variable name, not a credential
	// envIDPTokenFile is the legacy name, kept until we confirm no deployment sets it.
	// Remove together with the same fallback in hafslund.happi.py.
	envIDPTokenFile     = "HAPPI_IDP_TOKEN_FILE"        //nolint:gosec // environment variable name, not a credential
	envAWSTokenFile     = "AWS_WEB_IDENTITY_TOKEN_FILE" //nolint:gosec // environment variable name, not a credential
	defaultIDPTokenFile = "/happi/idp-token"            //nolint:gosec // well-known path on Happi pods, not a hardcoded credential
	defaultAWSTokenFile = "/tmp/ceph_token"             // #nosec G108 — well-known path on Happi pods
	defaultIDPS3Scope   = "ceph_rgw"

	envIDPS3Scope = "HAPPI_IDP_S3_SCOPE"

	tokenRequestTimeout = 10 * time.Second
)

// isValidARNComponent reports whether s contains only characters that are safe
// to embed in a Ceph IAM role ARN path component (ASCII alphanumeric, '-', '_').
func isValidARNComponent(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

// claimCheckRoleARN computes the Ceph IAM role ARN for a claim-check bucket.
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
	if !isValidARNComponent(system) {
		return "", fmt.Errorf(
			"claimCheckRoleARN: system %q contains characters not allowed in an ARN path component",
			system,
		)
	}
	if !isValidARNComponent(env) {
		return "", fmt.Errorf("claimCheckRoleARN: env %q contains characters not allowed in an ARN path component", env)
	}
	return fmt.Sprintf("arn:aws:iam:::role/happi/%s/%s/%s/%s.%s.%s.%s",
		system, env, bucket, system, env, bucket, access), nil
}

// idpTokenFileFromEnv returns the path to the platform IDP token. Blank values
// count as unset, matching token_file_from_env in hafslund.happi.py.
func idpTokenFileFromEnv() string {
	for _, name := range []string{envServiceAccountTokenPath, envIDPTokenFile} {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			return v
		}
	}
	return defaultIDPTokenFile
}

// newIDPTokenSource returns a cached Happi IDP token source for the given scope.
// identity keeps ctx for the source's lifetime, so it must be long-lived, and
// oauth2 reads its HTTP client from it — that is how httpClient's timeout
// reaches token refreshes, since Token() takes no ctx.
func newIDPTokenSource(issuerURL, tokenFile, scope string, httpClient *http.Client) oauth2.TokenSource {
	cred := &identity.WorkloadCredential{
		TokenFile: tokenFile,
		TokenURL:  strings.TrimRight(issuerURL, "/") + "/protocol/openid-connect/token",
	}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, httpClient)
	return cred.TokenSource(ctx, identity.WithScopes(scope))
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
	parsed, err := url.Parse(idpIssuerURL)
	if err != nil || parsed.Scheme != "https" {
		return nil, fmt.Errorf("claimcheck: %s must be an https:// URL, got %q", envIDPIssuerURL, idpIssuerURL)
	}

	idpTokenFile := idpTokenFileFromEnv()

	awsTokenFile := defaultAWSTokenFile
	if v := os.Getenv(envAWSTokenFile); v != "" {
		awsTokenFile = v
	}

	idpScope := defaultIDPS3Scope
	if v := os.Getenv(envIDPS3Scope); v != "" {
		idpScope = v
	}

	httpClient := &http.Client{Timeout: tokenRequestTimeout}
	return &tokenExchanger{
		awsTokenFile: awsTokenFile,
		tokenSource:  newIDPTokenSource(idpIssuerURL, idpTokenFile, idpScope, httpClient),
	}, nil
}

// tokenExchanger keeps awsTokenFile in sync with the current Happi IDP token,
// which the AWS SDK reads for AssumeRoleWithWebIdentity.
type tokenExchanger struct {
	awsTokenFile string
	tokenSource  oauth2.TokenSource

	mu      sync.Mutex
	written string // access token currently in awsTokenFile
}

// ensureFreshToken writes the current IDP token to awsTokenFile if it has
// changed. tokenSource owns caching and expiry, so this is cheap when nothing
// changed. Thread-safe.
func (e *tokenExchanger) ensureFreshToken(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tok, err := e.tokenSource.Token()
	if err != nil {
		return fmt.Errorf("claimcheck: get IDP token: %w", err)
	}
	if tok.AccessToken == e.written {
		return nil
	}
	if err := writeFileAtomic(e.awsTokenFile, tok.AccessToken); err != nil {
		return err
	}
	e.written = tok.AccessToken
	return nil
}

// writeFileAtomic writes content to path via a temp file and rename, so the
// AWS SDK never reads a half-written token.
func writeFileAtomic(path, content string) error {
	dir := "."
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		dir = path[:idx]
		if dir == "" {
			dir = "/"
		}
	}
	tmpFile, err := os.CreateTemp(dir, ".ceph_token.*")
	if err != nil {
		return fmt.Errorf("claimcheck: create temp token file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }() // clean up temp on error

	if err := tmpFile.Chmod(0o600); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("claimcheck: chmod temp token file: %w", err)
	}
	if _, err := tmpFile.WriteString(content); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("claimcheck: write temp token file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("claimcheck: close temp token file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("claimcheck: rename token file: %w", err)
	}
	return nil
}
