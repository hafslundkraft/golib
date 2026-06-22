package telemetry

import (
	"io"
	"net/http"

	"go.opentelemetry.io/contrib/processors/minsev"
)

// OptionFunc is a function that configures the telemetry provider.
type OptionFunc func(*config)

// WithLocal switches the OpenTelemetry collector backends off and enables a
// simple stdout backend for logs, metrics and traces.
func WithLocal(isLocal bool) OptionFunc {
	return func(c *config) {
		c.local = isLocal
	}
}

// WithLocalWriter allows you to specify a custom io.Writer for local terminal
// output. It also enables local mode.
func WithLocalWriter(w io.Writer) OptionFunc {
	return func(c *config) {
		c.localW = w
		c.local = true
	}
}

// WithLocalColors allows you to disable or enable (the default) ANSI colors on
// local terminal output.
func WithLocalColors(localColors bool) OptionFunc {
	return func(c *config) {
		c.localColors = localColors
	}
}

// WithAttributes adds extra attributes/fields to the underlying telemetry
// providers. Currently only works for logs.
func WithAttributes(attrs map[string]string) OptionFunc {
	return func(c *config) {
		c.attributes = attrs
	}
}

// WithTestIDGenerator enables deterministic trace and span ID generation for testing.
// This should only be used in tests.
func WithTestIDGenerator() OptionFunc {
	return func(c *config) {
		c.testIDGen = true
	}
}

// WithEndpoint overrides the OTLP endpoint that telemetry data is exported to.
// By default the endpoint is read from the OTEL_EXPORTER_OTLP_ENDPOINT
// environment variable (and the per-signal variants). The value must be a full
// URL, e.g. "https://otel.example.com:4318". This has no effect in local mode.
func WithEndpoint(endpoint string) OptionFunc {
	return func(c *config) {
		c.endpoint = endpoint
	}
}

// WithHTTPClient sets the HTTP client used by the OTLP exporters to send
// telemetry data. This is useful for customizing transport behavior, e.g.
// injecting authentication, proxies or custom TLS configuration. This has no
// effect in local mode.
func WithHTTPClient(client *http.Client) OptionFunc {
	return func(c *config) {
		c.httpClient = client
	}
}

// WithMinSeverity drops log records below the given severity. Pass a
// [minsev.Severity] for a static threshold, or a *[minsev.SeverityVar] to
// adjust it at runtime. If unset, no filtering is applied.
func WithMinSeverity(s minsev.Severitier) OptionFunc {
	return func(c *config) {
		c.minSeverity = s
	}
}
