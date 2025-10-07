package telemetry

import "io"

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
