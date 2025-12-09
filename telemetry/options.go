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
// output. It also enables local mode. NB! This option sets the writer for all
// three types of telemetry (logging, tracing, meters), so if it is called
// after any of WithLogWriter, WithTraceWriter, or WithMeterWriter, this
// writer will take precedence.
func WithLocalWriter(w io.Writer) OptionFunc {
	return func(c *config) {
		c.localLoggerW = w
		c.localMeterW = w
		c.localTracerW = w
		c.local = true
	}
}

// WithLogWriter allows you to specify a custom io.Writer for local terminal
// output for logs. It also enables local mode.
func WithLogWriter(w io.Writer) OptionFunc {
	return func(c *config) {
		c.localLoggerW = w
		c.local = true
	}
}

// WithTraceWriter allows you to specify a custom io.Writer for local terminal
// output for traces. It also enables local mode.
func WithTraceWriter(w io.Writer) OptionFunc {
	return func(c *config) {
		c.localTracerW = w
		c.local = true
	}
}

// WithMeterWriter allows you to specify a custom io.Writer for local terminal
// output for meters. It also enables local mode.
func WithMeterWriter(w io.Writer) OptionFunc {
	return func(c *config) {
		c.localMeterW = w
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

// WithDeterministicTestIDGenerator enables deterministic trace and span ID generation for testing.
// This should only be used in tests.
// NB! The given seed should be a positive value other than 0.
func WithDeterministicTestIDGenerator(seed int64) OptionFunc {
	return func(c *config) {
		c.idGenSeed = seed
	}
}
