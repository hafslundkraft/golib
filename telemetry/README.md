# telemetry

![Version](https://img.shields.io/github/v/tag/hafslundkraft/golib?filter=telemetry/v*&label=version)

Helper library for setting up OpenTelemetry.

## Minimal Example

```go
package main

import (
	"os"
	"context"

	"github.com/hafslundkraft/golib/telemetry"
)

func main() {
	ctx := context.Background()
	
	tel, shutdown := telemetry.New(ctx, telemetry.WithLocal(os.Getenv("ENV") == "local"))
	defer shutdown(ctx)

	log := tel.Logger()

	ctx, span := tel.Tracer().Start(ctx, "saying-hello")

	log.InfoContext(ctx, "Hello, HAPPI!")

	span.End()
}

```

## Provider
This package exposes the New function to create a telemetry provider. It sets up tracing and logging using OpenTelemetry. 
The provider object is used for accessing the tracer and logger:

* **Logger**: Access the logger using `tel.Logger()`. This logger is configured to include trace and span IDs in its output, which helps correlate logs with traces.
* **Tracer**: Access the tracer using `tel.Tracer()`. This tracer is used to create spans for tracing operations within your application.
* **Meter**: Access the meter using `tel.Meter()`. This meter is used for recording metrics.
* **HTTPMiddleware**: Access using `tel.HTTPMiddleware()`. Constructs a plain http middleware that instruments incoming requests with traces and metrics.
* **HTTPTransport**: Access using `tel.HTTPTransport()`. Constructs an instrumented RoundTripper. Add this to an HTTP client to instrument outgoing HTTP calls.

## Stdout logs and dedup marker

In the default (non-local) mode, log records are emitted both via OTLP to the
collector **and** to stdout in a key=value format, so that `kubectl logs` and
similar tools see them. Each stdout line carries a `happi.via="stdout"`
attribute that the otel collector's stdout-scraping pipeline can filter on to
drop duplicates of records already delivered via OTLP. The OTLP records
themselves are unchanged. Metrics and traces still flow only via OTLP unless
`WithLocal` is set.

## Options
The exact behavior of the provider can be configured using options passed to the New function:

* **WithLocal**: If set to true, the provider will print traces, metrics and logs to STDOUT *instead of* the OTLP collector. Useful for local development and testing. In this mode the stdout dedup marker is not emitted.
* **WithLocalWriter**: Allows you to specify a custom io.Writer for local terminal output. It also enables local mode.
* **WithLocalColors**: If set to true, the local output will include ANSI color codes for better readability in terminals that support colors. This option is only effective when local mode is enabled.
* **WithAttributes**: Adds extra attributes/fields to the underlying telemetry providers. Currently only works for logs.
* **WithMinSeverity**: Drops log records below the given severity. Pass a `minsev.Severity` for a static threshold, or a `*minsev.SeverityVar` to adjust at runtime.
* **WithTestIDGenerator**: Enables deterministic trace and span ID generation for testing. This should only be used in tests.

