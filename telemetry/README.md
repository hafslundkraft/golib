# telemetry

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
	
	tel, shutdown := telemetry.New(ctx, "my-app-name", telemetry.WithLocal(os.Getenv("ENV") == "local"))
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

## Options
The exact behavior of the provider can be configured using options passed to the New function:

* **WithLocal**: If set to true, the provider will print traces and logs to STDOUT. This is useful for local development and testing.
* **WithLocalWriter**: Allows you to specify a custom io.Writer for local terminal output. It also enables local mode.
* **WithLocalColors**: If set to true, the local output will include ANSI color codes for better readability in terminals that support colors. This option is only effective when local mode is enabled.
* **WithSimpleLogProcessor**: Configures the logger to use a simple log processor instead of the default batch processor. This can be useful for low-throughput applications or short-lived jobs.
* **WithAttributes**: Adds extra attributes/fields to the underlying telemetry providers. Currently only works for logs.
* **WithTestIDGenerator**: Enables deterministic trace and span ID generation for testing. This should only be used in tests.

