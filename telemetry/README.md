# telemetry

Helper library for setting up OpenTelemetry.

```go
package main

import (
	"os"

	"github.com/HafslundEcoVannkraft/golib/telemetry"
)

func main() {
	tel, shutdown := telemetry.New(ctx, "my-app-name", telemetry.WithLocal(os.Getenv("ENV") == "local"))
	defer shutdown()

	log := tel.Logger

	ctx, span := tel.Tracer.Start(ctx, "saying-hello")

	log.InfoContext(ctx, "Hello, HAPPI!")

	span.End()
}

```

See [the Snappi demo app](github.com/HafslundEcoVannkraft/snappi-demo-api) for more complete example usage.
