package telemetry

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/processors/minsev"
	"go.opentelemetry.io/otel/codes"
)

func TestProvider(t *testing.T) {
	ctx := t.Context()
	tel, shutdown := New(ctx,
		WithLocal(true),
		WithAttributes(map[string]string{
			"app": "test",
			"env": os.Getenv("ENV"),
		}),
	)
	require.NotNil(t, shutdown)
	require.NotNil(t, tel)

	log := tel.Logger()

	spanCtx, span := tel.tracer.Start(ctx, "test")
	require.NotNil(t, span)
	require.NotNil(t, spanCtx)
	span.SetStatus(codes.Error, "err")

	log.DebugContext(spanCtx, "testing testing", "some-field", "some-value")
	log.ErrorContext(spanCtx, "testing testing")
	log.WarnContext(spanCtx, "testing testing")

	span.End()

	counter, err := tel.Meter().Int64Counter("test-counter")
	require.NoError(t, err)
	counter.Add(ctx, 42)

	require.NoError(t, shutdown(ctx))
}

func TestProvider_withLocalWriter(t *testing.T) {
	//nolint:thelper // synctest.Test takes a test function, not a helper
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		var buf bytes.Buffer
		tel, shutdown := New(ctx,
			WithLocalWriter(&buf),
			WithLocalColors(false),
			WithTestIDGenerator(),
			WithAttributes(map[string]string{
				"app": "test",
			}),
		)
		require.NotNil(t, shutdown)
		require.NotNil(t, tel)

		spanCtx, span := tel.tracer.Start(ctx, "test")
		require.NotNil(t, span)
		require.NotNil(t, spanCtx)
		span.SetStatus(codes.Error, "err")

		tel.Logger().DebugContext(ctx, "testing testing", "some-field", "some-value")

		counter, err := tel.Meter().Int64Counter("test-counter")
		require.NoError(t, err)
		counter.Add(ctx, 42)

		span.End()

		require.NoError(t, shutdown(ctx))

		expectedContents := []string{
			// Log line
			`sev=DEBUG msg="testing testing" app="test" some-field="some-value"`,

			// Trace
			`[trace] id=8bef2b2824b41029c148c2769d143fcf name="test"`,
			"start=2000-01-01T",
			"status=Error",

			// Metric
			"[metric] 2000-01-01T",
			"name=test-counter value=42",
		}

		loggedContent := buf.Bytes()
		for _, e := range expectedContents {
			if !bytes.Contains(loggedContent, []byte(e)) {
				t.Errorf("Expected log to contain %q, but it did not. Full log:\n%s", e, loggedContent)
			}
		}
		require.NotContains(t, string(loggedContent), "happi.via",
			"local mode should not emit the stdout dedup marker")
	})
}

func TestProvider_withEndpoint(t *testing.T) {
	ctx := t.Context()

	gotPaths := sync.Map{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPaths.Store(r.URL.Path, true)
		// An empty 200 body is a valid (empty) OTLP export response.
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// WithEndpoint overrides the OTLP endpoint for all signals, so the OTLP
	// exporters should send to the test server rather than the default one
	// derived from OTEL_EXPORTER_OTLP_ENDPOINT.
	tel, shutdown := New(ctx, WithEndpoint(srv.URL))
	require.NotNil(t, tel)

	_, span := tel.Tracer().Start(ctx, "test")
	span.End()

	counter, err := tel.Meter().Int64Counter("test-counter")
	require.NoError(t, err)
	counter.Add(ctx, 42)

	// Shutdown flushes the batched span processor and the periodic metric
	// reader, forcing the exports to the overridden endpoint.
	require.NoError(t, shutdown(ctx))

	tp, found := gotPaths.Load("/v1/traces")
	require.True(t, found, "expected /v1/traces to be a key in the map")
	require.True(t, tp.(bool), "expected trace export to hit the overridden endpoint")
	mp, found := gotPaths.Load("/v1/metrics")
	require.True(t, found, "expected /v1/metrics to be a key in the map")
	require.True(t, mp.(bool), "expected metric export to hit the overridden endpoint")
}

func TestProvider_withMinSeverity(t *testing.T) {
	ctx := t.Context()
	var buf bytes.Buffer
	tel, shutdown := New(ctx,
		WithLocalWriter(&buf),
		WithLocalColors(false),
		WithMinSeverity(minsev.SeverityWarn),
	)

	log := tel.Logger()
	log.DebugContext(ctx, "debug-line")
	log.InfoContext(ctx, "info-line")
	log.WarnContext(ctx, "warn-line")
	log.ErrorContext(ctx, "error-line")

	require.NoError(t, shutdown(ctx))

	out := buf.String()
	require.NotContains(t, out, "debug-line")
	require.NotContains(t, out, "info-line")
	require.Contains(t, out, "warn-line")
	require.Contains(t, out, "error-line")
}
