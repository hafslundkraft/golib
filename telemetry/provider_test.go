package telemetry

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
)

func TestProvider(t *testing.T) {
	ctx := t.Context()
	tel, shutdown := New(ctx, "test",
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
		time.Local = time.UTC // in order to ensure consistent time formatting locally and on GitHub actions
		ctx := t.Context()
		var buf bytes.Buffer
		tel, shutdown := New(ctx, "test",
			WithLocalWriter(&buf),
			WithLocalColors(false),
			WithDeterministicTestIDGenerator(42),
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

		snapshotter := cupaloy.New(cupaloy.SnapshotSubdirectory("testdata"))
		snapshotter.SnapshotT(t, buf.String())
	})
}

func TestProvider_withLocalWriter_splitWriters(t *testing.T) {
	//nolint:thelper // synctest.Test takes a test function, not a helper
	synctest.Test(t, func(t *testing.T) {
		time.Local = time.UTC // in order to ensure consistent time formatting locally and on GitHub actions
		ctx := t.Context()
		var logBuf, meterBuf, traceBuf bytes.Buffer
		tel, shutdown := New(ctx, "test",
			WithLogWriter(&logBuf),
			WithMeterWriter(&meterBuf),
			WithTraceWriter(&traceBuf),
			WithLocalColors(false),
			WithDeterministicTestIDGenerator(42),
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

		snapshotter := cupaloy.New(cupaloy.SnapshotSubdirectory("testdata"))
		sb := strings.Builder{}
		sb.WriteString("=== LOGS ===\n")
		sb.WriteString(logBuf.String())
		sb.WriteString("\n=== METERS ===\n")
		sb.WriteString(meterBuf.String())
		sb.WriteString("\n=== TRACES ===\n")
		sb.WriteString(traceBuf.String())
		snapshotter.SnapshotT(t, sb.String())
	})
}
