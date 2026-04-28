package telemetry

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestLoggerFromContext(t *testing.T) {
	// nolint:thelper // synctest.Test takes a test function, not a helper
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		var buf bytes.Buffer
		tel, shutdown := New(ctx, "test-logger", WithLocalWriter(&buf),
			WithAttributes(map[string]string{
				"attribute": "value",
			}))

		// Insert logger into context:
		ctx = NewContextWithLogger(ctx, tel.Logger().With("some-field", "some-value"))

		logger := LoggerFromContext(ctx)

		logger.InfoContext(ctx, "testing testing")

		require.NoError(t, shutdown(ctx))

		expectedContents := []string{
			`sev=INFO msg="testing testing" attribute="value" some-field="some-value"`,
		}

		loggedContent := buf.Bytes()
		for _, e := range expectedContents {
			if !bytes.Contains(loggedContent, []byte(e)) {
				t.Errorf("Expected log to contain %q, but it did not. Full log:\n%s", e, loggedContent)
			}
		}
	})
}
