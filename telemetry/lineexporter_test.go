package telemetry

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log"
	logsdk "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/log/logtest"
)

func newTestRecord() logsdk.Record {
	return logtest.RecordFactory{
		Timestamp:                 time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC),
		Severity:                  log.SeverityInfo,
		SeverityText:              "INFO",
		Body:                      log.StringValue("hello"),
		Attributes:                []log.KeyValue{log.String("app", "test")},
		AttributeValueLengthLimit: -1,
	}.NewRecord()
}

func TestLineLogExporter_IncludeViaMarker(t *testing.T) {
	var buf bytes.Buffer
	exp := &LineLogExporter{IncludeViaMarker: true, w: &buf}

	require.NoError(t, exp.Export(t.Context(), []logsdk.Record{newTestRecord()}))

	out := buf.String()
	require.Contains(t, out, `sev=INFO msg="hello"`)
	require.Contains(t, out, `app="test"`)
	require.Contains(t, out, `happi.via="stdout"`)
}

func TestLineLogExporter_NoMarkerByDefault(t *testing.T) {
	var buf bytes.Buffer
	exp := &LineLogExporter{w: &buf}

	require.NoError(t, exp.Export(t.Context(), []logsdk.Record{newTestRecord()}))

	require.NotContains(t, buf.String(), "happi.via")
}
