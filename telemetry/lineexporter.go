// nolint:forbidigo // fmt.Print* will be replaced with fmt.Fprint* soon
package telemetry

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel/log"
	logsdk "go.opentelemetry.io/otel/sdk/log"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

const (
	colReset  = "\x1b[0m"
	colTrace  = "\x1b[36m" // cyan
	colMetric = "\x1b[35m" // magenta
	colError  = "\x1b[31m" // red
	colWarn   = "\x1b[33m" // yellow
)

func tint(color, s string) string {
	return color + s + colReset
}

// LineTraceExporter is an OpenTelemetry trace exporter that prints traces to
// STDOUT. Useful for local testing.
type LineTraceExporter struct {
	Colors bool
	w      io.Writer
}

// ExportSpans takes a slice of spans and prints them to STDOUT
func (e *LineTraceExporter) ExportSpans(ctx context.Context, spans []tracesdk.ReadOnlySpan) error {
	for _, s := range spans {
		status := s.Status().Code.String()
		tag := "[trace]"
		if e.Colors {
			if status == "Error" {
				tag = tint(colError, tag)
			} else {
				tag = tint(colTrace, tag)
			}
		}
		fmt.Fprintf(
			e.w,
			"%s id=%s name=%q start=%s dur=%s status=%s\n",
			tag,
			s.SpanContext().TraceID().String(),
			s.Name(),
			s.StartTime().Format(time.RFC3339),
			s.EndTime().Sub(s.StartTime()),
			status,
		)
	}
	return nil
}

// Shutdown does nothing
func (e *LineTraceExporter) Shutdown(ctx context.Context) error {
	return nil
}

var _ tracesdk.SpanExporter = (*LineTraceExporter)(nil)

// LineMetricExporter is an OpenTelemetry metric exporter that prints metrics to
// STDOUT. Useful for local testing.
type LineMetricExporter struct {
	Colors bool
	w      io.Writer
}

// Temporality returns CumulativeTemporality
func (l LineMetricExporter) Temporality(kind metricsdk.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

// Aggregation returns the aggregation type based on instrument kind
func (l LineMetricExporter) Aggregation(kind metricsdk.InstrumentKind) metricsdk.Aggregation {
	switch kind {
	case metricsdk.InstrumentKindCounter,
		metricsdk.InstrumentKindUpDownCounter,
		metricsdk.InstrumentKindObservableCounter:
		return metricsdk.AggregationSum{}
	case metricsdk.InstrumentKindGauge, metricsdk.InstrumentKindObservableGauge:
		return metricsdk.AggregationLastValue{}
	case metricsdk.InstrumentKindHistogram:
		return metricsdk.AggregationDrop{}
	default:
		return metricsdk.AggregationDefault{}
	}
}

// Export takes a metric and prints it to STDOUT.
func (l LineMetricExporter) Export(ctx context.Context, metrics *metricdata.ResourceMetrics) error {
	metricTag := "[metric]"
	if l.Colors {
		metricTag = tint(colMetric, "[metric]")
	}
	for _, sm := range metrics.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					fmt.Fprintf(l.w, "%s %s name=%s value=%d\n", metricTag, dp.Time.Format(time.RFC3339), m.Name, dp.Value)
				}
			case metricdata.Sum[float64]:
				for _, dp := range data.DataPoints {
					fmt.Fprintf(l.w, "%s %s name=%s value=%f\n", metricTag, dp.Time.Format(time.RFC3339), m.Name, dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					fmt.Fprintf(l.w, "%s %s name=%s value=%d\n", metricTag, dp.Time.Format(time.RFC3339), m.Name, dp.Value)
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					fmt.Fprintf(l.w, "%s %s name=%s value=%f\n", metricTag, dp.Time.Format(time.RFC3339), m.Name, dp.Value)
				}
			default:
				// Ignoring histograms, summaries, etc.
				fmt.Fprintf(l.w, "%s unsupported metric type: %T\n", metricTag, data)
			}
		}
	}
	return nil
}

// ForceFlush does nothing
func (l LineMetricExporter) ForceFlush(ctx context.Context) error {
	return nil
}

// Shutdown does nothing
func (l LineMetricExporter) Shutdown(ctx context.Context) error {
	return nil
}

var _ metricsdk.Exporter = (*LineMetricExporter)(nil)

// LineLogExporter is an OpenTelemetry log exporter that prints log lines to
// STDOUT. Useful for local testing.
type LineLogExporter struct {
	Colors bool
	w      io.Writer
}

// Export takes a slice of log records and prints them to STDOUT
func (l *LineLogExporter) Export(ctx context.Context, records []logsdk.Record) error {
	for i := range records {
		record := &records[i]
		severityText := record.SeverityText()

		if l.Colors {
			if record.Severity() >= log.SeverityError1 {
				severityText = tint(colError, severityText)
			} else if record.Severity() >= log.SeverityWarn1 {
				severityText = tint(colWarn, severityText)
			}
		}

		line := fmt.Sprintf(
			"%s sev=%s msg=%q",
			record.Timestamp().Format(time.RFC3339),
			severityText,
			record.Body().AsString(),
		)
		record.WalkAttributes(func(kv log.KeyValue) bool {
			line = fmt.Sprintf("%s %s=%q", line, kv.Key, kv.Value)
			return true
		})
		fmt.Fprintln(l.w, line)
	}
	return nil
}

// Shutdown does nothing
func (l *LineLogExporter) Shutdown(ctx context.Context) error {
	return nil
}

// ForceFlush does nothing
func (l *LineLogExporter) ForceFlush(ctx context.Context) error {
	return nil
}

var _ logsdk.Exporter = (*LineLogExporter)(nil)
