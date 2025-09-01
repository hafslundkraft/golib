package telemetry

import (
	"context"
	"fmt"
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

type LineTraceExporter struct {
	Colors bool
}

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
		fmt.Printf(
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

func (e *LineTraceExporter) Shutdown(ctx context.Context) error {
	return nil
}

var _ tracesdk.SpanExporter = (*LineTraceExporter)(nil)

type LineMetricExporter struct {
	Colors bool
}

func (l LineMetricExporter) Temporality(kind metricsdk.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (l LineMetricExporter) Aggregation(kind metricsdk.InstrumentKind) metricsdk.Aggregation {
	switch kind {
	case metricsdk.InstrumentKindCounter, metricsdk.InstrumentKindUpDownCounter, metricsdk.InstrumentKindObservableCounter:
		return metricsdk.AggregationSum{}
	case metricsdk.InstrumentKindGauge, metricsdk.InstrumentKindObservableGauge:
		return metricsdk.AggregationLastValue{}
	case metricsdk.InstrumentKindHistogram:
		return metricsdk.AggregationDrop{}
	default:
		return metricsdk.AggregationDefault{}
	}
}

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
					fmt.Printf("%s %s name=%s value=%d\n", metricTag, dp.Time.Format(time.RFC3339), m.Name, dp.Value)
				}
			case metricdata.Sum[float64]:
				for _, dp := range data.DataPoints {
					fmt.Printf("%s %s name=%s value=%f\n", metricTag, dp.Time.Format(time.RFC3339), m.Name, dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					fmt.Printf("%s %s name=%s value=%d\n", metricTag, dp.Time.Format(time.RFC3339), m.Name, dp.Value)
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					fmt.Printf("%s %s name=%s value=%f\n", metricTag, dp.Time.Format(time.RFC3339), m.Name, dp.Value)
				}
			default:
				// Ignoring histograms, summaries, etc.
				fmt.Printf("%s unsupported metric type: %T\n", metricTag, data)
			}
		}
	}
	return nil
}

func (l LineMetricExporter) ForceFlush(ctx context.Context) error {
	return nil
}

func (l LineMetricExporter) Shutdown(ctx context.Context) error {
	return nil
}

var _ metricsdk.Exporter = (*LineMetricExporter)(nil)

type LineLogExporter struct {
	Colors bool
}

func (l *LineLogExporter) Export(ctx context.Context, records []logsdk.Record) error {
	for _, record := range records {
		severityText := record.SeverityText()

		if l.Colors {
			if record.Severity() >= 17 {
				severityText = tint(colError, severityText)
			} else if record.Severity() >= 13 {
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
		fmt.Println(line)
	}
	return nil
}

func (l *LineLogExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (l *LineLogExporter) ForceFlush(ctx context.Context) error {
	return nil
}

var _ logsdk.Exporter = (*LineLogExporter)(nil)
