package telemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	logsdk "go.opentelemetry.io/otel/sdk/log"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const happiTelemetryName = "happi-telemetry"

// Provider is the telemetry provider. Contains handles for logging, metrics and
// traces.
type Provider struct {
	propagator  propagation.TextMapPropagator
	serviceName string

	tracer         trace.Tracer
	meter          metric.Meter
	logger         *slog.Logger
	tracerProvider *tracesdk.TracerProvider
	meterProvider  *metricsdk.MeterProvider
}

type config struct {
	localW      io.Writer
	local       bool
	localColors bool
	attributes  map[string]string
	testIDGen   bool
}

func (c config) localWriter() io.Writer {
	if c.localW != nil {
		return c.localW
	}
	return os.Stdout
}

// New creates a telemetry provider for the OpenTelemetry stack.
//
// The caller should call the returned shutdown function to make sure remaining
// data is flushed and resources are freed.
func New(
	ctx context.Context,
	serviceName string,
	opts ...OptionFunc,
) (provider *Provider, shutdown func(ctx context.Context) error) {
	cfg := config{localColors: true}
	for _, opt := range opts {
		opt(&cfg)
	}

	shutdownFuncs := make([]func(context.Context) error, 0, 3)

	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	lp := newLoggerProvider(ctx, cfg)
	shutdownFuncs = append(shutdownFuncs, lp.Shutdown)
	tp := newTracerProvider(ctx, cfg)
	shutdownFuncs = append(shutdownFuncs, tp.Shutdown)
	mp := newMeterProvider(ctx, cfg)
	shutdownFuncs = append(shutdownFuncs, mp.Shutdown)

	propagator := newPropagator()

	logger := otelslog.NewLogger(serviceName, otelslog.WithLoggerProvider(lp))

	// Sort attribute keys for deterministic ordering
	keys := make([]string, 0, len(cfg.attributes))
	for k := range cfg.attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		logger = logger.With(k, cfg.attributes[k])
	}

	return &Provider{
		propagator:  propagator,
		serviceName: serviceName,

		tracerProvider: tp,
		meterProvider:  mp,

		tracer: tp.Tracer(happiTelemetryName),
		meter:  mp.Meter(happiTelemetryName),
		logger: logger,
	}, shutdown
}

// Logger returns the logger
func (p *Provider) Logger() *slog.Logger {
	return p.logger
}

// Meter returns the meter
func (p *Provider) Meter() metric.Meter {
	return p.meter
}

// Tracer returns the tracer
func (p *Provider) Tracer() trace.Tracer {
	return p.tracer
}

// HTTPMiddleware constructs a plain http middleware that instruments incoming
// requests with traces and metrics.
func (p *Provider) HTTPMiddleware() func(http.Handler) http.Handler {
	return otelhttp.NewMiddleware("http-server",
		otelhttp.WithPropagators(p.propagator),
		otelhttp.WithTracerProvider(p.tracerProvider),
		otelhttp.WithMeterProvider(p.meterProvider),
		otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
			if r.Pattern == "" {
				return r.Method + " 404 Not Found"
			}
			return r.Pattern
		}),
	)
}

// HTTPTransport constructs an instrumented RoundTripper. Add this to an HTTP
// client to instrument outgoing HTTP calls.
func (p *Provider) HTTPTransport(rt http.RoundTripper) *otelhttp.Transport {
	return otelhttp.NewTransport(rt,
		otelhttp.WithPropagators(p.propagator),
		otelhttp.WithTracerProvider(p.tracerProvider),
		otelhttp.WithMeterProvider(p.meterProvider),
	)
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTracerProvider(ctx context.Context, cfg config) *tracesdk.TracerProvider {
	otlpExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		panic(err)
	}

	var exporter tracesdk.SpanExporter = otlpExporter
	if cfg.local {
		exporter = &LineTraceExporter{Colors: cfg.localColors, w: cfg.localWriter()}
	}

	opts := []tracesdk.TracerProviderOption{tracesdk.WithBatcher(exporter)}
	if cfg.testIDGen {
		opts = append(opts, tracesdk.WithIDGenerator(singleIDGenerator{}))
	}

	return tracesdk.NewTracerProvider(opts...)
}

func newMeterProvider(ctx context.Context, cfg config) *metricsdk.MeterProvider {
	otlpExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		panic(err)
	}

	var exporter metricsdk.Exporter = otlpExporter
	if cfg.local {
		exporter = &LineMetricExporter{Colors: cfg.localColors, w: cfg.localWriter()}
	}

	return metricsdk.NewMeterProvider(metricsdk.WithReader(metricsdk.NewPeriodicReader(exporter)))
}

func newLoggerProvider(ctx context.Context, cfg config) *logsdk.LoggerProvider {
	opts := make([]logsdk.LoggerProviderOption, 0, 2)
	if cfg.local {
		opts = append(
			opts,
			logsdk.WithProcessor(
				logsdk.NewBatchProcessor(&LineLogExporter{Colors: cfg.localColors, w: cfg.localWriter()}),
			),
		)
	} else {
		otlpLogExporter, err := otlploghttp.New(ctx)
		if err != nil {
			panic(err)
		}
		opts = append(opts,
			logsdk.WithProcessor(logsdk.NewBatchProcessor(otlpLogExporter)),
		)
	}

	return logsdk.NewLoggerProvider(opts...)
}
