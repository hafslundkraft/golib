package kafkarator

import (
	"context"
	"fmt"
	"net/http"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	subjectNameStringSpan = "schema_registry.subject_name"
	idStringSpan          = "schema_registry.schema_id"
)

// SchemaRegistryClient is an interface for interacting with the schema registry
type SchemaRegistryClient interface {
	GetBySubjectAndID(ctx context.Context, subject string, id int) (sr.SchemaInfo, error)
	GetLatestSchemaMetadata(ctx context.Context, subject string) (sr.SchemaMetadata, error)
}

type confluentSchemaRegistryClient struct {
	client sr.Client
}

type tracedSchemaRegistryClient struct {
	next   SchemaRegistryClient
	tracer trace.Tracer
}

type telemetryHTTPTransporter interface {
	HTTPTransport(rt http.RoundTripper) *otelhttp.Transport
}

func withSchemaRegistryTracing(client SchemaRegistryClient, tel TelemetryProvider) SchemaRegistryClient {
	if client == nil {
		return nil
	}
	if tel == nil {
		return client
	}
	if _, already := client.(*tracedSchemaRegistryClient); already {
		return client
	}

	return &tracedSchemaRegistryClient{
		next:   client,
		tracer: tel.Tracer(),
	}
}

func newSchemaRegistryClient(cfg *SchemaRegistryConfig, tel TelemetryProvider) (SchemaRegistryClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("schema registry config was not provided")
	}
	if cfg.SchemaRegistryURL == "" {
		return nil, fmt.Errorf("schema registry url is empty")
	}

	if tel == nil {
		return nil, fmt.Errorf("telemetry provider was not provided")
	}

	cfgSR := sr.NewConfigWithBasicAuthentication(
		cfg.SchemaRegistryURL, cfg.SchemaRegistryUser, cfg.SchemaRegistryPassword,
	)
	cfgSR.HTTPClient = instrumentHTTPClient(cfgSR.HTTPClient, tel)
	srClient, err := sr.NewClient(cfgSR)
	if err != nil {
		return nil, fmt.Errorf("create schema registry client (url=%s): %w",
			cfg.SchemaRegistryURL,
			err,
		)
	}
	base := &confluentSchemaRegistryClient{client: srClient}
	return withSchemaRegistryTracing(base, tel), nil
}

func (a *confluentSchemaRegistryClient) GetBySubjectAndID(
	ctx context.Context,
	subject string,
	id int,
) (sr.SchemaInfo, error) {
	// NOTE: ctx cannot be passed into confluent SR calls directly (their API doesn't accept ctx),
	// but we keep ctx in our interface for spans.
	schemaInfo, err := a.client.GetBySubjectAndID(subject, id)
	return schemaInfo, fmt.Errorf("confluent SR GetBySubjectAndID error (subject=%s, id=%d): %w", subject, id, err)
}

func (a *confluentSchemaRegistryClient) GetLatestSchemaMetadata(
	ctx context.Context,
	subject string,
) (sr.SchemaMetadata, error) {
	meta, err := a.client.GetLatestSchemaMetadata(subject)
	return meta, fmt.Errorf("confluent SR GetLatestSchemaMetadata error (subject=%s): %w", subject, err)
}

func (c *tracedSchemaRegistryClient) GetBySubjectAndID(
	ctx context.Context,
	subject string,
	id int,
) (sr.SchemaInfo, error) {
	ctx, span := c.tracer.Start(ctx, "schema_registry.get_by_subject_and_id", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	span.SetAttributes(
		attribute.String(subjectNameStringSpan, subject),
		attribute.Int(idStringSpan, id),
	)

	info, err := c.next.GetBySubjectAndID(ctx, subject, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return sr.SchemaInfo{}, fmt.Errorf("traced SR GetSubjectAndID error (subject=%s, id=%d): %w", subject, id, err)
	}

	return info, nil
}

func (c *tracedSchemaRegistryClient) GetLatestSchemaMetadata(
	ctx context.Context,
	subject string,
) (sr.SchemaMetadata, error) {
	ctx, span := c.tracer.Start(
		ctx,
		"schema_registry.get_latest_schema_metadata",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	meta, err := c.next.GetLatestSchemaMetadata(ctx, subject)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return sr.SchemaMetadata{}, fmt.Errorf("traced SR GetLatestSchemaMetadata error (subject=%s): %w", subject, err)
	}

	span.SetAttributes(attribute.String(subjectNameStringSpan, subject))
	span.SetAttributes(attribute.Int(idStringSpan, meta.ID))
	return meta, nil
}

func instrumentHTTPClient(base *http.Client, tel TelemetryProvider) *http.Client {
	if base == nil {
		base = http.DefaultClient
	}

	c := *base

	rt := c.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}

	if _, already := rt.(*otelhttp.Transport); already {
		return &c
	}

	if t, ok := tel.(telemetryHTTPTransporter); ok {
		c.Transport = t.HTTPTransport(rt)
		return &c
	}

	c.Transport = otelhttp.NewTransport(rt)

	return &c
}
