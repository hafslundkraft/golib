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
	"golang.org/x/oauth2"
)

const (
	subjectNameStringSpan = "schema_registry.subject_name"
	idStringSpan          = "schema_registry.schema_id"
)

type confluentSchemaRegistryClient interface {
	GetBySubjectAndID(subject string, id int) (sr.SchemaInfo, error)
	GetLatestSchemaMetadata(subject string) (sr.SchemaMetadata, error)
}

// SchemaRegistryClient is an interface for interacting with the schema registry, allowing for easier testing and abstraction over the actual client implementation.
// wraps confluent's sr.Client with methods that include context and tracing
type SchemaRegistryClient interface {
	GetBySubjectAndID(ctx context.Context, subject string, id int) (sr.SchemaInfo, error)
	GetLatestSchemaMetadata(ctx context.Context, subject string) (sr.SchemaMetadata, error)
}

type tracedSchemaRegistryClient struct {
	client confluentSchemaRegistryClient
	tracer trace.Tracer
}

type telemetryHTTPTransporter interface {
	HTTPTransport(rt http.RoundTripper) *otelhttp.Transport
}

func newSchemaRegistryClient(
	cfg *SchemaRegistryConfig,
	tel TelemetryProvider,
	ts oauth2.TokenSource,
) (SchemaRegistryClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("schema registry config was not provided")
	}
	if cfg.SchemaRegistryURL == "" {
		return nil, fmt.Errorf("schema registry url is empty")
	}

	if tel == nil {
		return nil, fmt.Errorf("telemetry provider was not provided")
	}

	var cfgSR *sr.Config
	switch {
	case ts != nil:
		cfgSR = sr.NewConfig(cfg.SchemaRegistryURL)
	case cfg.SchemaRegistryUser == "" && cfg.SchemaRegistryPassword == "":
		cfgSR = sr.NewConfig(cfg.SchemaRegistryURL)
	default:
		cfgSR = sr.NewConfigWithBasicAuthentication(
			cfg.SchemaRegistryURL, cfg.SchemaRegistryUser, cfg.SchemaRegistryPassword,
		)
	}
	cfgSR.HTTPClient = instrumentHTTPClient(cfgSR.HTTPClient, tel)
	if ts != nil {
		cfgSR.HTTPClient = withBearerTokenSource(cfgSR.HTTPClient, ts)
	}
	srClient, err := sr.NewClient(cfgSR)
	if err != nil {
		return nil, fmt.Errorf("create schema registry client (url=%s): %w",
			cfg.SchemaRegistryURL,
			err,
		)
	}
	return &tracedSchemaRegistryClient{
		client: srClient,
		tracer: tel.Tracer(),
	}, nil
}

func (c *tracedSchemaRegistryClient) GetBySubjectAndID(
	ctx context.Context,
	subject string,
	id int,
) (sr.SchemaInfo, error) {
	_, span := c.tracer.Start(ctx, "schema_registry.get_by_subject_and_id", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	span.SetAttributes(
		attribute.String(subjectNameStringSpan, subject),
		attribute.Int(idStringSpan, id),
	)

	info, err := c.client.GetBySubjectAndID(subject, id)
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
	_, span := c.tracer.Start(
		ctx,
		"schema_registry.get_latest_schema_metadata",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	meta, err := c.client.GetLatestSchemaMetadata(subject)
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

type bearerTransport struct {
	base http.RoundTripper
	ts   oauth2.TokenSource
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, err := t.ts.Token()
	if err != nil {
		return nil, fmt.Errorf("get schema registry bearer token: %w", err)
	}
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	return t.base.RoundTrip(req)
}

func withBearerTokenSource(base *http.Client, ts oauth2.TokenSource) *http.Client {
	if base == nil {
		base = http.DefaultClient
	}

	c := *base

	rt := c.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}

	c.Transport = &bearerTransport{base: rt, ts: ts}

	return &c
}
