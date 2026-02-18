package kafkarator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2"

	"github.com/hafslundkraft/golib/kafkarator/internal/auth"
)

const (
	// Custom metrics not in semconv
	meterPollFailures = "messaging.client.poll.failures"
)

// Connection represents a Connection to a Kafka service. Connection supports both writing
// and reading messages. For reading, a consumer group must be supplied. This means
// that multiple copies of the service using this library can be started simultaneously, and Kafka
// will automatically balance consumption between the consumers, i.e. the service can be scaled
// horizontally. Of course, this only makes sense if the topic has more than one partition.
//
// Three modes of reading are supported: ChannelReader, Reader and Processor. The former exposes a channel
// that emits messages, while the second exposes a reader. They support two different
// use-cases where ChannelReader is best for low volume scenarios, while Reader is best for
// high volume scenarios and/or situations where the client needs to control exactly how and
// when high watermark offsets are committed. Processor is also available as a higher-level
// abstraction that wraps Reader with automatic trace propagation and offset management.
//
// For writing, a writer is exposed. It supports writing messages, one at a time.
type Connection struct {
	config    Config
	configMap *kafka.ConfigMap
	tel       TelemetryProvider
	srClient  SchemaRegistryClient

	tokenProvider auth.AccessTokenProvider // this is optional
}

// TelemetryProvider interface providing logger, metrics and tracing
type TelemetryProvider interface {
	Logger() *slog.Logger
	Meter() metric.Meter
	Tracer() trace.Tracer
}

// Option ... to pass to the NewConnection() connection
type Option func(*options)

type options struct {
	tokenSource oauth2.TokenSource
	srClient    SchemaRegistryClient
}

// WithTokenSource provides the optional TokenSource to use instead of default token provider
func WithTokenSource(ts oauth2.TokenSource) Option {
	return func(o *options) {
		o.tokenSource = ts
	}
}

// WithSchemaRegistryClient sets the schema registry client to use internally.
// This is useful for tests and examples using mock schema registries.
func WithSchemaRegistryClient(client SchemaRegistryClient) Option {
	return func(o *options) {
		o.srClient = client
	}
}

// NewConnection creates and returns a new connection.
func NewConnection(
	config *Config,
	tel TelemetryProvider,
	opts ...Option,
) (*Connection, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if tel == nil {
		return nil, fmt.Errorf("telemetry provider is nil")
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}

	configMap, err := buildKafkaConfigMap(config)
	if err != nil {
		return nil, fmt.Errorf("failed building Kafka config map: %w", err)
	}

	var tp auth.AccessTokenProvider

	if config.AuthMode == AuthSASL {
		switch {
		case o.tokenSource != nil:
			tp = auth.NewOAuth2TokenSourceAdapter(o.tokenSource)

		default:
			if config.SASL.Scope == "" {
				return nil, fmt.Errorf("KAFKA_SASL_SCOPE env variable not set")
			}

			tp, err = auth.NewDefaultTokenProvider(config.SASL.Scope)
			if err != nil {
				return nil, fmt.Errorf("failed to create token provider: %w", err)
			}
		}
	}

	srClient := o.srClient
	if srClient == nil && config.SchemaRegistryConfig.SchemaRegistryURL != "" {
		srClient, err = newSchemaRegistryClient(&config.SchemaRegistryConfig, tel)
		if err != nil {
			return nil, fmt.Errorf("schema registry client: %w", err)
		}
	}

	srClient = withSchemaRegistryTracing(srClient, tel)

	return &Connection{
		config:        *config,
		tel:           tel,
		configMap:     configMap,
		srClient:      srClient,
		tokenProvider: tp,
	}, nil
}

// Test tests whether a Connection to Kafka has been established. It is designed to be called early by the client
// application so that apps can fail early if something is wrong with the Connection.
func (c *Connection) Test(ctx context.Context) error {
	conf := cloneConfigMap(c.configMap)
	admin, err := kafka.NewAdminClient(&conf)
	if err != nil {
		return fmt.Errorf("create admin: %w", err)
	}

	if c.config.AuthMode == AuthSASL {
		if err := c.startOAuth(ctx, admin); err != nil {
			admin.Close()
			return fmt.Errorf("create token provider: %w", err)
		}
	}

	_, err = admin.GetMetadata(nil, true, 5_000)
	if err != nil {
		return fmt.Errorf("metadata failed: %w", err)
	}

	return nil
}

// Serializer returns a serializer for serializing Go objects to Avro bytes
func (c *Connection) Serializer() ValueSerializer {
	return newAvroSerializer(c.srClient, c.tel)
}

// Writer returns a writer for writing messages to Kafka.
func (c *Connection) Writer() (*Writer, error) {
	conf := cloneConfigMap(c.configMap)

	p, err := kafka.NewProducer(&conf)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	if c.config.AuthMode == AuthSASL {
		if err := c.startOAuth(context.Background(), p); err != nil {
			p.Close()
			return nil, err
		}
	}

	counter, err := messagingconv.NewClientSentMessages(c.tel.Meter())
	if err != nil {
		p.Close()
		return nil, fmt.Errorf("create sent messages counter: %w", err)
	}

	return newWriter(p, counter, c.tel), nil
}

// Deserializer returns a deserializer for deserializing Avro bytes to Go objects
func (c *Connection) Deserializer() ValueDeserializer {
	return newAvroDeserializer(c.srClient, c.tel)
}

// ReaderOption for options to pass to the Reader() function
type ReaderOption func(*readerOptions)

type readerOptions struct {
	autoOffsetReset string
}

func defaultReaderOptions() readerOptions {
	return readerOptions{
		autoOffsetReset: "earliest",
	}
}

// WithAutoOffsetReset overrides Kafka auto.offset.reset.
// Default is `earliest` if not provided.
//
// Possible values:
//   - `earliest`: start from the earliest available offset when no committed offset exists
//   - `latest`: start from the latest offset when no committed offset exists
//   - `none`: error if no committed offset exists for the consumer group
func WithAutoOffsetReset(value string) ReaderOption {
	return func(o *readerOptions) {
		o.autoOffsetReset = value
	}
}

// Reader returns a reader that is used to fetch messages from Kafka.
func (c *Connection) Reader(topic, group string, opts ...ReaderOption) (*Reader, error) {
	ro := defaultReaderOptions()

	for _, opt := range opts {
		opt(&ro)
	}

	conf := cloneConfigMap(c.configMap)

	conf["group.id"] = group
	conf["auto.offset.reset"] = ro.autoOffsetReset

	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	if c.config.AuthMode == AuthSASL {
		if err := c.startOAuth(context.Background(), consumer); err != nil {
			_ = consumer.Close()
			return nil, fmt.Errorf("start oauth: %w", err)
		}
	}
	counter, err := messagingconv.NewClientConsumedMessages(c.tel.Meter())
	if err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("create consumed messages counter: %w", err)
	}

	failureCounter, err := c.tel.Meter().Int64Counter(meterPollFailures)
	if err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("create meter counter %q: %w", meterPollFailures, err)
	}

	r, err := newReader(consumer, counter, failureCounter, c.tel, topic, group)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Processor returns a processor that automatically handles trace propagation and span management
// for message processing. It wraps a Reader and provides a higher-level abstraction where:
//   - Messages are read in groups (up to maxMessages per ProcessNext call)
//   - Trace context is automatically extracted from message headers
//   - Processing spans are created for each message
//   - Handler is called once per message individually
//   - Offsets are committed once after all messages are successfully processed
//
// The handler function is called for each message individually with a context that includes
// the propagated trace. If any message fails, processing stops and offsets are NOT committed.
//
// This is ideal for services that want automatic distributed tracing and offset management
// without manual span handling. For more control over reading and committing, use Reader() instead.
//
// Use ProcessorOption to configure optional parameters like readTimeout.
func (c *Connection) Processor(
	topic string,
	consumerGroup string,
	handler ProcessFunc,
	opts ...ProcessorOption,
) (*Processor, error) {
	cfg := defaultProcessorConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	reader, err := c.Reader(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("creating reader: %w", err)
	}
	return newProcessor(reader, c.tel, handler, cfg.readTimeout, cfg.maxMessages), nil
}

// ChannelReader returns a channel that emits messages from the given Kafka topic.
//
// If an internal error is raised, the error will be logged, and the channel will be closed.
//
// The high watermark offset is automatically committed for each message. This potentially has significant
// performance consequences. Also, it sacrifices control, for instance the client's handling of a message
// might fail its offset is committed. Please use Reader if you are concerned about performance, or you
// want to explicitly commit offsets.
func (c *Connection) ChannelReader(
	ctx context.Context,
	topic string,
	group string,
) (<-chan Message, error) {
	reader, err := c.Reader(topic, group)
	if err != nil {
		return nil, fmt.Errorf("creating reader: %w", err)
	}

	outgoing := make(chan Message)

	go func() {
		defer reader.Close(ctx)
		defer close(outgoing)

		for {
			select {
			case <-ctx.Done():
				// Caller is stopping the consumer
				return

			default:
				// Read max 1 message, wait up to 10 seconds
				msgs, committer, err := reader.Read(ctx, 1, 10*time.Second)
				if err != nil {
					// Reader already logs errors; exit silently
					return
				}

				for _, m := range msgs {
					outgoing <- m
				}

				if err := committer(ctx); err != nil {
					return
				}
			}
		}
	}()

	return outgoing, nil
}

func (c *Connection) startOAuth(ctx context.Context, tr auth.TokenReceiver) error {
	tracer := c.tel.Tracer()
	if c.tokenProvider == nil {
		return fmt.Errorf("no token provider configured")
	}

	if err := auth.StartOAuthRefreshLoop(ctx, c.tokenProvider, tr, tracer); err != nil {
		return fmt.Errorf("start oauth refresh loop: %w", err)
	}

	return nil
}
