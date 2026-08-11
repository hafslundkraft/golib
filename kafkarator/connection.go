package kafkarator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2"

	"github.com/hafslundkraft/golib/kafkarator/internal/auth"
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
//
// Connection holds no broker connection of its own: it is a factory that carries
// configuration, telemetry and a Schema Registry client, and builds a fresh
// producer or consumer on each Writer, Reader, Processor or ChannelReader call.
// Each of those owns its Kafka handle and its OAuth refresh loop, and must be
// closed individually. The Reader behind ChannelReader is the exception: it is
// owned by the returned channel's goroutine and closed when that goroutine
// exits. Cancellation is only observed between messages, so callers must keep
// receiving until the channel closes; abandoning it strands the goroutine, and
// the Reader and refresh loop with it.
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

// WithTokenSource provides the optional TokenSource to use instead of default token provider.
//
// ts is called from the OAuth refresh loop, which cannot cancel it: oauth2.TokenSource
// takes no context. Back ts with an http.Client that has a timeout, otherwise a stalled
// token fetch blocks whoever is waiting for the loop to exit — Writer.Close,
// Reader.Close, Processor.Close or Connection.Test — for as long as it lasts.
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
				return nil, fmt.Errorf("%s env variable not set", envAzureScope)
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
	defer admin.Close()

	if c.config.AuthMode == AuthSASL {
		// The refresh loop is scoped to this call: the admin client does not
		// outlive Test, so neither should the goroutine feeding it tokens.
		stopAuth, err := c.startOAuth(ctx, admin)
		if err != nil {
			return fmt.Errorf("create token provider: %w", err)
		}
		// Registered after admin.Close and therefore run before it: the loop
		// must be gone before the handle it writes to is destroyed.
		defer stopAuth()
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
func (c *Connection) Writer() (_ *Writer, err error) {
	conf := cloneConfigMap(c.configMap)

	p, err := kafka.NewProducer(&conf)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	// Unwind on failure; on success ownership transfers to the Writer.
	var stopAuth func()
	defer func() {
		if err != nil {
			if stopAuth != nil {
				stopAuth()
			}
			p.Close()
		}
	}()

	if c.config.AuthMode == AuthSASL {
		// The refresh loop is scoped to this producer, not to the Connection:
		// it must stop when the writer is closed, not at process exit.
		if stopAuth, err = c.startOAuth(context.Background(), p); err != nil {
			return nil, err
		}
	}

	counter, err := messagingconv.NewClientSentMessages(c.tel.Meter())
	if err != nil {
		return nil, fmt.Errorf("create sent messages counter: %w", err)
	}

	opDuration, err := messagingconv.NewClientOperationDuration(
		c.tel.Meter(),
		metric.WithExplicitBucketBoundaries(messagingDurationBuckets...),
	)
	if err != nil {
		p.Close()
		return nil, fmt.Errorf("create operation duration histogram: %w", err)
	}

	return newWriter(p, counter, opDuration, parseServerInfo(c.config.Broker), c.tel, stopAuth), nil
}

// Deserializer returns a deserializer for deserializing Avro bytes to Go objects
func (c *Connection) Deserializer() ValueDeserializer {
	return newAvroDeserializer(c.srClient, c.tel)
}

// SchemaRegistryClient returns the underlying Schema Registry client.
// Useful for sub-packages (e.g. claimcheck) that need direct SR access.
func (c *Connection) SchemaRegistryClient() SchemaRegistryClient {
	return c.srClient
}

// Tracer returns the OpenTelemetry tracer from the connection's TelemetryProvider.
// Useful for sub-packages (e.g. claimcheck) that need to emit spans.
func (c *Connection) Tracer() trace.Tracer {
	return c.tel.Tracer()
}

// Logger returns the slog.Logger from the connection's TelemetryProvider.
// Useful for sub-packages (e.g. claimcheck) that need structured logging.
func (c *Connection) Logger() *slog.Logger {
	return c.tel.Logger()
}

// Config returns a copy of the connection's configuration.
func (c *Connection) Config() Config {
	return c.config
}

// ReaderOption for options to pass to the Reader() function
type ReaderOption func(*readerOptions) error

type readerOptions struct {
	autoOffsetReset AutoOffsetReset
}

func defaultReaderOptions() readerOptions {
	return readerOptions{
		autoOffsetReset: OffsetEarliest,
	}
}

// WithReaderAutoOffsetReset overrides Kafka auto.offset.reset.
// Default is `earliest` if not provided.
// Possible values:
//   - `earliest`: start from the earliest available offset when no committed offset exists
//   - `latest`: start from the latest offset when no committed offset exists
func WithReaderAutoOffsetReset(v AutoOffsetReset) ReaderOption {
	return func(o *readerOptions) error {
		if err := v.validate(); err != nil {
			return fmt.Errorf("WithReaderAutoOffsetReset: %w", err)
		}
		o.autoOffsetReset = v
		return nil
	}
}

// Reader returns a reader that is used to fetch messages from Kafka.
func (c *Connection) Reader(topic string, opts ...ReaderOption) (_ *Reader, err error) {
	ro := defaultReaderOptions()

	for _, opt := range opts {
		if err := opt(&ro); err != nil {
			return nil, fmt.Errorf("reader option failed: %w", err)
		}
	}

	conf := cloneConfigMap(c.configMap)
	group, err := c.consumerGroupName(topic)
	if err != nil {
		return nil, fmt.Errorf("consumer group name: %w", err)
	}

	applyConsumerConfig(conf, group, ro.autoOffsetReset, c.config.MaxPollIntervalMs)

	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	// Unwind on failure; on success ownership transfers to the Reader.
	var stopAuth func()
	defer func() {
		if err != nil {
			if stopAuth != nil {
				stopAuth()
			}
			_ = consumer.Close()
		}
	}()

	if err = consumer.Subscribe(topic, nil); err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	if c.config.AuthMode == AuthSASL {
		// The refresh loop is scoped to this consumer, not to the Connection:
		// it must stop when the reader is closed, not at process exit.
		if stopAuth, err = c.startOAuth(context.Background(), consumer); err != nil {
			return nil, err
		}
	}

	counter, err := messagingconv.NewClientConsumedMessages(c.tel.Meter())
	if err != nil {
		return nil, fmt.Errorf("create consumed messages counter: %w", err)
	}

	opDuration, err := messagingconv.NewClientOperationDuration(
		c.tel.Meter(),
		metric.WithExplicitBucketBoundaries(messagingDurationBuckets...),
	)
	if err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("create operation duration histogram: %w", err)
	}

	return newReader(
		consumer,
		counter,
		opDuration,
		parseServerInfo(c.config.Broker),
		c.tel,
		topic,
		group,
		stopAuth,
	), nil
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
	handler ProcessFunc,
	opts ...ProcessorOption,
) (*Processor, error) {
	cfg := defaultProcessorConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, fmt.Errorf("processor option failed: %w", err)
		}
	}

	reader, err := c.Reader(topic,
		WithReaderAutoOffsetReset(cfg.autoOffsetReset),
	)
	if err != nil {
		return nil, fmt.Errorf("creating reader: %w", err)
	}

	processDuration, err := messagingconv.NewProcessDuration(
		c.tel.Meter(),
		metric.WithExplicitBucketBoundaries(messagingDurationBuckets...),
	)
	if err != nil {
		_ = reader.Close(context.Background())
		return nil, fmt.Errorf("create process duration histogram: %w", err)
	}

	return newProcessor(reader, c.tel, handler, cfg.readTimeout, cfg.maxMessages, processDuration), nil
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
) (<-chan Message, error) {
	reader, err := c.Reader(topic)
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

// startOAuth starts the OAuth refresh loop feeding tr. The returned stop
// function terminates the loop and blocks until it has exited; the caller owns
// it and must call it before tearing tr down.
func (c *Connection) startOAuth(ctx context.Context, tr auth.TokenReceiver) (func(), error) {
	tracer := c.tel.Tracer()
	if c.tokenProvider == nil {
		return nil, fmt.Errorf("no token provider configured")
	}

	stop, err := auth.StartOAuthRefreshLoop(ctx, c.tokenProvider, tr, tracer)
	if err != nil {
		return nil, fmt.Errorf("start oauth refresh loop: %w", err)
	}

	return stop, nil
}

func (c *Connection) consumerGroupName(topic string) (string, error) {
	system := strings.TrimSpace(c.config.SystemName)
	env := strings.TrimSpace(c.config.Env)
	workloadName := strings.TrimSpace(c.config.WorkloadName)
	topic = strings.TrimSpace(topic)

	if system == "" || env == "" || workloadName == "" {
		return "", fmt.Errorf("system name, env and workload name must be set in config")
	}

	if topic == "" {
		return "", fmt.Errorf("topic cannot be empty")
	}

	return fmt.Sprintf("%s.%s.%s.%s", system, env, workloadName, topic), nil
}
