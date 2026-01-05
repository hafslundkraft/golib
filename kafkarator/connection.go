package kafkarator

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2"

	"github.com/hafslundkraft/golib/kafkarator/internal/auth"
)

const (
	meterProducedMessages = "messages_produced_total"
	meterConsumedMessages = "kafka_messages_consumed"
	gaugeLag              = "kafka_message_lag"
)

// Connection represents a Connection to a Kafka service. Connection (currently) only supports
// message consumption via consumer group, so the group to use must be supplied. This means
// that multiple copies of the service using this library can be started simultaneously, and Kafka
// will automatically balance consumption between the consumers, i.e. the service can be scaled
// horizontally. Of course, this only makes sense if the topic has more than one partition.
//
// Two modes of reading are supported: ChannelReader and Reader. The former exposes a channel
// that emits messages, while the latter exposes a reader. They support two different
// use-cases where ChannelReader is best for low volume scenarios, while Reader is best for
// high volume scenarios and/or situations where the client needs to control exactly how and
// when high watermark offsets are committed.
//
// For writing, a writer is exposed. It supported writing messages, one at a time.
type Connection struct {
	config    Config
	configMap *kafka.ConfigMap
	tel       TelemetryProvider
	srClient  sr.Client

	tokenProvider auth.AccessTokenProvider // this is optional
}

// TelemetryProvider interface providing logger, metrics and tracing
type TelemetryProvider interface {
	Logger() Logger
	Meter() metric.Meter
	Tracer() trace.Tracer
}

// Logger interface for telemetry logging
type Logger interface {
	ErrorContext(ctx context.Context, msg string, args ...any)
}

// Option ... to pass to the New() connection
type Option func(*options)

type options struct {
	tokenSource oauth2.TokenSource
}

// WithTokenSource provides the optional TokenSource to use instead of default token provider
func WithTokenSource(ts oauth2.TokenSource) Option {
	return func(o *options) {
		o.tokenSource = ts
	}
}

// New creates and returns a new connection.
func New(
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

	if config.AuthMode == "sasl" {
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

	var srClient sr.Client
	if config.UseSchemaRegistry {
		srClient, err = newSchemaRegistryClient(&config.SchemaRegistryConfig)
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

	if c.config.AuthMode == "sasl" {
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

// Serializer returns a serializer for serializing messages from bytes to avro
func (c *Connection) Serializer() ValueSerializer {
	return newAvroSerializer(c.srClient, c.tel)
}

// Writer returns a writer for writing messages to Kafka.
func (c *Connection) Writer(topic string) (*Writer, error) {
	conf := cloneConfigMap(c.configMap)

	p, err := kafka.NewProducer(&conf)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	if c.config.AuthMode == "sasl" {
		if err := c.startOAuth(context.Background(), p); err != nil {
			p.Close()
			return nil, err
		}
	}

	counter, _ := c.tel.Meter().Int64Counter(meterProducedMessages)

	return newWriter(p, topic, counter, c.tel), nil
}

// Deserializer returns a deserializer for deserializing messages from avro to bytes
func (c *Connection) Deserializer() ValueDeserializer {
	return newAvroDeserializer(c.srClient, c.tel)
}

// Reader returns a reader that is used to fetch messages from Kafka.
func (c *Connection) Reader(topic, group string) (*Reader, error) {
	conf := cloneConfigMap(c.configMap)

	conf["group.id"] = group
	conf["auto.offset.reset"] = "earliest"

	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	if c.config.AuthMode == "sasl" {
		if err := c.startOAuth(context.Background(), consumer); err != nil {
			_ = consumer.Close()
			return nil, fmt.Errorf("start oauth: %w", err)
		}
	}

	lagGauge, err := c.tel.Meter().Int64Gauge(gaugeLag)
	if err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("create lag gauge %q: %w", gaugeLag, err)
	}

	counter, err := c.tel.Meter().Int64Counter(meterConsumedMessages)
	if err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("create meter counter %q: %w", meterConsumedMessages, err)
	}

	r, err := newReader(consumer, counter, lagGauge, c.tel, topic)
	if err != nil {
		return nil, err
	}

	return r, nil
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
