package kafkarator

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hafslundkraft/golib/telemetry"

	"github.com/hafslundkraft/golib/kafkarator/auth"
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
	tel       *telemetry.Provider
	srClient  sr.Client
}

// New creates and returns a new connection.
func New(config *Config, tel *telemetry.Provider) (*Connection, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if tel == nil {
		return nil, fmt.Errorf("telemetry provider is nil")
	}

	configMap, err := buildKafkaConfigMap(config)
	if err != nil {
		return nil, fmt.Errorf("failed building Kafka config map: %w", err)
	}

	var srClient sr.Client
	if config.UseSchemaRegistry {
		srClient, err = newSchemaRegistryClient(&config.SchemaRegistryConfig)
		if err != nil {
			return nil, fmt.Errorf("schema registry client: %w", err)
		}
	}

	return &Connection{
		config:    *config,
		tel:       tel,
		configMap: configMap,
		srClient:  srClient,
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

	_, err = admin.GetMetadata(nil, true, 5_000)
	if err != nil {
		return fmt.Errorf("metadata failed: %w", err)
	}

	return nil
}

// Serializer returns a serializer for serializing messages from bytes to avro
func (c *Connection) Serializer(topic string) ValueSerializer {
	return newAvroSerializer(c.srClient, topic, c.tel)
}

// Writer returns a writer for writing messages to Kafka.
func (c *Connection) Writer(topic string) (*Writer, error) {
	conf := cloneConfigMap(c.configMap)

	p, err := kafka.NewProducer(&conf)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	if c.config.AuthMode == "sasl" {
		go c.startOAuth(ctxBackground(), p)
	}

	counter, _ := c.tel.Meter().Int64Counter(meterProducedMessages)

	return newWriter(p, topic, counter, c.tel), nil
}

// Deserializer returns a deserializer for deserializing messages from avro to bytes
func (c *Connection) Deserializer(topic string) ValueDeserializer {
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
		go c.startOAuth(ctxBackground(), consumer)
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

func (c *Connection) startOAuth(ctx context.Context, tr auth.TokenReceiver) {
	tp, err := auth.NewDefaultTokenProvider(c.config.SASL.OID)
	if err != nil {
		c.tel.Logger().ErrorContext(ctx, "failed to create token provider", "error", err)
		return
	}

	tracer := c.tel.Tracer()

	if err := auth.StartOAuthRefreshLoop(ctx, tp, tr, tracer); err != nil {
		c.tel.Logger().ErrorContext(ctx, "failed to refresh token", "error", err)
		return
	}
}

func ctxBackground() context.Context {
	return context.Background()
}
