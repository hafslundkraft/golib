package kafkarator

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hafslundkraft/golib/kafkarator/auth"
	"github.com/hafslundkraft/golib/telemetry"
)

const (
	meterProducedMessages = "messages_produced_total"
	meterConsumedMessages = "kafka_messages_consumed"
	gaugeLag              = "kafka_message_lag"
)

type Connection struct {
	config    Config
	configMap *kafka.ConfigMap
	tel       *telemetry.Provider
}

// New creates and returns a new connection.
func New(config Config, tel *telemetry.Provider) (*Connection, error) {
	configMap, err := buildKafkaConfigMap(config, tel)
	if err != nil {
		return nil, fmt.Errorf("failed building Kafka config map: %w", err)
	}
	return &Connection{
		config:    config,
		tel:       tel,
		configMap: configMap,
	}, nil
}

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

func (c *Connection) Writer(topic string) (*Writer, error) {
	conf := cloneConfigMap(c.configMap)

	p, err := kafka.NewProducer(&conf)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	if c.config.SASL.Enabled {
		go c.startOAuth(ctxBackground(), p)
	}

	counter, _ := c.tel.Meter().Int64Counter(meterProducedMessages)

	return newWriter(p, topic, counter, c.tel), nil
}

func (c *Connection) Reader(topic, group string) (*Reader, error) {
	conf := cloneConfigMap(c.configMap)

	conf["group.id"] = group
	conf["auto.offset.reset"] = "earliest"

	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	if c.config.SASL.Enabled {
		go c.startOAuth(ctxBackground(), consumer)
	}

	lagGauge, err := c.tel.Meter().Int64Gauge(gaugeLag)
	if err != nil {
		consumer.Close()
		return nil, err
	}

	counter, err := c.tel.Meter().Int64Counter(meterConsumedMessages)
	if err != nil {
		consumer.Close()
		return nil, err
	}

	return newReader(consumer, counter, lagGauge, c.tel), nil
}

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
	tp, err := auth.NewDefaultTokenProvider(c.config.SASL.Scope)
	if err != nil {
		c.tel.Logger().Error("failed to create token provider", "error", err)
		return
	}

	tracer := c.tel.Tracer()

	go auth.StartOAuthRefreshLoop(ctx, tp, tr, tracer)
}

func ctxBackground() context.Context {
	return context.Background()
}
