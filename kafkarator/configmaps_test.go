package kafkarator

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
)

func TestApplyConsumerConfig_DefaultsMaxPollInterval(t *testing.T) {
	conf := kafka.ConfigMap{}

	applyConsumerConfig(conf, "my-group", OffsetEarliest, 0)

	assert.Equal(t, "my-group", conf["group.id"])
	assert.Equal(t, string(OffsetEarliest), conf["auto.offset.reset"])
	assert.Equal(t, DefaultMaxPollIntervalMs, conf["max.poll.interval.ms"])
	assert.Equal(t, 300_000, conf["max.poll.interval.ms"],
		"default max.poll.interval.ms should be 5 minutes (300000 ms)")
}

func TestApplyConsumerConfig_NegativeMaxPollIntervalUsesDefault(t *testing.T) {
	conf := kafka.ConfigMap{}

	applyConsumerConfig(conf, "my-group", OffsetLatest, -1)

	assert.Equal(t, DefaultMaxPollIntervalMs, conf["max.poll.interval.ms"])
}

func TestApplyConsumerConfig_UsesProvidedMaxPollInterval(t *testing.T) {
	conf := kafka.ConfigMap{}

	applyConsumerConfig(conf, "my-group", OffsetLatest, 600_000)

	assert.Equal(t, "my-group", conf["group.id"])
	assert.Equal(t, string(OffsetLatest), conf["auto.offset.reset"])
	assert.Equal(t, 600_000, conf["max.poll.interval.ms"])
}

func TestApplyConsumerConfig_PreservesExistingBaseKeys(t *testing.T) {
	conf := kafka.ConfigMap{
		"bootstrap.servers": "broker:9092",
		"security.protocol": "SSL",
	}

	applyConsumerConfig(conf, "g", OffsetEarliest, 1000)

	assert.Equal(t, "broker:9092", conf["bootstrap.servers"])
	assert.Equal(t, "SSL", conf["security.protocol"])
	assert.Equal(t, 1000, conf["max.poll.interval.ms"])
}
