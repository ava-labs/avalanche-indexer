package testutils

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// NewTestLogger creates a test logger that writes to testing.T
func NewTestLogger(t *testing.T) *zap.SugaredLogger {
	return zaptest.NewLogger(t).Sugar()
}

// NewTestMessage creates a test Kafka message with the given topic, partition, and offset
func NewTestMessage(topic string, partition int32, offset int64, key, value []byte) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    kafka.Offset(offset),
		},
		Key:   key,
		Value: value,
	}
}
