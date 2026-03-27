package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/ava-labs/avalanche-indexer/pkg/data/dynamodb"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
)

// Config holds all configuration for the coreconsumer application.
type Config struct {
	// Application settings
	Verbose bool

	// Kafka consumer settings
	BootstrapServers               string
	GroupID                        string
	Topic                          string
	DLQTopic                       string
	AutoOffsetReset                string
	Concurrency                    int64
	OffsetCommitInterval           time.Duration
	EnableKafkaLogs                bool
	SessionTimeout                 time.Duration
	MaxPollInterval                time.Duration
	FlushTimeout                   time.Duration
	GoroutineWaitTimeout           time.Duration
	PollInterval                   time.Duration
	PublishToDLQ                   bool
	KafkaTopicNumPartitions        int
	KafkaTopicReplicationFactor    int
	KafkaTopicRetentionMs          string
	KafkaTopicRetentionBytes       string
	KafkaDLQTopicNumPartitions     int
	KafkaDLQTopicReplicationFactor int
	KafkaDLQTopicRetentionMs       string
	KafkaDLQTopicRetentionBytes    string
	KafkaTopicMessageMaxBytes      string
	KafkaSASL                      kafka.SASLConfig

	// Consumer retry policy
	ConsumerRetryMaxRetries int
	ConsumerRetryBaseDelay  time.Duration
	ConsumerRetryMaxDelay   time.Duration

	// DynamoDB settings
	DynamoDB dynamodb.Config

	// Metrics settings
	MetricsHost   string
	MetricsPort   int
	ChainID       uint64
	Environment   string
	Region        string
	CloudProvider string
}

// MetricsAddr returns the formatted metrics address.
func (c *Config) MetricsAddr() string {
	return fmt.Sprintf("%s:%d", c.MetricsHost, c.MetricsPort)
}

// validateRetentionValue validates a Kafka retention configuration value.
func validateRetentionValue(value, fieldName string) error {
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("%s must be a valid integer or -1 for infinite retention, got: %s", fieldName, value)
	}
	if parsed != -1 && parsed <= 0 {
		return fmt.Errorf("%s must be positive or -1 for infinite retention, got: %d", fieldName, parsed)
	}
	return nil
}

// buildConfig builds a Config from CLI context flags.
func buildConfig(c *cli.Context) (*Config, error) {
	topicRetentionMs := c.String("kafka-topic-retention-ms")
	if err := validateRetentionValue(topicRetentionMs, "kafka-topic-retention-ms"); err != nil {
		return nil, err
	}
	topicRetentionBytes := c.String("kafka-topic-retention-bytes")
	if err := validateRetentionValue(topicRetentionBytes, "kafka-topic-retention-bytes"); err != nil {
		return nil, err
	}
	dlqRetentionMs := c.String("kafka-dlq-topic-retention-ms")
	if err := validateRetentionValue(dlqRetentionMs, "kafka-dlq-topic-retention-ms"); err != nil {
		return nil, err
	}
	dlqRetentionBytes := c.String("kafka-dlq-topic-retention-bytes")
	if err := validateRetentionValue(dlqRetentionBytes, "kafka-dlq-topic-retention-bytes"); err != nil {
		return nil, err
	}

	return &Config{
		Verbose:                        c.Bool("verbose"),
		BootstrapServers:               c.String("bootstrap-servers"),
		GroupID:                        c.String("group-id"),
		Topic:                          c.String("topic"),
		DLQTopic:                       c.String("dlq-topic"),
		AutoOffsetReset:                c.String("auto-offset-reset"),
		Concurrency:                    c.Int64("concurrency"),
		OffsetCommitInterval:           c.Duration("offset-commit-interval"),
		EnableKafkaLogs:                c.Bool("enable-kafka-logs"),
		SessionTimeout:                 c.Duration("session-timeout"),
		MaxPollInterval:                c.Duration("max-poll-interval"),
		FlushTimeout:                   c.Duration("flush-timeout"),
		GoroutineWaitTimeout:           c.Duration("goroutine-wait-timeout"),
		PollInterval:                   c.Duration("poll-interval"),
		PublishToDLQ:                   c.Bool("publish-to-dlq"),
		KafkaTopicNumPartitions:        c.Int("kafka-topic-num-partitions"),
		KafkaTopicReplicationFactor:    c.Int("kafka-topic-replication-factor"),
		KafkaTopicRetentionMs:          topicRetentionMs,
		KafkaTopicRetentionBytes:       topicRetentionBytes,
		KafkaDLQTopicNumPartitions:     c.Int("kafka-dlq-topic-num-partitions"),
		KafkaDLQTopicReplicationFactor: c.Int("kafka-dlq-topic-replication-factor"),
		KafkaDLQTopicRetentionMs:       dlqRetentionMs,
		KafkaDLQTopicRetentionBytes:    dlqRetentionBytes,
		KafkaTopicMessageMaxBytes:      c.String("kafka-topic-message-max-bytes"),
		ConsumerRetryMaxRetries:        c.Int("consumer-retry-max-retries"),
		ConsumerRetryBaseDelay:         c.Duration("consumer-retry-base-delay"),
		ConsumerRetryMaxDelay:          c.Duration("consumer-retry-max-delay"),
		KafkaSASL: kafka.SASLConfig{
			Username:         c.String("kafka-sasl-username"),
			Password:         c.String("kafka-sasl-password"),
			Mechanism:        c.String("kafka-sasl-mechanism"),
			SecurityProtocol: c.String("kafka-security-protocol"),
		},
		DynamoDB: dynamodb.Config{
			Region:       c.String("dynamodb-region"),
			Endpoint:     c.String("dynamodb-endpoint"),
			HistoryTable: c.String("dynamodb-history-table"),
			ERCTable:     c.String("dynamodb-erc-table"),
			StatusTable:  c.String("dynamodb-status-table"),
			MaxRetries:   c.Int("dynamodb-max-retries"),
			MaxBatchSize: 25,
			MaxInflight:  c.Int("dynamodb-max-inflight"),
		},
		MetricsHost:   c.String("metrics-host"),
		MetricsPort:   c.Int("metrics-port"),
		ChainID:       c.Uint64("chain-id"),
		Environment:   c.String("environment"),
		Region:        c.String("region"),
		CloudProvider: c.String("cloud-provider"),
	}, nil
}
