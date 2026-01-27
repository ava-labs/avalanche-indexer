package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/urfave/cli/v2"
)

const (
	// minBlockBufferSize is the minimum valid value for BlockBufferSize (uint8: 0)
	minBlockBufferSize = 0
	// maxBlockBufferSize is the maximum valid value for BlockBufferSize (uint8: 255)
	maxBlockBufferSize = 255
)

// validateBlockBufferSize validates that the block buffer size is within uint8 range (0-255)
// and returns the validated uint8 value or an error
func validateBlockBufferSize(size int) (uint8, error) {
	if size < minBlockBufferSize || size > maxBlockBufferSize {
		return 0, fmt.Errorf(
			"clickhouse-block-buffer-size must be between %d and %d, got %d",
			minBlockBufferSize, maxBlockBufferSize, size,
		)
	}
	return uint8(size), nil
}

// Config holds all configuration for the consumerindexer application
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
	KafkaDLQTopicNumPartitions     int
	KafkaDLQTopicReplicationFactor int

	// ClickHouse settings
	ClickHouse clickhouse.Config

	// Table names
	RawBlocksTableName       string
	RawTransactionsTableName string

	// Metrics settings
	MetricsHost   string
	MetricsPort   int
	ChainID       uint64
	Environment   string
	Region        string
	CloudProvider string
}

// MetricsAddr returns the formatted metrics address
func (c *Config) MetricsAddr() string {
	return fmt.Sprintf("%s:%d", c.MetricsHost, c.MetricsPort)
}

// buildConfig builds a Config from CLI context flags
func buildConfig(c *cli.Context) (*Config, error) {
	chCfg, err := buildClickHouseConfig(c)
	if err != nil {
		return nil, fmt.Errorf("failed to build ClickHouse config: %w", err)
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
		KafkaDLQTopicNumPartitions:     c.Int("kafka-dlq-topic-num-partitions"),
		KafkaDLQTopicReplicationFactor: c.Int("kafka-dlq-topic-replication-factor"),
		ClickHouse:                     chCfg,
		RawBlocksTableName:             c.String("raw-blocks-table-name"),
		RawTransactionsTableName:       c.String("raw-transactions-table-name"),
		MetricsHost:                    c.String("metrics-host"),
		MetricsPort:                    c.Int("metrics-port"),
		ChainID:                        c.Uint64("chain-id"),
		Environment:                    c.String("environment"),
		Region:                         c.String("region"),
		CloudProvider:                  c.String("cloud-provider"),
	}, nil
}

// buildClickHouseConfig builds a ClickhouseConfig from CLI context flags
func buildClickHouseConfig(c *cli.Context) (clickhouse.Config, error) {
	// Handle hosts - StringSliceFlag returns []string, but we need to handle comma-separated values
	hosts := c.StringSlice("clickhouse-hosts")
	// If hosts is a single comma-separated string, split it
	if len(hosts) == 1 && strings.Contains(hosts[0], ",") {
		hosts = strings.Split(hosts[0], ",")
		for i, host := range hosts {
			hosts[i] = strings.TrimSpace(host)
		}
	}

	// Validate and convert block buffer size to uint8 range
	blockBufferSize := c.Int("clickhouse-block-buffer-size")
	validatedSize, err := validateBlockBufferSize(blockBufferSize)
	if err != nil {
		return clickhouse.Config{}, err
	}

	return clickhouse.Config{
		Hosts:                hosts,
		Database:             c.String("clickhouse-database"),
		Username:             c.String("clickhouse-username"),
		Password:             c.String("clickhouse-password"),
		Debug:                c.Bool("clickhouse-debug"),
		InsecureSkipVerify:   c.Bool("clickhouse-insecure-skip-verify"),
		MaxExecutionTime:     c.Int("clickhouse-max-execution-time"),
		DialTimeout:          c.Int("clickhouse-dial-timeout"),
		MaxOpenConns:         c.Int("clickhouse-max-open-conns"),
		MaxIdleConns:         c.Int("clickhouse-max-idle-conns"),
		ConnMaxLifetime:      c.Int("clickhouse-conn-max-lifetime"),
		BlockBufferSize:      validatedSize,
		MaxBlockSize:         c.Int("clickhouse-max-block-size"),
		MaxCompressionBuffer: c.Int("clickhouse-max-compression-buffer"),
		ClientName:           c.String("clickhouse-client-name"),
		ClientVersion:        c.String("clickhouse-client-version"),
		UseHTTP:              c.Bool("clickhouse-use-http"),
	}, nil
}
