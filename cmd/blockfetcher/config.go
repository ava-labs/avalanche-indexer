package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	// minBlockBufferSize is the minimum valid value for BlockBufferSize (uint8: 0)
	minBlockBufferSize = 0
	// maxBlockBufferSize is the maximum valid value for BlockBufferSize (uint8: 255)
	maxBlockBufferSize = 255
	messageMaxBytes    = 20971521 // 20MB
)

// Config holds all configuration for the blockfetcher application
type Config struct {
	// Application settings
	Verbose bool

	// Blockchain settings
	EVMChainID uint64
	BCID       string
	RPCURL     string
	ClientType string
	Start      uint64
	End        uint64

	// Worker settings
	Concurrency    int64
	ReceiptTimeout time.Duration
	Backfill       int64
	BlocksCap      int
	MaxFailures    int

	// Kafka settings
	KafkaBrokers                string
	KafkaTopic                  string
	KafkaEnableLogs             bool
	KafkaClientID               string
	KafkaTopicNumPartitions     int
	KafkaTopicReplicationFactor int
	KafkaSASL                   kafka.SASLConfig

	// ClickHouse settings
	ClickHouse clickhouse.Config

	// Checkpoint settings
	CheckpointTableName string
	CheckpointInterval  time.Duration
	GapWatchdogInterval time.Duration
	GapWatchdogMaxGap   uint64

	// Metrics settings
	MetricsHost   string
	MetricsPort   int
	Environment   string
	Region        string
	CloudProvider string
}

// MetricsAddr returns the formatted metrics address
func (c *Config) MetricsAddr() string {
	return fmt.Sprintf("%s:%d", c.MetricsHost, c.MetricsPort)
}

// KafkaProducerConfig builds a Kafka producer ConfigMap from the config
func (c *Config) KafkaProducerConfig() *confluentKafka.ConfigMap {
	cfg := &confluentKafka.ConfigMap{
		// Required
		"bootstrap.servers": c.KafkaBrokers,
		"client.id":         c.KafkaClientID,

		// Reliability: wait for all replicas to acknowledge
		"acks": "all",

		// Performance tuning
		"linger.ms":        5,     // Batch messages for 5ms
		"batch.size":       16384, // 16KB batch size
		"compression.type": "lz4", // Fast compression

		// Idempotence for exactly-once semantics
		"enable.idempotence": true,

		// Go channel for logs (optional, enable for debugging)
		"go.logs.channel.enable": c.KafkaEnableLogs,
		"message.max.bytes":      messageMaxBytes,
	}
	// Apply SASL configuration if enabled
	c.KafkaSASL.ApplyToConfigMap(cfg)
	return cfg
}

// buildConfig builds a Config from CLI context flags
func buildConfig(c *cli.Context) (*Config, error) {
	chCfg, err := buildClickHouseConfig(c)
	if err != nil {
		return nil, fmt.Errorf("failed to build ClickHouse config: %w", err)
	}

	return &Config{
		Verbose:                     c.Bool("verbose"),
		EVMChainID:                  c.Uint64("evm-chain-id"),
		BCID:                        c.String("bc-id"),
		RPCURL:                      c.String("rpc-url"),
		ClientType:                  c.String("client-type"),
		Start:                       c.Uint64("start-height"),
		End:                         c.Uint64("end-height"),
		Concurrency:                 c.Int64("concurrency"),
		ReceiptTimeout:              c.Duration("receipt-timeout"),
		Backfill:                    c.Int64("backfill-priority"),
		BlocksCap:                   c.Int("blocks-ch-capacity"),
		MaxFailures:                 c.Int("max-failures"),
		KafkaBrokers:                c.String("kafka-brokers"),
		KafkaTopic:                  c.String("kafka-topic"),
		KafkaEnableLogs:             c.Bool("kafka-enable-logs"),
		KafkaClientID:               c.String("kafka-client-id"),
		KafkaTopicNumPartitions:     c.Int("kafka-topic-num-partitions"),
		KafkaTopicReplicationFactor: c.Int("kafka-topic-replication-factor"),
		KafkaSASL: kafka.SASLConfig{
			Username:         c.String("kafka-sasl-username"),
			Password:         c.String("kafka-sasl-password"),
			Mechanism:        c.String("kafka-sasl-mechanism"),
			SecurityProtocol: c.String("kafka-security-protocol"),
		},
		ClickHouse:          chCfg,
		CheckpointTableName: c.String("checkpoint-table-name"),
		CheckpointInterval:  c.Duration("checkpoint-interval"),
		GapWatchdogInterval: c.Duration("gap-watchdog-interval"),
		GapWatchdogMaxGap:   c.Uint64("gap-watchdog-max-gap"),
		MetricsHost:         c.String("metrics-host"),
		MetricsPort:         c.Int("metrics-port"),
		Environment:         c.String("environment"),
		Region:              c.String("region"),
		CloudProvider:       c.String("cloud-provider"),
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
		Cluster:              c.String("clickhouse-cluster"),
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
