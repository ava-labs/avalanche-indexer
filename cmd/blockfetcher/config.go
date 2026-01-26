package main

import (
	"fmt"
	"time"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/urfave/cli/v2"
)

// Config holds all configuration for the blockfetcher application
type Config struct {
	// Application settings
	Verbose bool

	// Blockchain settings
	EVMChainID uint64
	BCID       string
	RPCURL     string
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
	return &confluentKafka.ConfigMap{
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
	}
}

// buildConfig builds a Config from CLI context flags
func buildConfig(c *cli.Context) *Config {
	return &Config{
		Verbose:                     c.Bool("verbose"),
		EVMChainID:                  c.Uint64("evm-chain-id"),
		BCID:                        c.String("bc-id"),
		RPCURL:                      c.String("rpc-url"),
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
		CheckpointTableName:         c.String("checkpoint-table-name"),
		CheckpointInterval:          c.Duration("checkpoint-interval"),
		GapWatchdogInterval:         c.Duration("gap-watchdog-interval"),
		GapWatchdogMaxGap:           c.Uint64("gap-watchdog-max-gap"),
		MetricsHost:                 c.String("metrics-host"),
		MetricsPort:                 c.Int("metrics-port"),
		Environment:                 c.String("environment"),
		Region:                      c.String("region"),
		CloudProvider:               c.String("cloud-provider"),
	}
}
