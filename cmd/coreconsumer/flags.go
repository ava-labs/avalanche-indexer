package main

import (
	"time"

	"github.com/urfave/cli/v2"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
)

// runFlags returns all CLI flags for the coreconsumer run command.
func runFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Enable verbose logging",
			EnvVars: []string{"VERBOSE"},
			Value:   false,
		},
		// Kafka configuration flags
		&cli.StringFlag{
			Name:     "bootstrap-servers",
			Aliases:  []string{"b"},
			Usage:    "Kafka bootstrap servers (comma-separated)",
			EnvVars:  []string{"KAFKA_BOOTSTRAP_SERVERS"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "group-id",
			Aliases:  []string{"g"},
			Usage:    "Kafka consumer group ID",
			EnvVars:  []string{"KAFKA_GROUP_ID"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "topic",
			Aliases:  []string{"t"},
			Usage:    "Kafka topic to consume from",
			EnvVars:  []string{"KAFKA_TOPIC"},
			Required: true,
		},
		&cli.StringFlag{
			Name:    "dlq-topic",
			Usage:   "Dead letter queue topic for failed messages",
			EnvVars: []string{"KAFKA_DLQ_TOPIC"},
		},
		&cli.StringFlag{
			Name:    "auto-offset-reset",
			Aliases: []string{"o"},
			Usage:   "Kafka auto offset reset policy (earliest, latest, none)",
			EnvVars: []string{"KAFKA_AUTO_OFFSET_RESET"},
			Value:   "earliest",
		},
		&cli.Int64Flag{
			Name:    "concurrency",
			Usage:   "Concurrent message processors",
			EnvVars: []string{"KAFKA_CONCURRENCY"},
			Value:   10,
		},
		&cli.DurationFlag{
			Name:    "offset-commit-interval",
			Usage:   "Interval for committing offsets",
			EnvVars: []string{"KAFKA_OFFSET_COMMIT_INTERVAL"},
			Value:   10 * time.Second,
		},
		&cli.BoolFlag{
			Name:    "enable-kafka-logs",
			Usage:   "Enable librdkafka client logs",
			EnvVars: []string{"KAFKA_ENABLE_LOGS"},
		},
		&cli.DurationFlag{
			Name:    "session-timeout",
			Usage:   "Kafka consumer session timeout",
			EnvVars: []string{"KAFKA_SESSION_TIMEOUT"},
			Value:   240 * time.Second,
		},
		&cli.DurationFlag{
			Name:    "max-poll-interval",
			Usage:   "Kafka consumer max poll interval",
			EnvVars: []string{"KAFKA_MAX_POLL_INTERVAL"},
			Value:   3400 * time.Second,
		},
		&cli.DurationFlag{
			Name:    "flush-timeout",
			Usage:   "Kafka DLQ producer flush timeout when closing",
			EnvVars: []string{"KAFKA_FLUSH_TIMEOUT"},
			Value:   15 * time.Second,
		},
		&cli.DurationFlag{
			Name:    "goroutine-wait-timeout",
			Usage:   "Timeout for waiting in-flight goroutines on shutdown",
			EnvVars: []string{"KAFKA_GOROUTINE_WAIT_TIMEOUT"},
			Value:   30 * time.Second,
		},
		&cli.DurationFlag{
			Name:    "poll-interval",
			Usage:   "Poll interval for Kafka consumer",
			EnvVars: []string{"KAFKA_POLL_INTERVAL"},
			Value:   100 * time.Millisecond,
		},
		&cli.BoolFlag{
			Name:    "publish-to-dlq",
			Usage:   "Publish failed messages to DLQ",
			EnvVars: []string{"KAFKA_PUBLISH_TO_DLQ"},
		},
		&cli.IntFlag{
			Name:    "kafka-topic-num-partitions",
			Usage:   "Number of partitions for the Kafka topic",
			EnvVars: []string{"KAFKA_TOPIC_NUM_PARTITIONS"},
			Value:   1,
		},
		&cli.IntFlag{
			Name:    "kafka-topic-replication-factor",
			Usage:   "Replication factor for the Kafka topic",
			EnvVars: []string{"KAFKA_TOPIC_REPLICATION_FACTOR"},
			Value:   1,
		},
		&cli.StringFlag{
			Name:    "kafka-topic-retention-ms",
			Usage:   "Retention time in ms for the Kafka topic (-1 for infinite)",
			EnvVars: []string{"KAFKA_TOPIC_RETENTION_MS"},
			Value:   "604800000",
		},
		&cli.StringFlag{
			Name:    "kafka-topic-retention-bytes",
			Usage:   "Retention size in bytes for the Kafka topic (-1 for infinite)",
			EnvVars: []string{"KAFKA_TOPIC_RETENTION_BYTES"},
			Value:   "161061273600",
		},
		&cli.IntFlag{
			Name:    "kafka-dlq-topic-num-partitions",
			Usage:   "Number of partitions for the DLQ topic",
			EnvVars: []string{"KAFKA_DLQ_TOPIC_NUM_PARTITIONS"},
			Value:   1,
		},
		&cli.IntFlag{
			Name:    "kafka-dlq-topic-replication-factor",
			Usage:   "Replication factor for the DLQ topic",
			EnvVars: []string{"KAFKA_DLQ_TOPIC_REPLICATION_FACTOR"},
			Value:   1,
		},
		&cli.StringFlag{
			Name:    "kafka-dlq-topic-retention-ms",
			Usage:   "Retention time in ms for the DLQ topic",
			EnvVars: []string{"KAFKA_DLQ_TOPIC_RETENTION_MS"},
			Value:   "604800000",
		},
		&cli.StringFlag{
			Name:    "kafka-dlq-topic-retention-bytes",
			Usage:   "Retention size in bytes for the DLQ topic",
			EnvVars: []string{"KAFKA_DLQ_TOPIC_RETENTION_BYTES"},
			Value:   "161061273600",
		},
		&cli.StringFlag{
			Name:    "kafka-topic-message-max-bytes",
			Usage:   "Maximum message size in bytes for Kafka topics",
			EnvVars: []string{"KAFKA_TOPIC_MESSAGE_MAX_BYTES"},
		},
		&cli.StringFlag{
			Name:    "kafka-sasl-username",
			Usage:   "SASL username for Kafka authentication",
			EnvVars: []string{"KAFKA_SASL_USERNAME"},
		},
		&cli.StringFlag{
			Name:    "kafka-sasl-password",
			Usage:   "SASL password for Kafka authentication",
			EnvVars: []string{"KAFKA_SASL_PASSWORD"},
		},
		&cli.StringFlag{
			Name:    "kafka-sasl-mechanism",
			Usage:   "SASL mechanism (SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN)",
			EnvVars: []string{"KAFKA_SASL_MECHANISM"},
			Value:   "SCRAM-SHA-512",
		},
		&cli.StringFlag{
			Name:    "kafka-security-protocol",
			Usage:   "Security protocol (SASL_SSL or SASL_PLAINTEXT)",
			EnvVars: []string{"KAFKA_SECURITY_PROTOCOL"},
			Value:   "SASL_SSL",
		},
		// Consumer retry policy
		&cli.IntFlag{
			Name:    "consumer-retry-max-retries",
			Usage:   "Max retry attempts for failed message processing (-1 = infinite, 0 = disabled)",
			EnvVars: []string{"CONSUMER_RETRY_MAX_RETRIES"},
			Value:   3,
		},
		&cli.DurationFlag{
			Name:    "consumer-retry-base-delay",
			Usage:   "Initial backoff delay between retries",
			EnvVars: []string{"CONSUMER_RETRY_BASE_DELAY"},
			Value:   kafka.DefaultRetryBaseDelay,
		},
		&cli.DurationFlag{
			Name:    "consumer-retry-max-delay",
			Usage:   "Maximum backoff delay between retries",
			EnvVars: []string{"CONSUMER_RETRY_MAX_DELAY"},
			Value:   kafka.DefaultRetryMaxDelay,
		},
		// DynamoDB configuration flags
		&cli.StringFlag{
			Name:    "dynamodb-region",
			Usage:   "AWS region for DynamoDB",
			EnvVars: []string{"DYNAMODB_REGION"},
			Value:   "us-east-1",
		},
		&cli.StringFlag{
			Name:    "dynamodb-endpoint",
			Usage:   "DynamoDB endpoint override (for LocalStack/local dev)",
			EnvVars: []string{"DYNAMODB_ENDPOINT"},
		},
		&cli.StringFlag{
			Name:    "dynamodb-history-table",
			Usage:   "DynamoDB table name for block/tx history",
			EnvVars: []string{"DYNAMODB_HISTORY_TABLE"},
			Value:   "history",
		},
		&cli.StringFlag{
			Name:    "dynamodb-erc-table",
			Usage:   "DynamoDB table name for ERC token metadata",
			EnvVars: []string{"DYNAMODB_ERC_TABLE"},
			Value:   "erc",
		},
		&cli.StringFlag{
			Name:    "dynamodb-status-table",
			Usage:   "DynamoDB table name for stream commit status",
			EnvVars: []string{"DYNAMODB_STATUS_TABLE"},
			Value:   "status",
		},
		&cli.IntFlag{
			Name:    "dynamodb-max-retries",
			Usage:   "Max retries for DynamoDB batch writes",
			EnvVars: []string{"DYNAMODB_MAX_RETRIES"},
			Value:   10,
		},
		&cli.IntFlag{
			Name:    "dynamodb-max-inflight",
			Usage:   "Max concurrent DynamoDB batch write operations",
			EnvVars: []string{"DYNAMODB_MAX_INFLIGHT"},
			Value:   100,
		},
		// Metrics configuration flags
		&cli.StringFlag{
			Name:    "metrics-host",
			Usage:   "Host for Prometheus metrics server (empty for all interfaces)",
			EnvVars: []string{"METRICS_HOST"},
		},
		&cli.IntFlag{
			Name:    "metrics-port",
			Aliases: []string{"m"},
			Usage:   "Port for Prometheus metrics server",
			EnvVars: []string{"METRICS_PORT"},
			Value:   9090,
		},
		&cli.Uint64Flag{
			Name:    "chain-id",
			Aliases: []string{"C"},
			Usage:   "EVM chain ID for metrics labels",
			EnvVars: []string{"CHAIN_ID"},
		},
		&cli.StringFlag{
			Name:    "environment",
			Aliases: []string{"E"},
			Usage:   "Deployment environment for metrics labels",
			EnvVars: []string{"ENVIRONMENT"},
		},
		&cli.StringFlag{
			Name:    "region",
			Aliases: []string{"R"},
			Usage:   "Cloud region for metrics labels",
			EnvVars: []string{"REGION"},
		},
		&cli.StringFlag{
			Name:    "cloud-provider",
			Aliases: []string{"P"},
			Usage:   "Cloud provider for metrics labels",
			EnvVars: []string{"CLOUD_PROVIDER"},
		},
	}
}

// removeFlags returns CLI flags for the coreconsumer remove command.
func removeFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "dynamodb-region",
			Usage:   "AWS region for DynamoDB",
			EnvVars: []string{"DYNAMODB_REGION"},
			Value:   "us-east-1",
		},
		&cli.StringFlag{
			Name:    "dynamodb-endpoint",
			Usage:   "DynamoDB endpoint override",
			EnvVars: []string{"DYNAMODB_ENDPOINT"},
		},
		&cli.StringFlag{
			Name:     "dynamodb-history-table",
			Usage:    "DynamoDB history table to delete",
			EnvVars:  []string{"DYNAMODB_HISTORY_TABLE"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dynamodb-erc-table",
			Usage:    "DynamoDB ERC table to delete",
			EnvVars:  []string{"DYNAMODB_ERC_TABLE"},
			Required: true,
		},
	}
}
