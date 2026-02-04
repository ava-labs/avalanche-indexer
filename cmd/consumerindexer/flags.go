package main

import (
	"time"

	"github.com/urfave/cli/v2"
)

// runFlags returns all CLI runFlags for the consumerindexer run command
func runFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Enable verbose logging",
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
			Value:   "",
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
			Value:   false,
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
			Usage:   "Kafka dlq producer flush timeout when closing",
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
		&cli.IntFlag{
			Name:    "kafka-topic-num-partitions",
			Usage:   "The number of partitions to use for the Kafka topic (must be greater than 0)",
			EnvVars: []string{"KAFKA_TOPIC_NUM_PARTITIONS"},
			Value:   1,
		},
		&cli.IntFlag{
			Name:    "kafka-dlq-topic-num-partitions",
			Usage:   "The number of partitions to use for the Kafka DLQ topic (must be greater than 0)",
			EnvVars: []string{"KAFKA_DLQ_TOPIC_NUM_PARTITIONS"},
			Value:   1,
		},
		&cli.IntFlag{
			Name:    "kafka-topic-replication-factor",
			Usage:   "The replication factor to use for the Kafka topic (must be greater than 0)",
			EnvVars: []string{"KAFKA_TOPIC_REPLICATION_FACTOR"},
			Value:   1,
		},
		&cli.IntFlag{
			Name:    "kafka-dlq-topic-replication-factor",
			Usage:   "The replication factor to use for the Kafka DLQ topic (must be greater than 0)",
			EnvVars: []string{"KAFKA_DLQ_TOPIC_REPLICATION_FACTOR"},
			Value:   1,
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
		&cli.BoolFlag{
			Name:    "publish-to-dlq",
			Usage:   "Publish failed messages to DLQ",
			EnvVars: []string{"KAFKA_PUBLISH_TO_DLQ"},
			Value:   false,
		},
		// ClickHouse configuration flags
		&cli.StringSliceFlag{
			Name:    "clickhouse-hosts",
			Usage:   "ClickHouse server hosts (comma-separated)",
			EnvVars: []string{"CLICKHOUSE_HOSTS"},
			Value:   cli.NewStringSlice("localhost:9000"),
		},
		&cli.StringFlag{
			Name:    "clickhouse-cluster",
			Usage:   "ClickHouse cluster name",
			EnvVars: []string{"CLICKHOUSE_CLUSTER"},
			Value:   "default",
		},
		&cli.StringFlag{
			Name:    "clickhouse-database",
			Usage:   "ClickHouse database name",
			EnvVars: []string{"CLICKHOUSE_DATABASE"},
			Value:   "default",
		},
		&cli.StringFlag{
			Name:    "clickhouse-username",
			Usage:   "ClickHouse username",
			EnvVars: []string{"CLICKHOUSE_USERNAME"},
			Value:   "default",
		},
		&cli.StringFlag{
			Name:    "clickhouse-password",
			Usage:   "ClickHouse password",
			EnvVars: []string{"CLICKHOUSE_PASSWORD"},
			Value:   "",
		},
		&cli.BoolFlag{
			Name:    "clickhouse-debug",
			Usage:   "Enable ClickHouse debug logging",
			EnvVars: []string{"CLICKHOUSE_DEBUG"},
		},
		&cli.BoolFlag{
			Name:    "clickhouse-insecure-skip-verify",
			Usage:   "Skip TLS certificate verification for ClickHouse",
			EnvVars: []string{"CLICKHOUSE_INSECURE_SKIP_VERIFY"},
			Value:   true,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-execution-time",
			Usage:   "ClickHouse max execution time in seconds",
			EnvVars: []string{"CLICKHOUSE_MAX_EXECUTION_TIME"},
			Value:   60,
		},
		&cli.IntFlag{
			Name:    "clickhouse-dial-timeout",
			Usage:   "ClickHouse dial timeout in seconds",
			EnvVars: []string{"CLICKHOUSE_DIAL_TIMEOUT"},
			Value:   30,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-open-conns",
			Usage:   "ClickHouse maximum open connections",
			EnvVars: []string{"CLICKHOUSE_MAX_OPEN_CONNS"},
			Value:   5,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-idle-conns",
			Usage:   "ClickHouse maximum idle connections",
			EnvVars: []string{"CLICKHOUSE_MAX_IDLE_CONNS"},
			Value:   5,
		},
		&cli.IntFlag{
			Name:    "clickhouse-conn-max-lifetime",
			Usage:   "ClickHouse connection max lifetime in minutes",
			EnvVars: []string{"CLICKHOUSE_CONN_MAX_LIFETIME"},
			Value:   10,
		},
		&cli.IntFlag{
			Name:    "clickhouse-block-buffer-size",
			Usage:   "ClickHouse block buffer size",
			EnvVars: []string{"CLICKHOUSE_BLOCK_BUFFER_SIZE"},
			Value:   10,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-block-size",
			Usage:   "ClickHouse max block size (recommended maximum number of rows in a single block)",
			EnvVars: []string{"CLICKHOUSE_MAX_BLOCK_SIZE"},
			Value:   1000,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-compression-buffer",
			Usage:   "ClickHouse max compression buffer in bytes",
			EnvVars: []string{"CLICKHOUSE_MAX_COMPRESSION_BUFFER"},
			Value:   10240,
		},
		&cli.StringFlag{
			Name:    "clickhouse-client-name",
			Usage:   "ClickHouse client name for ClientInfo",
			EnvVars: []string{"CLICKHOUSE_CLIENT_NAME"},
			Value:   "ac-client-name",
		},
		&cli.StringFlag{
			Name:    "clickhouse-client-version",
			Usage:   "ClickHouse client version for ClientInfo",
			EnvVars: []string{"CLICKHOUSE_CLIENT_VERSION"},
			Value:   "1.0",
		},
		&cli.BoolFlag{
			Name:    "clickhouse-use-http",
			Usage:   "Use HTTP protocol instead of native protocol",
			EnvVars: []string{"CLICKHOUSE_USE_HTTP"},
			Value:   false,
		},
		&cli.StringFlag{
			Name:    "raw-blocks-table-name",
			Usage:   "ClickHouse table name for raw blocks",
			EnvVars: []string{"CLICKHOUSE_RAW_BLOCKS_TABLE_NAME"},
			Value:   "raw_blocks",
		},
		&cli.StringFlag{
			Name:    "raw-transactions-table-name",
			Usage:   "ClickHouse table name for raw transactions",
			EnvVars: []string{"CLICKHOUSE_RAW_TRANSACTIONS_TABLE_NAME"},
			Value:   "raw_transactions",
		},
		&cli.StringFlag{
			Name:    "raw-logs-table-name",
			Usage:   "ClickHouse table name for raw logs",
			EnvVars: []string{"CLICKHOUSE_RAW_LOGS_TABLE_NAME"},
			Value:   "raw_logs",
		},
		// Metrics configuration flags
		&cli.StringFlag{
			Name:    "metrics-host",
			Usage:   "Host for Prometheus metrics server (empty for all interfaces)",
			EnvVars: []string{"METRICS_HOST"},
			Value:   "",
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
			Usage:   "EVM chain ID for metrics labels (e.g., 43114 for C-Chain mainnet)",
			EnvVars: []string{"CHAIN_ID"},
			Value:   0,
		},
		&cli.StringFlag{
			Name:    "environment",
			Aliases: []string{"E"},
			Usage:   "Deployment environment for metrics labels (e.g., 'production', 'staging')",
			EnvVars: []string{"ENVIRONMENT"},
			Value:   "",
		},
		&cli.StringFlag{
			Name:    "region",
			Aliases: []string{"R"},
			Usage:   "Cloud region for metrics labels (e.g., 'us-east-1')",
			EnvVars: []string{"REGION"},
			Value:   "",
		},
		&cli.StringFlag{
			Name:    "cloud-provider",
			Aliases: []string{"P"},
			Usage:   "Cloud provider for metrics labels (e.g., 'aws', 'oci', 'gcp')",
			EnvVars: []string{"CLOUD_PROVIDER"},
			Value:   "",
		},
	}
}

// removeFlags returns all CLI removeFlags for the consumerindexer remove command
func removeFlags() []cli.Flag {
	return []cli.Flag{
		&cli.Uint64Flag{
			Name:     "evm-chain-id",
			Aliases:  []string{"C"},
			Usage:    "The EVM chain ID of the blockchain resources being removed",
			EnvVars:  []string{"EVM_CHAIN_ID"},
			Required: true,
		},
		&cli.StringSliceFlag{
			Name:    "clickhouse-hosts",
			Usage:   "ClickHouse server hosts (comma-separated)",
			EnvVars: []string{"CLICKHOUSE_HOSTS"},
			Value:   cli.NewStringSlice("localhost:9000"),
		},
		&cli.StringFlag{
			Name:    "clickhouse-cluster",
			Usage:   "ClickHouse cluster name",
			EnvVars: []string{"CLICKHOUSE_CLUSTER"},
			Value:   "default",
		},
		&cli.StringFlag{
			Name:    "clickhouse-database",
			Usage:   "ClickHouse database name",
			EnvVars: []string{"CLICKHOUSE_DATABASE"},
			Value:   "default",
		},
		&cli.StringFlag{
			Name:    "clickhouse-username",
			Usage:   "ClickHouse username",
			EnvVars: []string{"CLICKHOUSE_USERNAME"},
			Value:   "default",
		},
		&cli.StringFlag{
			Name:    "clickhouse-password",
			Usage:   "ClickHouse password",
			EnvVars: []string{"CLICKHOUSE_PASSWORD"},
			Value:   "",
		},
		&cli.BoolFlag{
			Name:    "clickhouse-debug",
			Usage:   "Enable ClickHouse debug logging",
			EnvVars: []string{"CLICKHOUSE_DEBUG"},
		},
		&cli.BoolFlag{
			Name:    "clickhouse-insecure-skip-verify",
			Usage:   "Skip TLS certificate verification for ClickHouse",
			EnvVars: []string{"CLICKHOUSE_INSECURE_SKIP_VERIFY"},
			Value:   true,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-execution-time",
			Usage:   "ClickHouse max execution time in seconds",
			EnvVars: []string{"CLICKHOUSE_MAX_EXECUTION_TIME"},
			Value:   60,
		},
		&cli.IntFlag{
			Name:    "clickhouse-dial-timeout",
			Usage:   "ClickHouse dial timeout in seconds",
			EnvVars: []string{"CLICKHOUSE_DIAL_TIMEOUT"},
			Value:   30,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-open-conns",
			Usage:   "ClickHouse maximum open connections",
			EnvVars: []string{"CLICKHOUSE_MAX_OPEN_CONNS"},
			Value:   5,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-idle-conns",
			Usage:   "ClickHouse maximum idle connections",
			EnvVars: []string{"CLICKHOUSE_MAX_IDLE_CONNS"},
			Value:   5,
		},
		&cli.IntFlag{
			Name:    "clickhouse-conn-max-lifetime",
			Usage:   "ClickHouse connection max lifetime in minutes",
			EnvVars: []string{"CLICKHOUSE_CONN_MAX_LIFETIME"},
			Value:   10,
		},
		&cli.IntFlag{
			Name:    "clickhouse-block-buffer-size",
			Usage:   "ClickHouse block buffer size",
			EnvVars: []string{"CLICKHOUSE_BLOCK_BUFFER_SIZE"},
			Value:   10,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-block-size",
			Usage:   "ClickHouse max block size (recommended maximum number of rows in a single block)",
			EnvVars: []string{"CLICKHOUSE_MAX_BLOCK_SIZE"},
			Value:   1000,
		},
		&cli.IntFlag{
			Name:    "clickhouse-max-compression-buffer",
			Usage:   "ClickHouse max compression buffer in bytes",
			EnvVars: []string{"CLICKHOUSE_MAX_COMPRESSION_BUFFER"},
			Value:   10240,
		},
		&cli.StringFlag{
			Name:    "clickhouse-client-name",
			Usage:   "ClickHouse client name for ClientInfo",
			EnvVars: []string{"CLICKHOUSE_CLIENT_NAME"},
			Value:   "ac-client-name",
		},
		&cli.StringFlag{
			Name:    "clickhouse-client-version",
			Usage:   "ClickHouse client version for ClientInfo",
			EnvVars: []string{"CLICKHOUSE_CLIENT_VERSION"},
			Value:   "1.0",
		},
		&cli.BoolFlag{
			Name:    "clickhouse-use-http",
			Usage:   "Use HTTP protocol instead of native protocol",
			EnvVars: []string{"CLICKHOUSE_USE_HTTP"},
			Value:   false,
		},
		&cli.StringFlag{
			Name:    "raw-blocks-table-name",
			Usage:   "ClickHouse table name for raw blocks",
			EnvVars: []string{"CLICKHOUSE_RAW_BLOCKS_TABLE_NAME"},
			Value:   "raw_blocks",
		},
		&cli.StringFlag{
			Name:    "raw-transactions-table-name",
			Usage:   "ClickHouse table name for raw transactions",
			EnvVars: []string{"CLICKHOUSE_RAW_TRANSACTIONS_TABLE_NAME"},
			Value:   "raw_transactions",
		},
		&cli.StringFlag{
			Name:    "raw-logs-table-name",
			Usage:   "ClickHouse table name for raw logs",
			EnvVars: []string{"CLICKHOUSE_RAW_LOGS_TABLE_NAME"},
			Value:   "raw_logs",
		},
	}
}
