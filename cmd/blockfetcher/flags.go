package main

import (
	"time"

	"github.com/urfave/cli/v2"
)

// runFlags returns all CLI runFlags for the blockfetcher run command
func runFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Enable verbose logging",
			EnvVars: []string{"VERBOSE"},
			Value:   false,
		},
		&cli.StringFlag{
			Name:     "evm-chain-id",
			Aliases:  []string{"C"},
			Usage:    "The EVM chain ID of the blockchain being ingested",
			EnvVars:  []string{"EVM_CHAIN_ID"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "bc-id",
			Usage:    "The blockchain ID of the blockchain being ingested",
			EnvVars:  []string{"BLOCKCHAIN_ID"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "rpc-url",
			Aliases:  []string{"r"},
			Usage:    "The websocket RPC URL to fetch blocks from",
			EnvVars:  []string{"RPC_URL"},
			Required: true,
		},
		&cli.StringFlag{
			Name:    "client-type",
			Aliases: []string{"ct"},
			Usage:   "The type of client to use to fetch blocks from (coreth or subnet-evm)",
			EnvVars: []string{"CLIENT_TYPE"},
			Value:   "coreth",
		},
		&cli.Uint64Flag{
			Name:    "start-height",
			Aliases: []string{"s"},
			Usage:   "The start height to fetch blocks from",
			EnvVars: []string{"START_HEIGHT"},
		},
		&cli.Uint64Flag{
			Name:    "end-height",
			Aliases: []string{"e"},
			Usage:   "The end height to fetch blocks to. If not specified, will fetch the latest block height",
			EnvVars: []string{"END_HEIGHT"},
		},
		&cli.Uint64Flag{
			Name:     "concurrency",
			Aliases:  []string{"c"},
			Usage:    "The number of concurrent workers to use",
			EnvVars:  []string{"CONCURRENCY"},
			Required: true,
		},
		&cli.DurationFlag{
			Name:    "receipt-timeout",
			Aliases: []string{"rt"},
			Usage:   "The timeout for fetching a transaction receipt",
			EnvVars: []string{"RECEIPT_TIMEOUT"},
			Value:   10 * time.Second,
		},
		&cli.Uint64Flag{
			Name:     "backfill-priority",
			Aliases:  []string{"b"},
			Usage:    "The priority of the backfill workers (must be less than concurrency)",
			EnvVars:  []string{"BACKFILL_PRIORITY"},
			Required: true,
		},
		&cli.IntFlag{
			Name:    "blocks-ch-capacity",
			Aliases: []string{"B"},
			Usage:   "The capacity of the eth_subscribe channel",
			EnvVars: []string{"BLOCKS_CH_CAPACITY"},
			Value:   100,
		},
		&cli.IntFlag{
			Name:    "max-failures",
			Aliases: []string{"f"},
			Usage:   "The maximum number of block processing failures before stopping",
			EnvVars: []string{"MAX_FAILURES"},
			Value:   3,
		},
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
		&cli.StringFlag{
			Name:     "kafka-brokers",
			Usage:    "The Kafka brokers to use (comma-separated list)",
			EnvVars:  []string{"KAFKA_BROKERS"},
			Required: true,
			Value:    "localhost:9092",
		},
		&cli.StringFlag{
			Name:     "kafka-topic",
			Aliases:  []string{"t"},
			Usage:    "The Kafka topic to use",
			EnvVars:  []string{"KAFKA_TOPIC"},
			Required: true,
		},
		&cli.BoolFlag{
			Name:    "kafka-enable-logs",
			Aliases: []string{"l"},
			Usage:   "Enable Kafka logs",
			EnvVars: []string{"KAFKA_ENABLE_LOGS"},
			Value:   false,
		},
		&cli.StringFlag{
			Name:    "kafka-client-id",
			Usage:   "The Kafka client ID to use",
			EnvVars: []string{"KAFKA_CLIENT_ID"},
			Value:   "blockfetcher",
		},
		&cli.IntFlag{
			Name:    "kafka-topic-num-partitions",
			Usage:   "The number of partitions to use for the Kafka topic (must be greater than 0)",
			EnvVars: []string{"KAFKA_TOPIC_NUM_PARTITIONS"},
			Value:   1,
		},
		&cli.IntFlag{
			Name:    "kafka-topic-replication-factor",
			Usage:   "The replication factor to use for the Kafka topic (must be greater than 0)",
			EnvVars: []string{"KAFKA_TOPIC_REPLICATION_FACTOR"},
			Value:   1,
		},
		&cli.Int64Flag{
			Name:    "kafka-topic-retention-ms",
			Usage:   "Retention time in milliseconds for the Kafka topic (e.g., 604800000 for 7 days, -1 for infinite)",
			EnvVars: []string{"KAFKA_TOPIC_RETENTION_MS"},
			Value:   604800000, // 7 days
		},
		&cli.StringFlag{
			Name:    "kafka-topic-retention-bytes",
			Usage:   "Retention size in bytes for the Kafka topic (e.g., 161061273600 for 150GB, -1 for infinite)",
			EnvVars: []string{"KAFKA_TOPIC_RETENTION_BYTES"},
			Value:   "161061273600", // 150GB
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
		&cli.StringFlag{
			Name:    "checkpoint-table-name",
			Aliases: []string{"T"},
			Usage:   "The name of the table to write the checkpoint to",
			EnvVars: []string{"CHECKPOINT_TABLE_NAME"},
			Value:   "checkpoints",
		},
		&cli.DurationFlag{
			Name:    "checkpoint-interval",
			Aliases: []string{"i"},
			Usage:   "The interval to write the checkpoint to the repository",
			EnvVars: []string{"CHECKPOINT_INTERVAL"},
			Value:   1 * time.Minute,
		},
		&cli.DurationFlag{
			Name:    "gap-watchdog-interval",
			Aliases: []string{"g"},
			Usage:   "The interval to check the gap between the lowest and highest block heights",
			EnvVars: []string{"GAP_WATCHDOG_INTERVAL"},
			Value:   15 * time.Minute,
		},
		&cli.Uint64Flag{
			Name:    "gap-watchdog-max-gap",
			Aliases: []string{"G"},
			Usage:   "The maximum gap between the lowest and highest block heights before a warning is logged",
			EnvVars: []string{"GAP_WATCHDOG_MAX_GAP"},
			Value:   100,
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
	}
}

func removeFlags() []cli.Flag {
	return []cli.Flag{
		&cli.Uint64Flag{
			Name:     "evm-chain-id",
			Aliases:  []string{"C"},
			Usage:    "The EVM chain ID of the blockchain resources being removed",
			EnvVars:  []string{"EVM_CHAIN_ID"},
			Required: true,
		},
		&cli.StringFlag{
			Name:    "checkpoint-table-name",
			Aliases: []string{"T"},
			Usage:   "The name of the table to write the checkpoint to",
			EnvVars: []string{"CHECKPOINT_TABLE_NAME"},
			Value:   "checkpoints",
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
	}
}
