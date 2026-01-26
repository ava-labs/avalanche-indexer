package main

import (
	"time"

	"github.com/urfave/cli/v2"
)

// flags returns all CLI flags for the blockfetcher run command
func flags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Enable verbose logging",
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
		&cli.StringFlag{
			Name:    "checkpoint-table-name",
			Aliases: []string{"T"},
			Usage:   "The name of the table to write the checkpoint to",
			EnvVars: []string{"CHECKPOINT_TABLE_NAME"},
			Value:   "test_db.checkpoints",
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
	}
}
