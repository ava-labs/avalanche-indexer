package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/checkpoint"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/scheduler"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/rpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"

	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const flushTimeoutOnClose = 15 * time.Second

func main() {
	app := &cli.App{
		Name:  "blockfetcher",
		Usage: "Fetch blocks from a given RPC endpoint",
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "Run the block fetcher",
				Flags: []cli.Flag{
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
					&cli.StringFlag{
						Name:    "kafka-enable-logs",
						Aliases: []string{"l"},
						Usage:   "Enable Kafka logs",
						EnvVars: []string{"KAFKA_ENABLE_LOGS"},
						Value:   "false",
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
				},
				Action: run,
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(c *cli.Context) error {
	verbose := c.Bool("verbose")
	evmChainID := c.Uint64("evm-chain-id")
	bcID := c.String("bc-id")
	rpcURL := c.String("rpc-url")
	start := c.Uint64("start-height")
	end := c.Uint64("end-height")
	concurrency := c.Int64("concurrency")
	backfill := c.Int64("backfill-priority")
	blocksCap := c.Int("blocks-ch-capacity")
	maxFailures := c.Int("max-failures")
	metricsHost := c.String("metrics-host")
	metricsPort := c.Int("metrics-port")
	metricsAddr := fmt.Sprintf("%s:%d", metricsHost, metricsPort)
	kafkaBrokers := c.String("kafka-brokers")
	kafkaTopic := c.String("kafka-topic")
	kafkaEnableLogs := c.Bool("kafka-enable-logs")
	kafkaClientID := c.String("kafka-client-id")
	kafkaTopicNumPartitions := c.Int("kafka-topic-num-partitions")
	kafkaTopicReplicationFactor := c.Int("kafka-topic-replication-factor")
	checkpointTableName := c.String("checkpoint-table-name")
	checkpointInterval := c.Duration("checkpoint-interval")
	gapWatchdogInterval := c.Duration("gap-watchdog-interval")
	gapWatchdogMaxGap := c.Uint64("gap-watchdog-max-gap")
	sugar, err := utils.NewSugaredLogger(verbose)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer sugar.Desugar().Sync() //nolint:errcheck // best-effort flush; ignore sync errors
	sugar.Infow("config",
		"verbose", verbose,
		"evmChainID", evmChainID,
		"bcID", bcID,
		"rpcURL", rpcURL,
		"start", start,
		"end", end,
		"concurrency", concurrency,
		"backfill", backfill,
		"blocksCap", blocksCap,
		"maxFailures", maxFailures,
		"metricsHost", metricsHost,
		"metricsPort", metricsPort,
		"checkpointTableName", checkpointTableName,
		"checkpointInterval", checkpointInterval,
	)

	var fetchStartHeight bool
	if start == 0 {
		sugar.Infof("start block height: not specified, will fetch from the latest checkpoint")
		fetchStartHeight = true
	} else {
		sugar.Infof("start block height: %d", start)
	}

	var fetchLatestHeight bool
	if end == 0 {
		sugar.Infof("end block height: not specified, will fetch until the latest block")
		fetchLatestHeight = true
	} else {
		sugar.Infof("end block height: %d", end)
	}

	// Initialize Prometheus metrics
	registry := prometheus.NewRegistry()
	m, err := metrics.New(registry)
	if err != nil {
		return fmt.Errorf("failed to create metrics: %w", err)
	}

	// Start metrics server
	metricsServer := metrics.NewServer(metricsAddr, registry)
	metricsErrCh := metricsServer.Start()
	sugar.Infof("metrics server started at http://localhost%s/metrics", metricsAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := rpc.DialContext(ctx, rpcURL)
	if err != nil {
		return fmt.Errorf("failed to dial rpc: %w", err)
	}
	defer client.Close()

	// Create Kafka admin client to ensure topic exists
	kafkaAdminClient, err := cKafka.NewAdminClient(&cKafka.ConfigMap{"bootstrap.servers": kafkaBrokers})
	if err != nil {
		return fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer kafkaAdminClient.Close()

	err = kafka.CreateTopicWithConfigIfNotExists(ctx, kafkaAdminClient, kafka.TopicConfig{
		Name:              kafkaTopic,
		NumPartitions:     kafkaTopicNumPartitions,
		ReplicationFactor: kafkaTopicReplicationFactor,
	}, sugar)
	if err != nil {
		return fmt.Errorf("failed to ensure kafka topic exists: %w", err)
	}

	// Kafka producer configuration
	kafkaConfig := &cKafka.ConfigMap{
		// Required
		"bootstrap.servers": kafkaBrokers,
		"client.id":         kafkaClientID,

		// Reliability: wait for all replicas to acknowledge
		"acks": "all",

		// Performance tuning
		"linger.ms":        5,     // Batch messages for 5ms
		"batch.size":       16384, // 16KB batch size
		"compression.type": "lz4", // Fast compression

		// Idempotence for exactly-once semantics
		"enable.idempotence": true,

		// Go channel for logs (optional, enable for debugging)
		"go.logs.channel.enable": kafkaEnableLogs,
	}

	producer, err := kafka.NewProducer(ctx, kafkaConfig, sugar)
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	defer producer.Close(flushTimeoutOnClose)

	w, err := worker.NewCorethWorker(ctx, rpcURL, producer, kafkaTopic, evmChainID, bcID, sugar, m)
	if err != nil {
		return fmt.Errorf("failed to create worker: %w", err)
	}

	// Initialize ClickHouse client
	chCfg := clickhouse.Load()
	chClient, err := clickhouse.New(chCfg, sugar)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	defer chClient.Close()

	sugar.Info("ClickHouse client created successfully")

	if fetchLatestHeight {
		end, err = customethclient.New(client).BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("failed to get latest block height: %w", err)
		}
		sugar.Infof("latest block height: %d", end)
	}

	repo := checkpoint.NewRepository(chClient, checkpointTableName)

	err = repo.CreateTableIfNotExists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check existence or create checkpoints table: %w", err)
	}

	if fetchStartHeight {
		checkpoint, err := repo.ReadCheckpoint(ctx, evmChainID)
		if err != nil {
			return fmt.Errorf("failed to read checkpoint: %w", err)
		}
		if checkpoint == nil {
			sugar.Infof("checkpoint not found, will start from block height 0")
			start = 0
		} else {
			start = checkpoint.Lowest
			sugar.Infof("start block height: %d", start)
		}
	}

	s, err := slidingwindow.NewState(start, end)
	if err != nil {
		return fmt.Errorf("failed to create state: %w", err)
	}

	mgr, err := slidingwindow.NewManager(sugar, s, w, concurrency, backfill, blocksCap, maxFailures, m)
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}

	// Initialize window metrics with starting state
	m.UpdateWindowMetrics(start, end, 0)

	sub := subscriber.NewCoreth(sugar, customethclient.New(client))
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return sub.Subscribe(gctx, blocksCap, mgr)
	})
	g.Go(func() error {
		return mgr.Run(gctx)
	})
	g.Go(func() error {
		select {
		case <-gctx.Done():
			return nil
		case err := <-metricsErrCh:
			if err != nil {
				return fmt.Errorf("metrics server failed: %w", err)
			}
			return nil
		}
	})
	g.Go(func() error {
		select {
		case <-gctx.Done():
			return gctx.Err()
		case err := <-producer.Errors():
			return err
		}
	})
	g.Go(func() error {
		return scheduler.Start(gctx, s, repo, checkpointInterval, evmChainID)
	})

	go slidingwindow.StartGapWatchdog(gctx, sugar, s, gapWatchdogInterval, gapWatchdogMaxGap)

	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		sugar.Infow("exiting due to context cancellation")
	} else if err != nil {
		sugar.Errorw("run failed", "error", err)
	}

	// Gracefully shutdown metrics server
	sugar.Info("shutting down metrics server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		sugar.Warnw("metrics server shutdown error", "error", err)
	}

	sugar.Info("shutdown complete")
	return err
}
