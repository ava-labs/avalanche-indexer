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
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/snapshot"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/scheduler"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/rpc"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
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
						Name:     "chain-id",
						Aliases:  []string{"C"},
						Usage:    "The chain ID to write the snapshot to",
						EnvVars:  []string{"CHAIN_ID"},
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
					&cli.StringFlag{
						Name:    "snapshot-table-name",
						Aliases: []string{"T"},
						Usage:   "The name of the table to write the snapshot to",
						EnvVars: []string{"SNAPSHOT_TABLE_NAME"},
						Value:   "test_db.snapshots",
					},
					&cli.DurationFlag{
						Name:    "snapshot-interval",
						Aliases: []string{"i"},
						Usage:   "The interval to write the snapshot to the repository",
						EnvVars: []string{"SNAPSHOT_INTERVAL"},
						Value:   1 * time.Minute,
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
	chainID := c.Uint64("chain-id")
	rpcURL := c.String("rpc-url")
	start := c.Uint64("start-height")
	end := c.Uint64("end-height")
	concurrency := c.Uint64("concurrency")
	backfill := c.Uint64("backfill-priority")
	blocksCap := c.Int("blocks-ch-capacity")
	maxFailures := c.Int("max-failures")
	kafkaBrokers := c.String("kafka-brokers")
	kafkaTopic := c.String("kafka-topic")
	kafkaEnableLogs := c.Bool("kafka-enable-logs")
	kafkaClientID := c.String("kafka-client-id")
	snapshotTableName := c.String("snapshot-table-name")
	snapshotInterval := c.Duration("snapshot-interval")
	sugar, err := utils.NewSugaredLogger(verbose)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer sugar.Desugar().Sync() //nolint:errcheck // best-effort flush; ignore sync errors
	sugar.Infow("config",
		"verbose", verbose,
		"chainID", chainID,
		"rpcURL", rpcURL,
		"start", start,
		"end", end,
		"concurrency", concurrency,
		"backfill", backfill,
		"blocksCap", blocksCap,
		"maxFailures", maxFailures,
		"snapshotTableName", snapshotTableName,
		"snapshotInterval", snapshotInterval,
	)

	var fetchStartHeight bool
	if start == 0 {
		sugar.Infof("start block height: not specified, will fetch from the latest snapshot")
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := rpc.DialContext(ctx, rpcURL)
	if err != nil {
		return fmt.Errorf("failed to dial rpc: %w", err)
	}
	defer client.Close()

	// Kafka producer configuration
	kafkaConfig := &confluentKafka.ConfigMap{
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

	w, err := worker.NewCorethWorker(ctx, rpcURL, producer, kafkaTopic, sugar)
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

	repo := snapshot.NewRepository(chClient, snapshotTableName)

	err = repo.CreateTableIfNotExists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check existence or create snapshots table: %w", err)
	}

	if fetchStartHeight {
		snapshot, err := repo.ReadSnapshot(ctx, chainID)
		if err != nil {
			return fmt.Errorf("failed to read snapshot: %w", err)
		}
		if snapshot == nil {
			sugar.Infof("snapshot not found, will start from block height 0")
			start = 0
		} else {
			start = snapshot.Lowest
			sugar.Infof("start block height: %d", start)
		}
	}

	s, err := slidingwindow.NewState(start, end)
	if err != nil {
		return fmt.Errorf("failed to create state: %w", err)
	}

	m, err := slidingwindow.NewManager(sugar, s, w, concurrency, backfill, blocksCap, maxFailures)
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}

	sub := subscriber.NewCoreth(sugar, customethclient.New(client))
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return sub.Subscribe(gctx, blocksCap, m)
	})
	g.Go(func() error {
		return m.Run(gctx)
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
		return scheduler.Start(gctx, s, repo, snapshotInterval, chainID)
	})

	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		sugar.Infow("exiting due to context cancellation")
		return nil
	}
	if err != nil {
		sugar.Errorw("run failed", "error", err)
		return err
	}

	sugar.Info("shutting down")
	return nil
}
