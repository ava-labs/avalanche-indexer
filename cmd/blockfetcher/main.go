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
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/rpc"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

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
						Name:    "snapshot-table-name",
						Aliases: []string{"t"},
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

	w, err := worker.NewCorethWorker(ctx, rpcURL)
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
	if fetchStartHeight {
		snapshot, err := repo.ReadSnapshot(ctx, chainID)
		if err != nil {
			return fmt.Errorf("failed to read snapshot: %w", err)
		}
		if snapshot == nil {
			return fmt.Errorf("snapshot not found")
		}
		start = snapshot.Lowest
		sugar.Infof("start block height: %d", start)
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
		return startSnapshotScheduler(gctx, s, repo, snapshotInterval, chainID)
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
