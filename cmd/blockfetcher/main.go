package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/rpc"
	"github.com/prometheus/client_golang/prometheus"
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
						Name:     "rpc-url",
						Aliases:  []string{"r"},
						Usage:    "The websocket RPC URL to fetch blocks from",
						EnvVars:  []string{"RPC_URL"},
						Required: true,
					},
					&cli.Uint64Flag{
						Name:     "start-height",
						Aliases:  []string{"s"},
						Usage:    "The start height to fetch blocks from",
						EnvVars:  []string{"START_HEIGHT"},
						Required: true,
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
	rpcURL := c.String("rpc-url")
	start := c.Uint64("start-height")
	end := c.Uint64("end-height")
	concurrency := c.Uint64("concurrency")
	backfill := c.Uint64("backfill-priority")
	blocksCap := c.Int("blocks-ch-capacity")
	maxFailures := c.Int("max-failures")
	metricsHost := c.String("metrics-host")
	metricsPort := c.Int("metrics-port")
	metricsAddr := fmt.Sprintf("%s:%d", metricsHost, metricsPort)

	sugar, err := utils.NewSugaredLogger(verbose)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer sugar.Desugar().Sync() //nolint:errcheck // best-effort flush; ignore sync errors
	sugar.Infow("config",
		"verbose", verbose,
		"rpcURL", rpcURL,
		"start", start,
		"end", end,
		"concurrency", concurrency,
		"backfill", backfill,
		"blocksCap", blocksCap,
		"maxFailures", maxFailures,
		"metricsHost", metricsHost,
		"metricsPort", metricsPort,
	)

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

	w, err := worker.NewCorethWorker(ctx, rpcURL, m)
	if err != nil {
		return fmt.Errorf("failed to create worker: %w", err)
	}

	if fetchLatestHeight {
		end, err = customethclient.New(client).BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("failed to get latest block height: %w", err)
		}
		sugar.Infof("latest block height: %d", end)
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
