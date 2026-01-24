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
)

const flushTimeoutOnClose = 15 * time.Second

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
	environment := c.String("environment")
	region := c.String("region")
	cloudProvider := c.String("cloud-provider")
	kafkaBrokers := c.String("kafka-brokers")
	kafkaTopic := c.String("kafka-topic")
	kafkaEnableLogs := c.Bool("kafka-enable-logs")
	kafkaClientID := c.String("kafka-client-id")
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
		"environment", environment,
		"region", region,
		"cloudProvider", cloudProvider,
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

	// Initialize Prometheus metrics with labels for multi-instance filtering
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   environment,
		Region:        region,
		CloudProvider: cloudProvider,
	})
	if err != nil {
		return fmt.Errorf("failed to create metrics: %w", err)
	}

	// Start metrics server
	metricsServer := metrics.NewServer(metricsAddr, registry)
	metricsErrCh := metricsServer.Start()
	if metricsHost == "" {
		sugar.Infof("metrics server listening on http://0.0.0.0:%d/metrics", metricsPort)
	} else {
		sugar.Infof("metrics server listening on http://%s/metrics", metricsAddr)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := rpc.DialContext(ctx, rpcURL)
	if err != nil {
		return fmt.Errorf("failed to dial rpc: %w", err)
	}
	defer client.Close()

	// Build Kafka producer configuration
	kafkaConfig := buildKafkaProducerConfig(kafkaBrokers, kafkaClientID, kafkaEnableLogs)

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
