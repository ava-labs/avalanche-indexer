package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"

	"github.com/ava-labs/avalanche-indexer/pkg/checkpointer"
	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/checkpoint"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	corethClient "github.com/ava-labs/coreth/plugin/evm/customethclient"
	subnetClient "github.com/ava-labs/subnet-evm/ethclient"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const flushTimeoutOnClose = 15 * time.Second

func run(c *cli.Context) error {
	// Build configuration from CLI flags
	cfg, err := buildConfig(c)
	if err != nil {
		return fmt.Errorf("failed to build config: %w", err)
	}

	sugar, err := utils.NewSugaredLogger(cfg.Verbose)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer sugar.Desugar().Sync() //nolint:errcheck // best-effort flush; ignore sync errors

	sugar.Infow("config",
		"verbose", cfg.Verbose,
		"evmChainID", cfg.EVMChainID,
		"bcID", cfg.BCID,
		"rpcURL", cfg.RPCURL,
		"clientType", cfg.ClientType,
		"start", cfg.Start,
		"end", cfg.End,
		"concurrency", cfg.Concurrency,
		"receiptTimeout", cfg.ReceiptTimeout,
		"backfill", cfg.Backfill,
		"blocksCap", cfg.BlocksCap,
		"maxFailures", cfg.MaxFailures,
		"metricsHost", cfg.MetricsHost,
		"metricsPort", cfg.MetricsPort,
		"environment", cfg.Environment,
		"region", cfg.Region,
		"cloudProvider", cfg.CloudProvider,
		"checkpointTableName", cfg.CheckpointTableName,
		"checkpointInterval", cfg.CheckpointInterval,
		"clickhouseCluster", cfg.ClickHouse.Cluster,
		"clickhouseDatabase", cfg.ClickHouse.Database,
		"clickhouseTableName", cfg.CheckpointTableName,
	)

	var fetchStartHeight bool
	start := cfg.Start
	if start == 0 {
		sugar.Infof("start block height: not specified, will fetch from the latest checkpoint")
		fetchStartHeight = true
	} else {
		sugar.Infof("start block height: %d", start)
	}

	var fetchLatestHeight bool
	end := cfg.End
	if end == 0 {
		sugar.Infof("end block height: not specified, will fetch until the latest block")
		fetchLatestHeight = true
	} else {
		sugar.Infof("end block height: %d", end)
	}

	// Initialize Prometheus metrics with labels for multi-instance filtering
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    cfg.EVMChainID,
		Environment:   cfg.Environment,
		Region:        cfg.Region,
		CloudProvider: cfg.CloudProvider,
	})
	if err != nil {
		return fmt.Errorf("failed to create metrics: %w", err)
	}

	// Start metrics server
	metricsServer := metrics.NewServer(cfg.MetricsAddr(), registry)
	metricsErrCh := metricsServer.Start()
	if cfg.MetricsHost == "" {
		sugar.Infof("metrics server listening on http://0.0.0.0:%d/metrics", cfg.MetricsPort)
	} else {
		sugar.Infof("metrics server listening on http://%s/metrics", cfg.MetricsAddr())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Create Kafka admin client to ensure topic exists
	adminConfig := confluentKafka.ConfigMap{"bootstrap.servers": cfg.KafkaBrokers}
	cfg.KafkaSASL.ApplyToConfigMap(&adminConfig)
	kafkaAdminClient, err := confluentKafka.NewAdminClient(&adminConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer kafkaAdminClient.Close()

	err = kafka.EnsureTopic(ctx, kafkaAdminClient, kafka.TopicConfig{
		Name:              cfg.KafkaTopic,
		NumPartitions:     cfg.KafkaTopicNumPartitions,
		ReplicationFactor: cfg.KafkaTopicReplicationFactor,
	}, sugar)
	if err != nil {
		return fmt.Errorf("failed to ensure kafka topic exists: %w", err)
	}

	// Build Kafka producer configuration
	kafkaConfig := cfg.KafkaProducerConfig()

	producer, err := kafka.NewProducer(ctx, kafkaConfig, sugar)
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	defer producer.Close(flushTimeoutOnClose)

	var w worker.Worker
	var sub subscriber.Subscriber
	switch cfg.ClientType {
	case "coreth":
		client, err := corethClient.DialContext(ctx, cfg.RPCURL)
		if err != nil {
			return fmt.Errorf("failed to dial rpc: %w", err)
		}
		defer client.Close()

		// Create worker and subscriber
		w, err = worker.NewCorethWorker(client, producer, cfg.KafkaTopic, cfg.EVMChainID, cfg.BCID, sugar, m, cfg.ReceiptTimeout)
		if err != nil {
			return fmt.Errorf("failed to create worker: %w", err)
		}
		sub = subscriber.NewCoreth(sugar, client)

		if fetchLatestHeight {
			end, err = client.BlockNumber(ctx)
			if err != nil {
				return fmt.Errorf("failed to get latest block height: %w", err)
			}
			sugar.Infof("latest block height: %d", end)
		}
	case "subnet-evm":
		client, err := subnetClient.DialContext(ctx, cfg.RPCURL)
		if err != nil {
			return fmt.Errorf("failed to dial rpc: %w", err)
		}
		defer client.Close()

		// Create subscriber and worker
		w, err = worker.NewSubnetEVMWorker(client, producer, cfg.KafkaTopic, cfg.EVMChainID, cfg.BCID, sugar, m, cfg.ReceiptTimeout)
		if err != nil {
			return fmt.Errorf("failed to create worker: %w", err)
		}
		sub = subscriber.NewSubnetEVM(sugar, client)

		if fetchLatestHeight {
			end, err = client.BlockNumber(ctx)
			if err != nil {
				return fmt.Errorf("failed to get latest block height: %w", err)
			}
			sugar.Infof("latest block height: %d", end)
		}
	default:
		return fmt.Errorf("invalid client type: %s", cfg.ClientType)
	}

	// Initialize ClickHouse client
	chClient, err := clickhouse.New(cfg.ClickHouse, sugar)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	defer chClient.Close()

	sugar.Info("ClickHouse client created successfully")

	checkpointRepo, err := checkpoint.NewRepository(chClient, cfg.ClickHouse.Cluster, cfg.ClickHouse.Database, cfg.CheckpointTableName)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint repository: %w", err)
	}

	// Cast to checkpointer.Checkpointer interface to use Read/Write/Initialize methods
	chkpt := checkpointRepo.(checkpointer.Checkpointer)

	if fetchStartHeight {
		lowestUnprocessed, exists, err := chkpt.Read(ctx, cfg.EVMChainID)
		if err != nil {
			return fmt.Errorf("failed to read checkpoint: %w", err)
		}
		if !exists {
			sugar.Infof("checkpoint not found, will start from block height 0")
			start = 0
		} else {
			start = lowestUnprocessed
			sugar.Infof("start block height: %d", start)
		}
	}

	s, err := slidingwindow.NewState(start, end)
	if err != nil {
		return fmt.Errorf("failed to create state: %w", err)
	}

	mgr, err := slidingwindow.NewManager(sugar, s, w, cfg.Concurrency, cfg.Backfill, cfg.BlocksCap, cfg.MaxFailures, m)
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}

	// Initialize window metrics with starting state
	m.UpdateWindowMetrics(start, end, 0)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return sub.Subscribe(gctx, cfg.BlocksCap, mgr)
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
		checkpointCfg := checkpointer.Config{
			Interval:     cfg.CheckpointInterval,
			WriteTimeout: 1 * time.Second,
			MaxRetries:   3,
			RetryBackoff: 300 * time.Millisecond,
		}
		return checkpointer.Start(gctx, s, chkpt, checkpointCfg, cfg.EVMChainID)
	})

	go slidingwindow.StartGapWatchdog(gctx, sugar, s, cfg.GapWatchdogInterval, cfg.GapWatchdogMaxGap)

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
