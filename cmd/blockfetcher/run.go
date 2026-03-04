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
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	corethClient "github.com/ava-labs/coreth/plugin/evm/customethclient"
	corethRpc "github.com/ava-labs/coreth/rpc"
	subnetClient "github.com/ava-labs/subnet-evm/ethclient"
	subnetRpc "github.com/ava-labs/subnet-evm/rpc"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	flushTimeoutOnClose = 15 * time.Second
	blocksMode          = "blocks"
	tracesMode          = "traces"
)

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
		"mode", cfg.Mode,
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
		"checkpointBackend", cfg.CheckpointBackend,
		"checkpointTableName", cfg.CheckpointTableName,
		"checkpointInterval", cfg.CheckpointInterval,
		"clickhouseCluster", cfg.ClickHouse.Cluster,
		"clickhouseDatabase", cfg.ClickHouse.Database,
		"checkpointTableName", cfg.CheckpointTableName,
		"dynamoDBRegion", cfg.DynamoDBRegion,
		"dynamoDBCreateTables", cfg.DynamoDBCreateTable,
		"dynamoDBEndpointURL", cfg.DynamoDBEndpointURL,
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
	adminConfig := ckafka.ConfigMap{"bootstrap.servers": cfg.KafkaBrokers}
	cfg.KafkaSASL.ApplyToConfigMap(&adminConfig)
	kafkaAdminClient, err := ckafka.NewAdminClient(&adminConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer kafkaAdminClient.Close()

	topicConfig := kafka.TopicConfig{
		Name:              cfg.KafkaTopic,
		NumPartitions:     cfg.KafkaTopicNumPartitions,
		ReplicationFactor: cfg.KafkaTopicReplicationFactor,
		Config:            make(map[string]string),
	}

	if cfg.KafkaTopicRetentionMs != "" {
		topicConfig.Config["retention.ms"] = cfg.KafkaTopicRetentionMs
	}
	if cfg.KafkaTopicRetentionBytes != "" {
		topicConfig.Config["retention.bytes"] = cfg.KafkaTopicRetentionBytes
	}
	if cfg.KafkaTopicMessageMaxBytes != "" {
		topicConfig.Config["max.message.bytes"] = cfg.KafkaTopicMessageMaxBytes
	}

	err = kafka.EnsureTopic(ctx, kafkaAdminClient, topicConfig, sugar)
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

	switch cfg.Mode {
	case tracesMode:
		switch cfg.ClientType {
		case "coreth":
			client, err := corethRpc.DialContext(ctx, cfg.RPCURL)
			if err != nil {
				return fmt.Errorf("failed to dial rpc: %w", err)
			}
			defer client.Close()

			w, err = worker.NewCorethTracesWorker(client, producer, cfg.KafkaTopic, cfg.EVMChainID, cfg.BCID, sugar, m, cfg.TraceTimeout)
			if err != nil {
				return fmt.Errorf("failed to create traces worker: %w", err)
			}
			cclient := corethClient.New(client)
			sub = subscriber.NewCoreth(sugar, cclient)

			if fetchLatestHeight {
				end, err = cclient.BlockNumber(ctx)
				if err != nil {
					return fmt.Errorf("failed to get latest block height: %w", err)
				}
				sugar.Infof("latest block height: %d", end)
			}
		case "subnet-evm":
			client, err := subnetRpc.DialContext(ctx, cfg.RPCURL)
			if err != nil {
				return fmt.Errorf("failed to dial rpc: %w", err)
			}
			defer client.Close()

			w, err = worker.NewSubnetEVMTracesWorker(client, producer, cfg.KafkaTopic, cfg.EVMChainID, cfg.BCID, sugar, m, cfg.TraceTimeout)
			if err != nil {
				return fmt.Errorf("failed to create traces worker: %w", err)
			}
			sclient := subnetClient.NewClient(client)
			sub = subscriber.NewSubnetEVM(sugar, sclient)

			if fetchLatestHeight {
				end, err = sclient.BlockNumber(ctx)
				if err != nil {
					return fmt.Errorf("failed to get latest block height: %w", err)
				}
				sugar.Infof("latest block height: %d", end)
			}
		default:
			return fmt.Errorf("invalid client type: %s", cfg.ClientType)
		}
	case blocksMode:
		// blocks mode
		switch cfg.ClientType {
		case "coreth":
			client, err := corethClient.DialContext(ctx, cfg.RPCURL)
			if err != nil {
				return fmt.Errorf("failed to dial rpc: %w", err)
			}
			defer client.Close()

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
	default:
		return fmt.Errorf("invalid mode: %s", cfg.Mode)
	}

	chkpt, _, cleanupCheckpointStore, err := newCheckpointStore(ctx, cfg, sugar)
	if err != nil {
		return err
	}
	defer cleanupCheckpointStore()

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
			sugar.Infof("checkpoint found, lowest unprocessed block: %d", start)
		}
	}

	// When backfill is complete, lowest can advance past highest (lowest = highest + 1).
	// On restart, if the persisted lowest > current chain height, it means all blocks
	// have been processed. Reset start to end so we can create a valid initial state.
	if start > end {
		sugar.Infof("backfill was complete (lowest=%d > highest=%d), resetting start to end", start, end)
		start = end
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
