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
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/ava-labs/avalanche-indexer/pkg/checkpointer"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	corethClient "github.com/ava-labs/avalanchego/graft/coreth/ethclient"
	corethCustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	evmRpc "github.com/ava-labs/avalanchego/graft/evm/rpc"
	subnetClient "github.com/ava-labs/avalanchego/graft/subnet-evm/ethclient"
	subnetevmCustomtypes "github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customtypes"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	flushTimeoutOnClose = 15 * time.Second
	blocksMode          = "blocks"
	tracesMode          = "traces"

	clientTypeCoreth    = "coreth"
	clientTypeSubnetEVM = "subnet-evm"

	// initialDialBackoff is the wait before the first redial attempt.
	initialDialBackoff = 1 * time.Second
	// maxDialBackoff caps the exponential backoff between dial attempts.
	maxDialBackoff = 30 * time.Second
	// maxDialRetries caps how many times we retry a failed dial before giving
	// up and letting the process exit (so the orchestrator can restart it).
	maxDialRetries = 100
)

// dialWithRetry retries dial with capped backoff until it succeeds
func dialWithRetry[T any](ctx context.Context, log *zap.SugaredLogger, dial func(context.Context) (T, error)) (T, error) {
	backoff := utils.NewBackoff(initialDialBackoff, maxDialBackoff)
	for retries := 0; ; retries++ {
		client, err := dial(ctx)
		if err == nil {
			return client, nil
		}
		if ctx.Err() != nil {
			var zero T
			return zero, ctx.Err()
		}
		if retries >= maxDialRetries {
			var zero T
			return zero, fmt.Errorf("dial rpc: gave up after %d retries: %w", maxDialRetries, err)
		}

		retryIn := backoff.Next()
		log.Warnw("failed to dial rpc; retrying", "error", err, "retry", retries+1, "retryIn", retryIn.String())

		if err := utils.Sleep(ctx, retryIn); err != nil {
			var zero T
			return zero, err
		}
	}
}

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
		"dynamoDBRegion", cfg.DynamoDB.Region,
		"dynamoDBEndpointURL", cfg.DynamoDB.EndpointURL,
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

	// Register the libevm header/block extras for the configured client. This
	// mutates process-global state in libevm and must happen exactly once, before
	// any block is decoded. Coreth and Subnet-EVM register incompatible payload
	// types, so only the configured client's extras may be registered.
	switch cfg.ClientType {
	case clientTypeCoreth:
		corethCustomtypes.Register()
	case clientTypeSubnetEVM:
		subnetevmCustomtypes.Register()
	default:
		return fmt.Errorf("invalid client type: %s", cfg.ClientType)
	}

	var w worker.Worker
	var sub subscriber.Subscriber

	switch cfg.Mode {
	case tracesMode:
		switch cfg.ClientType {
		case clientTypeCoreth:
			rpc, err := dialWithRetry(ctx, sugar, func(ctx context.Context) (*evmRpc.Client, error) {
				return evmRpc.DialContext(ctx, cfg.RPCURL)
			})
			if err != nil {
				return fmt.Errorf("failed to dial rpc: %w", err)
			}
			defer rpc.Close()
			cclient := corethClient.NewClient(rpc)

			w, err = worker.NewCorethTracesWorker(cclient, rpc, producer, cfg.KafkaTopic, cfg.EVMChainID, cfg.BCID, sugar, m, cfg.TraceTimeout)
			if err != nil {
				return fmt.Errorf("failed to create traces worker: %w", err)
			}

			sub = subscriber.NewCoreth(sugar, cclient)

			if fetchLatestHeight {
				end, err = cclient.BlockNumber(ctx)
				if err != nil {
					return fmt.Errorf("failed to get latest block height: %w", err)
				}
				sugar.Infof("latest block height: %d", end)
			}
		case clientTypeSubnetEVM:
			rpc, err := dialWithRetry(ctx, sugar, func(ctx context.Context) (*evmRpc.Client, error) {
				return evmRpc.DialContext(ctx, cfg.RPCURL)
			})
			if err != nil {
				return fmt.Errorf("failed to dial rpc: %w", err)
			}
			defer rpc.Close()
			sclient := subnetClient.NewClient(rpc)

			w, err = worker.NewSubnetEVMTracesWorker(sclient, rpc, producer, cfg.KafkaTopic, cfg.EVMChainID, cfg.BCID, sugar, m, cfg.TraceTimeout)
			if err != nil {
				return fmt.Errorf("failed to create traces worker: %w", err)
			}
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
		case clientTypeCoreth:
			client, err := dialWithRetry(ctx, sugar, func(ctx context.Context) (*corethClient.Client, error) {
				return corethClient.DialContext(ctx, cfg.RPCURL)
			})
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
		case clientTypeSubnetEVM:
			client, err := dialWithRetry(ctx, sugar, func(ctx context.Context) (subnetClient.Client, error) {
				return subnetClient.DialContext(ctx, cfg.RPCURL)
			})
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

	chkpt, err := newCheckpointStore(ctx, cfg, sugar)
	if err != nil {
		return err
	}
	defer chkpt.Close()

	if fetchStartHeight {
		lowestUnprocessed, exists, err := chkpt.Read(ctx, cfg.EVMChainID, cfg.Mode)
		if err != nil {
			return fmt.Errorf("failed to read checkpoint: %w", err)
		}
		if !exists {
			sugar.Infof("checkpoint not found for mode %s, will start from block height 0", cfg.Mode)
			start = 0
		} else {
			start = lowestUnprocessed
			sugar.Infof("checkpoint found for mode %s, lowest unprocessed block: %d", cfg.Mode, start)
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
		return checkpointer.Start(gctx, s, chkpt, checkpointCfg, cfg.EVMChainID, cfg.Mode)
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
