package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
		"verbose", cfg.Verbose,
		"bootstrapServers", cfg.BootstrapServers,
		"groupID", cfg.GroupID,
		"topic", cfg.Topic,
		"dlqTopic", cfg.DLQTopic,
		"autoOffsetReset", cfg.AutoOffsetReset,
		"maxConcurrency", cfg.Concurrency,
		"offsetCommitInterval", cfg.OffsetCommitInterval,
		"enableKafkaLogs", cfg.EnableKafkaLogs,
		"sessionTimeout", cfg.SessionTimeout,
		"maxPollInterval", cfg.MaxPollInterval,
		"flushTimeout", cfg.FlushTimeout,
		"goroutineWaitTimeout", cfg.GoroutineWaitTimeout,
		"pollInterval", cfg.PollInterval,
		"clickhouseHosts", cfg.ClickHouse.Hosts,
		"clickhouseDatabase", cfg.ClickHouse.Database,
		"clickhouseUsername", cfg.ClickHouse.Username,
		"clickhouseDebug", cfg.ClickHouse.Debug,
		"metricsHost", cfg.MetricsHost,
		"metricsPort", cfg.MetricsPort,
		"chainID", cfg.ChainID,
		"environment", cfg.Environment,
		"region", cfg.Region,
		"cloudProvider", cfg.CloudProvider,
		"rawBlocksTableName", cfg.RawBlocksTableName,
		"rawTransactionsTableName", cfg.RawTransactionsTableName,
		"rawLogsTableName", cfg.RawLogsTableName,
		"publishToDLQ", cfg.PublishToDLQ,
		"kafkaTopicNumPartitions", cfg.KafkaTopicNumPartitions,
		"kafkaTopicReplicationFactor", cfg.KafkaTopicReplicationFactor,
		"kafkaDLQTopicNumPartitions", cfg.KafkaDLQTopicNumPartitions,
		"kafkaDLQTopicReplicationFactor", cfg.KafkaDLQTopicReplicationFactor,
	)

	// Initialize Prometheus metrics with labels for multi-instance filtering
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    cfg.ChainID,
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

	// Initialize ClickHouse client
	chClient, err := clickhouse.New(cfg.ClickHouse, sugar)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	defer chClient.Close()

	sugar.Info("ClickHouse client created successfully")

	// Initialize repositories (tables are created automatically)
	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, cfg.RawBlocksTableName)
	if err != nil {
		return fmt.Errorf("failed to create blocks repository: %w", err)
	}
	sugar.Info("Blocks table ready", "tableName", cfg.RawBlocksTableName)

	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, cfg.RawTransactionsTableName)
	if err != nil {
		return fmt.Errorf("failed to create transactions repository: %w", err)
	}
	sugar.Info("Transactions table ready", "tableName", cfg.RawTransactionsTableName)

	logsRepo, err := evmrepo.NewLogs(ctx, chClient, cfg.RawLogsTableName)
	if err != nil {
		return fmt.Errorf("failed to create logs repository: %w", err)
	}
	sugar.Info("Logs table ready", "tableName", cfg.RawLogsTableName)

	// Create CorethProcessor with ClickHouse persistence and metrics
	proc := processor.NewCorethProcessor(sugar, blocksRepo, transactionsRepo, logsRepo, m)

	adminConfig := confluentKafka.ConfigMap{"bootstrap.servers": cfg.BootstrapServers}
	cfg.KafkaSASL.ApplyToConfigMap(&adminConfig)
	adminClient, err := confluentKafka.NewAdminClient(&adminConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer adminClient.Close()

	err = kafka.EnsureTopic(ctx, adminClient, kafka.TopicConfig{
		Name:              cfg.Topic,
		NumPartitions:     cfg.KafkaTopicNumPartitions,
		ReplicationFactor: cfg.KafkaTopicReplicationFactor,
	}, sugar)
	if err != nil {
		return fmt.Errorf("failed to ensure kafka topic exists: %w", err)
	}

	if cfg.PublishToDLQ {
		err = kafka.EnsureTopic(ctx, adminClient, kafka.TopicConfig{
			Name:              cfg.DLQTopic,
			NumPartitions:     cfg.KafkaDLQTopicNumPartitions,
			ReplicationFactor: cfg.KafkaDLQTopicReplicationFactor,
		}, sugar)
		if err != nil {
			return fmt.Errorf("failed to ensure kafka DLQ topic exists: %w", err)
		}
	}

	// Configure consumer
	consumerCfg := kafka.ConsumerConfig{
		DLQTopic:                    cfg.DLQTopic,
		Topic:                       cfg.Topic,
		Concurrency:                 cfg.Concurrency,
		PublishToDLQ:                cfg.PublishToDLQ,
		BootstrapServers:            cfg.BootstrapServers,
		GroupID:                     cfg.GroupID,
		AutoOffsetReset:             cfg.AutoOffsetReset,
		EnableLogs:                  cfg.EnableKafkaLogs,
		OffsetManagerCommitInterval: cfg.OffsetCommitInterval,
		SessionTimeout:              &cfg.SessionTimeout,
		MaxPollInterval:             &cfg.MaxPollInterval,
		FlushTimeout:                &cfg.FlushTimeout,
		GoroutineWaitTimeout:        &cfg.GoroutineWaitTimeout,
		PollInterval:                &cfg.PollInterval,
		SASL:                        cfg.KafkaSASL,
	}

	// Create consumer
	consumer, err := kafka.NewConsumer(ctx, sugar, consumerCfg, proc)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	sugar.Infow("consumer created, starting consumption",
		"topic", cfg.Topic,
		"groupID", cfg.GroupID,
		"concurrency", cfg.Concurrency,
	)

	// Run consumer and metrics server error handling concurrently using errgroup
	g, gctx := errgroup.WithContext(ctx)

	// Consumer goroutine - blocks until shutdown or error
	g.Go(func() error {
		if err := consumer.Start(gctx); err != nil {
			return fmt.Errorf("consumer error: %w", err)
		}
		return nil
	})

	// Metrics server error monitoring goroutine
	g.Go(func() error {
		select {
		case <-gctx.Done():
			return gctx.Err()
		case err := <-metricsErrCh:
			if err != nil {
				return fmt.Errorf("metrics server error: %w", err)
			}
			return nil
		}
	})

	// Wait for first error or completion from any goroutine
	err = g.Wait()

	// Gracefully shutdown metrics server
	sugar.Info("shutting down metrics server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if shutdownErr := metricsServer.Shutdown(shutdownCtx); shutdownErr != nil {
		sugar.Warnw("metrics server shutdown error", "error", shutdownErr)
	}

	sugar.Info("shutdown complete")
	return err
}
