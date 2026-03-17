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

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	blocksMode = "blocks"
	tracesMode = "traces"
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

	mode := cfg.Mode

	sugar.Infow("config",
		"mode", cfg.Mode,
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
		"clickhouseCluster", cfg.ClickHouse.Cluster,
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
		"internalTransactionsTableName", cfg.InternalTransactionsTableName,
		"publishToDLQ", cfg.PublishToDLQ,
		"kafkaTopicNumPartitions", cfg.KafkaTopicNumPartitions,
		"kafkaTopicReplicationFactor", cfg.KafkaTopicReplicationFactor,
		"kafkaDLQTopicNumPartitions", cfg.KafkaDLQTopicNumPartitions,
		"kafkaDLQTopicReplicationFactor", cfg.KafkaDLQTopicReplicationFactor,
	)

	// Initialize Prometheus metrics with labels for multi-instance filtering.
	// The primary consumer and DLQ consumer each get their own metrics instance
	// differentiated by the "role" label so they can coexist on the same registry.
	registry := prometheus.NewRegistry()
	baseLabels := metrics.Labels{
		EVMChainID:    cfg.ChainID,
		Environment:   cfg.Environment,
		Region:        cfg.Region,
		CloudProvider: cfg.CloudProvider,
		Role:          metrics.RolePrimaryConsumer,
	}
	m, err := metrics.NewWithLabels(registry, baseLabels)
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

	// Named loggers for primary and DLQ consumers
	primaryLog := sugar.Named("primary_consumer")
	dlqLog := sugar.Named("dlq_consumer")

	proc, err := newProcessor(ctx, mode, primaryLog, chClient, cfg, m)
	if err != nil {
		return fmt.Errorf("failed to create processor: %w", err)
	}

	adminConfig := ckafka.ConfigMap{"bootstrap.servers": cfg.BootstrapServers}
	cfg.KafkaSASL.ApplyToConfigMap(&adminConfig)
	adminClient, err := ckafka.NewAdminClient(&adminConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer adminClient.Close()

	mainTopicConfig := kafka.TopicConfig{
		Name:              cfg.Topic,
		NumPartitions:     cfg.KafkaTopicNumPartitions,
		ReplicationFactor: cfg.KafkaTopicReplicationFactor,
		Config:            make(map[string]string),
	}

	if cfg.KafkaTopicRetentionMs != "" {
		mainTopicConfig.Config["retention.ms"] = cfg.KafkaTopicRetentionMs
	}
	if cfg.KafkaTopicRetentionBytes != "" {
		mainTopicConfig.Config["retention.bytes"] = cfg.KafkaTopicRetentionBytes
	}
	if cfg.KafkaTopicMessageMaxBytes != "" {
		mainTopicConfig.Config["max.message.bytes"] = cfg.KafkaTopicMessageMaxBytes
	}

	err = kafka.EnsureTopic(ctx, adminClient, mainTopicConfig, sugar)
	if err != nil {
		return fmt.Errorf("failed to ensure kafka topic exists: %w", err)
	}

	// Ensure DLQ topic exists with configs (if enabled)
	if cfg.PublishToDLQ {
		dlqTopicConfig := kafka.TopicConfig{
			Name:              cfg.DLQTopic,
			NumPartitions:     cfg.KafkaDLQTopicNumPartitions,
			ReplicationFactor: cfg.KafkaDLQTopicReplicationFactor,
			Config:            make(map[string]string),
		}

		if cfg.KafkaDLQTopicRetentionMs != "" {
			dlqTopicConfig.Config["retention.ms"] = cfg.KafkaDLQTopicRetentionMs
		}
		if cfg.KafkaDLQTopicRetentionBytes != "" {
			dlqTopicConfig.Config["retention.bytes"] = cfg.KafkaDLQTopicRetentionBytes
		}
		if cfg.KafkaTopicMessageMaxBytes != "" {
			dlqTopicConfig.Config["max.message.bytes"] = cfg.KafkaTopicMessageMaxBytes
		}

		err = kafka.EnsureTopic(ctx, adminClient, dlqTopicConfig, sugar)
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
		Retry: kafka.RetryPolicy{
			MaxRetries: 3,
			BaseDelay:  500 * time.Millisecond,
			MaxDelay:   2 * time.Second,
		},
	}

	// Create primary consumer with named logger
	consumer, err := kafka.NewConsumer(ctx, primaryLog, consumerCfg, proc, m)
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

	if cfg.EnableDLQConsumer {
		dlqConsumer, err := newDLQConsumer(ctx, cfg, baseLabels, registry, mode, dlqLog, chClient)
		if err != nil {
			return fmt.Errorf("failed to create DLQ consumer: %w", err)
		}
		sugar.Infow("DLQ consumer created, starting consumption",
			"topic", cfg.DLQTopic,
			"groupID", cfg.DLQConsumerGroupID,
			"concurrency", cfg.DLQConsumerConcurrency,
		)

		g.Go(func() error {
			if err := dlqConsumer.Start(gctx); err != nil {
				dlqLog.Errorw("DLQ consumer stopped with error", "error", err)
				return err
			}
			return nil
		})
	}

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

// newProcessor creates a processor.Processor for the given mode, wired with
// the provided logger, ClickHouse client, config, and metrics. Both the
// primary and DLQ consumers use this to avoid duplicating the repository
// and processor initialization logic.
func newProcessor(
	ctx context.Context,
	mode string,
	log *zap.SugaredLogger,
	chClient clickhouse.Client,
	cfg *Config,
	m *metrics.Metrics,
) (processor.Processor, error) {
	switch mode {
	case blocksMode:
		blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, cfg.ClickHouse.Cluster, cfg.ClickHouse.Database, cfg.RawBlocksTableName)
		if err != nil {
			return nil, fmt.Errorf("blocks repository: %w", err)
		}
		transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, cfg.ClickHouse.Cluster, cfg.ClickHouse.Database, cfg.RawTransactionsTableName)
		if err != nil {
			return nil, fmt.Errorf("transactions repository: %w", err)
		}
		logsRepo, err := evmrepo.NewLogs(ctx, chClient, cfg.ClickHouse.Cluster, cfg.ClickHouse.Database, cfg.RawLogsTableName)
		if err != nil {
			return nil, fmt.Errorf("logs repository: %w", err)
		}
		return processor.NewCorethProcessor(log, blocksRepo, transactionsRepo, logsRepo, m), nil
	case tracesMode:
		internalTxRepo, err := evmrepo.NewInternalTransactions(ctx, chClient, cfg.ClickHouse.Cluster, cfg.ClickHouse.Database, cfg.InternalTransactionsTableName)
		if err != nil {
			return nil, fmt.Errorf("internal transactions repository: %w", err)
		}
		return processor.NewCorethTracesProcessor(log, internalTxRepo, m), nil
	default:
		return nil, fmt.Errorf("invalid mode: %s", mode)
	}
}

// newDLQConsumer validates DLQ-specific configuration and creates a Consumer
// that subscribes to the DLQ topic with infinite-retry semantics. It never
// publishes failures to a secondary DLQ (no cascading failure loop).
func newDLQConsumer(
	ctx context.Context,
	cfg *Config,
	baseLabels metrics.Labels,
	registry *prometheus.Registry,
	mode string,
	log *zap.SugaredLogger,
	chClient clickhouse.Client,
) (*kafka.Consumer, error) {
	if cfg.DLQTopic == "" {
		return nil, errors.New("DLQ topic must be set when DLQ consumer is enabled")
	}
	if cfg.DLQConsumerGroupID == "" {
		return nil, errors.New("DLQ consumer group ID must be set when DLQ consumer is enabled")
	}
	if cfg.GroupID == cfg.DLQConsumerGroupID {
		return nil, errors.New("DLQ consumer group ID must differ from the primary consumer group ID")
	}

	dlqLabels := baseLabels
	dlqLabels.Role = metrics.RoleDLQConsumer
	dlqMetrics, err := metrics.NewWithLabels(registry, dlqLabels)
	if err != nil {
		return nil, fmt.Errorf("DLQ metrics: %w", err)
	}

	dlqProc, err := newProcessor(ctx, mode, log, chClient, cfg, dlqMetrics)
	if err != nil {
		return nil, fmt.Errorf("DLQ processor: %w", err)
	}

	dlqCfg := kafka.ConsumerConfig{
		Topic:                       cfg.DLQTopic,
		Concurrency:                 cfg.DLQConsumerConcurrency,
		PublishToDLQ:                false,
		BootstrapServers:            cfg.BootstrapServers,
		GroupID:                     cfg.DLQConsumerGroupID,
		AutoOffsetReset:             cfg.AutoOffsetReset,
		EnableLogs:                  cfg.EnableKafkaLogs,
		OffsetManagerCommitInterval: cfg.DLQConsumerOffsetCommitInterval,
		SessionTimeout:              &cfg.DLQConsumerSessionTimeout,
		MaxPollInterval:             &cfg.DLQConsumerMaxPollInterval,
		GoroutineWaitTimeout:        &cfg.DLQConsumerGoroutineWaitTimeout,
		PollInterval:                &cfg.DLQConsumerPollInterval,
		SASL:                        cfg.KafkaSASL,
		Retry: kafka.RetryPolicy{
			MaxRetries: kafka.InfiniteRetries,
			BaseDelay:  1 * time.Second,
			MaxDelay:   5 * time.Minute,
		},
	}

	return kafka.NewConsumer(ctx, log, dlqCfg, dlqProc, dlqMetrics)
}
