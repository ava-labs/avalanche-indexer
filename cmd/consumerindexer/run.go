package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
)

func run(c *cli.Context) error {
	verbose := c.Bool("verbose")
	bootstrapServers := c.String("bootstrap-servers")
	groupID := c.String("group-id")
	topic := c.String("topic")
	dlqTopic := c.String("dlq-topic")
	autoOffsetReset := c.String("auto-offset-reset")
	concurrency := c.Int64("concurrency")
	offsetCommitInterval := c.Duration("offset-commit-interval")
	enableKafkaLogs := c.Bool("enable-kafka-logs")
	sessionTimeout := c.Duration("session-timeout")
	maxPollInterval := c.Duration("max-poll-interval")
	flushTimeout := c.Duration("flush-timeout")
	goroutineWaitTimeout := c.Duration("goroutine-wait-timeout")
	pollInterval := c.Duration("poll-interval")
	rawTableName := c.String("raw-blocks-table-name")
	metricsHost := c.String("metrics-host")
	metricsPort := c.Int("metrics-port")
	chainID := c.Uint64("chain-id")
	environment := c.String("environment")
	region := c.String("region")
	cloudProvider := c.String("cloud-provider")
	metricsAddr := fmt.Sprintf("%s:%d", metricsHost, metricsPort)
	rawBlocksTableName := c.String("raw-blocks-table-name")
	rawTransactionsTableName := c.String("raw-transactions-table-name")

	sugar, err := utils.NewSugaredLogger(verbose)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer sugar.Desugar().Sync() //nolint:errcheck // best-effort flush; ignore sync errors

	// Build ClickHouse config from CLI flags
	chCfg := buildClickHouseConfig(c)

	sugar.Infow("config",
		"verbose", verbose,
		"bootstrapServers", bootstrapServers,
		"groupID", groupID,
		"topic", topic,
		"dlqTopic", dlqTopic,
		"autoOffsetReset", autoOffsetReset,
		"maxConcurrency", concurrency,
		"offsetCommitInterval", offsetCommitInterval,
		"enableKafkaLogs", enableKafkaLogs,
		"sessionTimeout", sessionTimeout,
		"maxPollInterval", maxPollInterval,
		"flushTimeout", flushTimeout,
		"goroutineWaitTimeout", goroutineWaitTimeout,
		"pollInterval", pollInterval,
		"clickhouseHosts", chCfg.Hosts,
		"clickhouseDatabase", chCfg.Database,
		"clickhouseUsername", chCfg.Username,
		"clickhouseDebug", chCfg.Debug,
		"rawTableName", rawTableName,
		"metricsHost", metricsHost,
		"metricsPort", metricsPort,
		"chainID", chainID,
		"environment", environment,
		"region", region,
		"cloudProvider", cloudProvider,
		"rawBlocksTableName", rawBlocksTableName,
		"rawTransactionsTableName", rawTransactionsTableName,
	)

	// Initialize Prometheus metrics with labels for multi-instance filtering
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    chainID,
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

	// Initialize ClickHouse client
	chClient, err := clickhouse.New(chCfg, sugar)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	defer chClient.Close()

	sugar.Info("ClickHouse client created successfully")

	// Initialize repositories (tables are created automatically)
	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, rawBlocksTableName)
	if err != nil {
		return fmt.Errorf("failed to create blocks repository: %w", err)
	}
	sugar.Info("Blocks table ready", "tableName", rawBlocksTableName)

	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, rawTransactionsTableName)
	if err != nil {
		return fmt.Errorf("failed to create transactions repository: %w", err)
	}
	sugar.Info("Transactions table ready", "tableName", rawTransactionsTableName)

	// Create CorethProcessor with ClickHouse persistence and metrics
	proc := processor.NewCorethProcessor(sugar, blocksRepo, transactionsRepo, m)

	// Configure consumer
	consumerCfg := kafka.ConsumerConfig{
		DLQTopic:                    dlqTopic,
		Topic:                       topic,
		Concurrency:                 concurrency,
		IsDLQConsumer:               false,
		BootstrapServers:            bootstrapServers,
		GroupID:                     groupID,
		AutoOffsetReset:             autoOffsetReset,
		EnableLogs:                  enableKafkaLogs,
		OffsetManagerCommitInterval: offsetCommitInterval,
		SessionTimeout:              &sessionTimeout,
		MaxPollInterval:             &maxPollInterval,
		FlushTimeout:                &flushTimeout,
		GoroutineWaitTimeout:        &goroutineWaitTimeout,
		PollInterval:                &pollInterval,
	}

	// Create consumer
	consumer, err := kafka.NewConsumer(ctx, sugar, consumerCfg, proc)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	sugar.Infow("consumer created, starting consumption",
		"topic", topic,
		"groupID", groupID,
		"concurrency", concurrency,
	)

	// Run consumer and metrics server error handling concurrently
	errCh := make(chan error, 2)

	go func() {
		// Consumer.Start blocks until shutdown
		if err := consumer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("consumer error: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		select {
		case <-ctx.Done():
			// Signal completion to avoid blocking on errCh read
			errCh <- nil
		case err := <-metricsErrCh:
			if err != nil {
				errCh <- fmt.Errorf("metrics server error: %w", err)
			} else {
				errCh <- nil
			}
		}
	}()

	// Wait for first error or completion
	firstErr := <-errCh

	// Gracefully shutdown metrics server
	sugar.Info("shutting down metrics server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		sugar.Warnw("metrics server shutdown error", "error", err)
	}

	sugar.Info("shutdown complete")
	return firstErr
}
