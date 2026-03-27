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

	"github.com/ava-labs/avalanche-indexer/pkg/data/dynamodb"
	"github.com/ava-labs/avalanche-indexer/pkg/data/dynamodb/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func run(c *cli.Context) error {
	cfg, err := buildConfig(c)
	if err != nil {
		return fmt.Errorf("failed to build config: %w", err)
	}

	sugar, err := utils.NewSugaredLogger(cfg.Verbose)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer sugar.Desugar().Sync() //nolint:errcheck

	sugar.Infow("config",
		"verbose", cfg.Verbose,
		"bootstrapServers", cfg.BootstrapServers,
		"groupID", cfg.GroupID,
		"topic", cfg.Topic,
		"dlqTopic", cfg.DLQTopic,
		"autoOffsetReset", cfg.AutoOffsetReset,
		"concurrency", cfg.Concurrency,
		"dynamoRegion", cfg.DynamoDB.Region,
		"dynamoEndpoint", cfg.DynamoDB.Endpoint,
		"historyTable", cfg.DynamoDB.HistoryTable,
		"ercTable", cfg.DynamoDB.ERCTable,
		"metricsPort", cfg.MetricsPort,
		"chainID", cfg.ChainID,
	)

	// Initialize Prometheus metrics
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

	metricsServer := metrics.NewServer(cfg.MetricsAddr(), registry)
	metricsErrCh := metricsServer.Start()
	sugar.Infof("metrics server listening on http://%s/metrics", cfg.MetricsAddr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize DynamoDB client
	ddbClient, err := dynamodb.New(ctx, cfg.DynamoDB, sugar)
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB client: %w", err)
	}

	// Initialize repository and ensure tables exist
	repo := evmrepo.NewRepository(ddbClient, cfg.DynamoDB.HistoryTable, cfg.DynamoDB.ERCTable, sugar)
	if err := repo.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize DynamoDB tables: %w", err)
	}

	sugar.Info("DynamoDB client and tables initialized")

	// Create processor
	proc := processor.NewCoreConsumerProcessor(sugar.Named("processor"), repo, m)

	// Ensure Kafka topic exists
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

	if err := kafka.EnsureTopic(ctx, adminClient, mainTopicConfig, sugar); err != nil {
		return fmt.Errorf("failed to ensure kafka topic exists: %w", err)
	}

	// Ensure DLQ topic exists (if enabled)
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
		if err := kafka.EnsureTopic(ctx, adminClient, dlqTopicConfig, sugar); err != nil {
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
			MaxRetries: cfg.ConsumerRetryMaxRetries,
			BaseDelay:  cfg.ConsumerRetryBaseDelay,
			MaxDelay:   cfg.ConsumerRetryMaxDelay,
		},
	}

	consumer, err := kafka.NewConsumer(ctx, sugar.Named("consumer"), consumerCfg, proc, m)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	sugar.Infow("consumer created, starting consumption",
		"topic", cfg.Topic,
		"groupID", cfg.GroupID,
		"concurrency", cfg.Concurrency,
	)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := consumer.Start(gctx); err != nil {
			return fmt.Errorf("consumer error: %w", err)
		}
		return nil
	})

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

	err = g.Wait()

	sugar.Info("shutting down metrics server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if shutdownErr := metricsServer.Shutdown(shutdownCtx); shutdownErr != nil {
		sugar.Warnw("metrics server shutdown error", "error", shutdownErr)
	}

	sugar.Info("shutdown complete")
	return err
}
