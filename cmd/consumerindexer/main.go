package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/models"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {
	app := &cli.App{
		Name:  "consumerindexer",
		Usage: "Consume blocks from Kafka pipeline",
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "Run the consumer indexer",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "verbose",
						Aliases: []string{"v"},
						Usage:   "Enable verbose logging",
					},
					&cli.StringFlag{
						Name:     "bootstrap-servers",
						Aliases:  []string{"b"},
						Usage:    "Kafka bootstrap servers (comma-separated)",
						EnvVars:  []string{"KAFKA_BOOTSTRAP_SERVERS"},
						Required: true,
					},
					&cli.StringFlag{
						Name:     "group-id",
						Aliases:  []string{"g"},
						Usage:    "Kafka consumer group ID",
						EnvVars:  []string{"KAFKA_GROUP_ID"},
						Required: true,
					},
					&cli.StringFlag{
						Name:     "topics",
						Aliases:  []string{"t"},
						Usage:    "Kafka topics to consume from (comma-separated)",
						EnvVars:  []string{"KAFKA_TOPICS"},
						Required: true,
					},
					&cli.StringFlag{
						Name:    "auto-offset-reset",
						Aliases: []string{"o"},
						Usage:   "Kafka auto offset reset policy (earliest, latest, none)",
						EnvVars: []string{"KAFKA_AUTO_OFFSET_RESET"},
						Value:   "earliest",
					},
					// ClickHouse configuration flags
					&cli.StringSliceFlag{
						Name:    "clickhouse-hosts",
						Usage:   "ClickHouse server hosts (comma-separated)",
						EnvVars: []string{"CLICKHOUSE_HOSTS"},
						Value:   cli.NewStringSlice("localhost:9000"),
					},
					&cli.StringFlag{
						Name:    "clickhouse-database",
						Usage:   "ClickHouse database name",
						EnvVars: []string{"CLICKHOUSE_DATABASE"},
						Value:   "default",
					},
					&cli.StringFlag{
						Name:    "clickhouse-username",
						Usage:   "ClickHouse username",
						EnvVars: []string{"CLICKHOUSE_USERNAME"},
						Value:   "default",
					},
					&cli.StringFlag{
						Name:    "clickhouse-password",
						Usage:   "ClickHouse password",
						EnvVars: []string{"CLICKHOUSE_PASSWORD"},
						Value:   "",
					},
					&cli.BoolFlag{
						Name:    "clickhouse-debug",
						Usage:   "Enable ClickHouse debug logging",
						EnvVars: []string{"CLICKHOUSE_DEBUG"},
					},
					&cli.BoolFlag{
						Name:    "clickhouse-insecure-skip-verify",
						Usage:   "Skip TLS certificate verification for ClickHouse",
						EnvVars: []string{"CLICKHOUSE_INSECURE_SKIP_VERIFY"},
						Value:   true,
					},
					&cli.IntFlag{
						Name:    "clickhouse-max-execution-time",
						Usage:   "ClickHouse max execution time in seconds",
						EnvVars: []string{"CLICKHOUSE_MAX_EXECUTION_TIME"},
						Value:   60,
					},
					&cli.IntFlag{
						Name:    "clickhouse-dial-timeout",
						Usage:   "ClickHouse dial timeout in seconds",
						EnvVars: []string{"CLICKHOUSE_DIAL_TIMEOUT"},
						Value:   30,
					},
					&cli.IntFlag{
						Name:    "clickhouse-max-open-conns",
						Usage:   "ClickHouse maximum open connections",
						EnvVars: []string{"CLICKHOUSE_MAX_OPEN_CONNS"},
						Value:   5,
					},
					&cli.IntFlag{
						Name:    "clickhouse-max-idle-conns",
						Usage:   "ClickHouse maximum idle connections",
						EnvVars: []string{"CLICKHOUSE_MAX_IDLE_CONNS"},
						Value:   5,
					},
					&cli.IntFlag{
						Name:    "clickhouse-conn-max-lifetime",
						Usage:   "ClickHouse connection max lifetime in minutes",
						EnvVars: []string{"CLICKHOUSE_CONN_MAX_LIFETIME"},
						Value:   10,
					},
					&cli.IntFlag{
						Name:    "clickhouse-block-buffer-size",
						Usage:   "ClickHouse block buffer size",
						EnvVars: []string{"CLICKHOUSE_BLOCK_BUFFER_SIZE"},
						Value:   10,
					},
					&cli.IntFlag{
						Name:    "clickhouse-max-block-size",
						Usage:   "ClickHouse max block size (recommended maximum number of rows in a single block)",
						EnvVars: []string{"CLICKHOUSE_MAX_BLOCK_SIZE"},
						Value:   1000,
					},
					&cli.IntFlag{
						Name:    "clickhouse-max-compression-buffer",
						Usage:   "ClickHouse max compression buffer in bytes",
						EnvVars: []string{"CLICKHOUSE_MAX_COMPRESSION_BUFFER"},
						Value:   10240,
					},
					&cli.StringFlag{
						Name:    "clickhouse-client-name",
						Usage:   "ClickHouse client name for ClientInfo",
						EnvVars: []string{"CLICKHOUSE_CLIENT_NAME"},
						Value:   "ac-client-name",
					},
					&cli.StringFlag{
						Name:    "clickhouse-client-version",
						Usage:   "ClickHouse client version for ClientInfo",
						EnvVars: []string{"CLICKHOUSE_CLIENT_VERSION"},
						Value:   "1.0",
					},
					&cli.BoolFlag{
						Name:    "clickhouse-use-http",
						Usage:   "Use HTTP protocol instead of native protocol",
						EnvVars: []string{"CLICKHOUSE_USE_HTTP"},
						Value:   false,
					},
					&cli.StringFlag{
						Name:    "raw-blocks-table-name",
						Usage:   "ClickHouse table name for raw blocks",
						EnvVars: []string{"CLICKHOUSE_RAW_BLOCKS_TABLE_NAME"},
						Value:   "default.raw_blocks",
					},
					// Metrics configuration flags
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
					&cli.StringFlag{
						Name:    "chain-id",
						Aliases: []string{"C"},
						Usage:   "Chain identifier for metrics labels (e.g., '43114' for C-Chain mainnet)",
						EnvVars: []string{"CHAIN_ID"},
						Value:   "",
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
	bootstrapServers := c.String("bootstrap-servers")
	groupID := c.String("group-id")
	topicsStr := c.String("topics")
	autoOffsetReset := c.String("auto-offset-reset")
	rawTableName := c.String("raw-blocks-table-name")
	metricsHost := c.String("metrics-host")
	metricsPort := c.Int("metrics-port")
	chainID := c.String("chain-id")
	metricsAddr := fmt.Sprintf("%s:%d", metricsHost, metricsPort)

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
		"topics", topicsStr,
		"autoOffsetReset", autoOffsetReset,
		"clickhouseHosts", chCfg.Hosts,
		"clickhouseDatabase", chCfg.Database,
		"clickhouseUsername", chCfg.Username,
		"clickhouseDebug", chCfg.Debug,
		"rawTableName", rawTableName,
		"metricsHost", metricsHost,
		"metricsPort", metricsPort,
		"chainID", chainID,
	)

	// Initialize Prometheus metrics with optional chain label
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{Chain: chainID})
	if err != nil {
		return fmt.Errorf("failed to create metrics: %w", err)
	}

	// Start metrics server
	metricsServer := metrics.NewServer(metricsAddr, registry)
	metricsErrCh := metricsServer.Start()
	sugar.Infof("metrics server started at http://localhost%s/metrics", metricsAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize ClickHouse client
	chClient, err := clickhouse.New(chCfg, sugar)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	defer chClient.Close()

	sugar.Info("ClickHouse client created successfully")

	// Initialize raw blocks repository
	rawBlocksRepo := models.NewRepository(chClient, rawTableName)
	sugar.Info("Raw blocks repository initialized", "tableName", rawTableName)

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupID,
		"auto.offset.reset": autoOffsetReset,
	})
	if err != nil {
		return fmt.Errorf("failed to create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	sugar.Info("Kafka consumer created successfully")

	// Parse topics
	topics := strings.Split(topicsStr, ",")
	for i, topic := range topics {
		topics[i] = strings.TrimSpace(topic)
	}

	// Rebalance callback to handle partition assignment/revocation
	rebalanceCallback := func(c *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			partitions := make([]string, len(e.Partitions))
			for i, p := range e.Partitions {
				partitions[i] = fmt.Sprintf("%s[%d]", *p.Topic, p.Partition)
			}
			sugar.Infow("partitions assigned", "partitions", partitions)
			return c.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			partitions := make([]string, len(e.Partitions))
			for i, p := range e.Partitions {
				partitions[i] = fmt.Sprintf("%s[%d]", *p.Topic, p.Partition)
			}
			sugar.Infow("partitions revoked", "partitions", partitions)
			return c.Unassign()
		default:
			return nil
		}
	}

	// Subscribe to topics
	err = consumer.SubscribeTopics(topics, rebalanceCallback)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	sugar.Infow("subscribed to topics", "topics", topics)

	sugar.Info("starting consumer indexer")

	// Consumer loop with metrics server error handling
	var loopErr error
consumerLoop:
	for {
		// Check for metrics server errors (non-blocking)
		select {
		case err := <-metricsErrCh:
			if err != nil {
				loopErr = fmt.Errorf("metrics server failed: %w", err)
				break consumerLoop
			}
		default:
		}

		select {
		case <-ctx.Done():
			sugar.Info("shutting down consumer...")
			break consumerLoop
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				sugar.Debugw("received message",
					"topic", *e.TopicPartition.Topic,
					"partition", e.TopicPartition.Partition,
					"offset", e.TopicPartition.Offset,
				)
				if err := processMessage(ctx, e, rawBlocksRepo, m, sugar); err != nil {
					sugar.Errorw("failed to process message",
						"topic", *e.TopicPartition.Topic,
						"partition", e.TopicPartition.Partition,
						"offset", e.TopicPartition.Offset,
						"error", err,
					)
					// Continue processing other messages even if one fails
					// TODO: Add retry logic and DLQ logic
					continue
				}
			case kafka.Error:
				if e.Code() == kafka.ErrPartitionEOF {
					sugar.Debugw("reached end of partition", "error", e)
					continue
				}
				if e.IsFatal() {
					sugar.Errorw("fatal kafka error", "code", fmt.Sprintf("%#x", e.Code()), "error", e)
					loopErr = fmt.Errorf("fatal kafka error: %w", e)
					break consumerLoop
				}
				if e.Code() == kafka.ErrAllBrokersDown {
					sugar.Errorw("all brokers down", "code", fmt.Sprintf("%#x", e.Code()), "error", e)
					loopErr = fmt.Errorf("all brokers down: %w", e)
					break consumerLoop
				}
				// Non-fatal errors are usually informational
				sugar.Warnw("ignoring unexpected kafka error", "code", fmt.Sprintf("%#x", e.Code()), "error", e)
				continue
			default:
				sugar.Debugw("ignored event", "type", fmt.Sprintf("%T", e))
			}
		}
	}

	// Gracefully shutdown metrics server
	sugar.Info("shutting down metrics server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		sugar.Warnw("metrics server shutdown error", "error", err)
	}

	sugar.Info("shutdown complete")
	return loopErr
}

// buildClickHouseConfig builds a ClickhouseConfig from CLI context flags
func buildClickHouseConfig(c *cli.Context) clickhouse.Config {
	// Handle hosts - StringSliceFlag returns []string, but we need to handle comma-separated values
	hosts := c.StringSlice("clickhouse-hosts")
	// If hosts is a single comma-separated string, split it
	if len(hosts) == 1 && strings.Contains(hosts[0], ",") {
		hosts = strings.Split(hosts[0], ",")
		for i, host := range hosts {
			hosts[i] = strings.TrimSpace(host)
		}
	}
	// Safely clamp and convert block buffer size to uint8 range
	return clickhouse.Config{
		Hosts:                hosts,
		Database:             c.String("clickhouse-database"),
		Username:             c.String("clickhouse-username"),
		Password:             c.String("clickhouse-password"),
		Debug:                c.Bool("clickhouse-debug"),
		InsecureSkipVerify:   c.Bool("clickhouse-insecure-skip-verify"),
		MaxExecutionTime:     c.Int("clickhouse-max-execution-time"),
		DialTimeout:          c.Int("clickhouse-dial-timeout"),
		MaxOpenConns:         c.Int("clickhouse-max-open-conns"),
		MaxIdleConns:         c.Int("clickhouse-max-idle-conns"),
		ConnMaxLifetime:      c.Int("clickhouse-conn-max-lifetime"),
		BlockBufferSize:      uint8(c.Int("clickhouse-block-buffer-size")),
		MaxBlockSize:         c.Int("clickhouse-max-block-size"),
		MaxCompressionBuffer: c.Int("clickhouse-max-compression-buffer"),
		ClientName:           c.String("clickhouse-client-name"),
		ClientVersion:        c.String("clickhouse-client-version"),
		UseHTTP:              c.Bool("clickhouse-use-http"),
	}
}

// processMessage processes a Kafka message and writes it to ClickHouse
func processMessage(ctx context.Context, msg *kafka.Message, rawBlocksRepo models.Repository, m *metrics.Metrics, sugar *zap.SugaredLogger) error {
	topic := *msg.TopicPartition.Topic

	switch topic {
	case "blocks":
		return processBlockMessage(ctx, msg.Value, rawBlocksRepo, m, sugar)
	default:
		sugar.Debugw("ignoring message from unknown topic", "topic", topic)
		return nil
	}
}

// processBlockMessage processes a block message from Kafka
func processBlockMessage(ctx context.Context, data []byte, rawBlocksRepo models.Repository, m *metrics.Metrics, sugar *zap.SugaredLogger) error {
	start := time.Now()

	// Parse the block - ParseBlockFromJSON will extract chainID internally
	block, err := models.ParseBlockFromJSON(data)
	if err != nil {
		m.IncError("parse_error")
		// TODO: Add DLQ logic
		return fmt.Errorf("failed to parse block: %w", err)
	}

	if err := rawBlocksRepo.WriteBlock(ctx, block); err != nil {
		m.IncError("write_error")
		return fmt.Errorf("failed to write block: %w", err)
	}

	// Record successful block processing
	m.ObserveBlockProcessingDuration(time.Since(start).Seconds())

	sugar.Debugw("successfully wrote block",
		"chainID", block.ChainID,
		"blockNumber", block.BlockNumber,
		"nonce", block.Nonce,
	)

	return nil
}
