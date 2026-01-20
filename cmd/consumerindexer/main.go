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
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/urfave/cli/v2"
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
						Name:     "topic",
						Aliases:  []string{"t"},
						Usage:    "Kafka topic to consume from",
						EnvVars:  []string{"KAFKA_TOPIC"},
						Required: true,
					},
					&cli.StringFlag{
						Name:    "dlq-topic",
						Usage:   "Dead letter queue topic for failed messages",
						EnvVars: []string{"KAFKA_DLQ_TOPIC"},
						Value:   "",
					},
					&cli.StringFlag{
						Name:    "auto-offset-reset",
						Aliases: []string{"o"},
						Usage:   "Kafka auto offset reset policy (earliest, latest, none)",
						EnvVars: []string{"KAFKA_AUTO_OFFSET_RESET"},
						Value:   "earliest",
					},
					&cli.Int64Flag{
						Name:    "concurrency",
						Usage:   "Concurrent message processors",
						EnvVars: []string{"KAFKA_CONCURRENCY"},
						Value:   10,
					},
					&cli.DurationFlag{
						Name:    "offset-commit-interval",
						Usage:   "Interval for committing offsets",
						EnvVars: []string{"KAFKA_OFFSET_COMMIT_INTERVAL"},
						Value:   10 * time.Second,
					},
					&cli.BoolFlag{
						Name:    "enable-kafka-logs",
						Usage:   "Enable librdkafka client logs",
						EnvVars: []string{"KAFKA_ENABLE_LOGS"},
						Value:   false,
					},
					&cli.DurationFlag{
						Name:    "session-timeout",
						Usage:   "Kafka consumer session timeout",
						EnvVars: []string{"KAFKA_SESSION_TIMEOUT"},
						Value:   240 * time.Second,
					},
					&cli.DurationFlag{
						Name:    "max-poll-interval",
						Usage:   "Kafka consumer max poll interval",
						EnvVars: []string{"KAFKA_MAX_POLL_INTERVAL"},
						Value:   3400 * time.Second,
					},
					&cli.DurationFlag{
						Name:    "flush-timeout",
						Usage:   "Kafka dlq producer flush timeout when closing",
						EnvVars: []string{"KAFKA_FLUSH_TIMEOUT"},
						Value:   15 * time.Second,
					},
					&cli.DurationFlag{
						Name:    "goroutine-wait-timeout",
						Usage:   "Timeout for waiting in-flight goroutines on shutdown",
						EnvVars: []string{"KAFKA_GOROUTINE_WAIT_TIMEOUT"},
						Value:   30 * time.Second,
					},
					&cli.DurationFlag{
						Name:    "poll-interval",
						Usage:   "Poll interval for Kafka consumer",
						EnvVars: []string{"KAFKA_POLL_INTERVAL"},
						Value:   100 * time.Millisecond,
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
	)

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

	// Create CorethProcessor with ClickHouse persistence
	proc := processor.NewCorethProcessor(sugar, rawBlocksRepo)

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

	// Start consumer (blocks until shutdown signal received)
	// Consumer closes itself gracefully inside Start() before returning
	if err := consumer.Start(ctx); err != nil {
		return fmt.Errorf("consumer error: %w", err)
	}

	sugar.Info("consumer shutdown complete")
	return nil
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
