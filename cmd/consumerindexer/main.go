package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/models"
	"github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const (
	// KafkaTopicBlocks is the Kafka topic name for blocks
	KafkaTopicBlocks = "blocks"
)

// repositories holds all the repositories needed for processing messages
type repositories struct {
	blocks       models.BlocksRepository
	transactions models.TransactionsRepository
}

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
					&cli.StringFlag{
						Name:    "raw-transactions-table-name",
						Usage:   "ClickHouse table name for raw transactions",
						EnvVars: []string{"CLICKHOUSE_RAW_TRANSACTIONS_TABLE_NAME"},
						Value:   "default.raw_transactions",
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
		"topics", topicsStr,
		"autoOffsetReset", autoOffsetReset,
		"clickhouseHosts", chCfg.Hosts,
		"clickhouseDatabase", chCfg.Database,
		"clickhouseUsername", chCfg.Username,
		"clickhouseDebug", chCfg.Debug,
		"rawBlocksTableName", rawBlocksTableName,
		"rawTransactionsTableName", rawTransactionsTableName,
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

	// Initialize repositories (tables are created automatically)
	blocksRepo, err := models.NewBlocksRepository(ctx, chClient, rawBlocksTableName)
	if err != nil {
		return fmt.Errorf("failed to create blocks repository: %w", err)
	}
	sugar.Info("Blocks table ready", "tableName", rawBlocksTableName)

	transactionsRepo, err := models.NewTransactionsRepository(ctx, chClient, rawTransactionsTableName)
	if err != nil {
		return fmt.Errorf("failed to create transactions repository: %w", err)
	}
	sugar.Info("Transactions table ready", "tableName", rawTransactionsTableName)

	repos := &repositories{
		blocks:       blocksRepo,
		transactions: transactionsRepo,
	}

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

	// Consumer loop
	for {
		select {
		case <-ctx.Done():
			sugar.Info("shutting down consumer...")
			return nil
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
				if err := processMessage(ctx, e, repos, sugar); err != nil {
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
					return fmt.Errorf("fatal kafka error: %w", e)
				}
				if e.Code() == kafka.ErrAllBrokersDown {
					sugar.Errorw("all brokers down", "code", fmt.Sprintf("%#x", e.Code()), "error", e)
					return fmt.Errorf("all brokers down: %w", e)
				}
				// Non-fatal errors are usually informational
				sugar.Warnw("ignoring unexpected kafka error", "code", fmt.Sprintf("%#x", e.Code()), "error", e)
				continue
			default:
				sugar.Debugw("ignored event", "type", fmt.Sprintf("%T", e))
			}
		}
	}
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
func processMessage(ctx context.Context, msg *kafka.Message, repos *repositories, sugar *zap.SugaredLogger) error {
	topic := *msg.TopicPartition.Topic

	switch topic {
	case KafkaTopicBlocks:
		return processBlockMessage(ctx, msg.Value, repos, sugar)
	default:
		sugar.Debugw("ignoring message from unknown topic", "topic", topic)
		return nil
	}
}

// processBlockMessage processes a block message from Kafka
func processBlockMessage(ctx context.Context, data []byte, repos *repositories, sugar *zap.SugaredLogger) error {
	// Parse JSON payload directly to coreth.Block
	var block coreth.Block
	if err := json.Unmarshal(data, &block); err != nil {
		// TODO: Add DLQ logic
		return fmt.Errorf("failed to unmarshal block JSON: %w", err)
	}

	// Validate chainID
	if block.ChainID == nil {
		return models.ErrBlockChainIDRequired
	}
	chainID := uint32(block.ChainID.Uint64())

	// Extract block number and hash for idempotency check
	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}
	blockHash := block.Hash

	// Check if block already exists (idempotency check)
	exists, err := repos.blocks.BlockExists(ctx, chainID, blockNumber, blockHash)
	if err != nil {
		return fmt.Errorf("failed to check if block exists: %w", err)
	}
	if exists {
		sugar.Debugw("block already exists, skipping",
			"chainID", chainID,
			"blockNumber", blockNumber,
			"blockHash", blockHash,
		)
		return nil
	}

	// Process the block
	if err := processBlock(ctx, &block, repos, sugar); err != nil {
		return err
	}

	// Process transactions if any exist
	if len(block.Transactions) > 0 {
		if err := processTransactions(ctx, &block, repos, sugar); err != nil {
			return err
		}
	}

	return nil
}

// processBlock converts a coreth.Block to BlockRow and writes it to ClickHouse
func processBlock(ctx context.Context, block *coreth.Block, repos *repositories, sugar *zap.SugaredLogger) error {
	// Validate chainID
	if block.ChainID == nil {
		return models.ErrBlockChainIDRequired
	}
	chainID := uint32(block.ChainID.Uint64())

	// Convert to BlockRow
	blockRow := corethBlockToBlockRow(block, chainID)

	// Write the block
	if err := repos.blocks.WriteBlock(ctx, blockRow); err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}

	sugar.Debugw("successfully wrote block",
		"chainID", blockRow.ChainID,
		"blockNumber", blockRow.BlockNumber,
		"nonce", blockRow.Nonce,
	)

	return nil
}

// processTransactions converts transactions from a coreth.Block to TransactionRow and writes them to ClickHouse
func processTransactions(ctx context.Context, block *coreth.Block, repos *repositories, sugar *zap.SugaredLogger) error {
	// Validate chainID
	if block.ChainID == nil {
		return models.ErrBlockChainIDRequiredForTx
	}
	chainID := uint32(block.ChainID.Uint64())

	// Convert and write each transaction
	// TODO: Add batching (in a future PR)
	for i, tx := range block.Transactions {
		txRow, err := corethTransactionToTransactionRow(tx, block, uint64(i))
		if err != nil {
			return fmt.Errorf("failed to convert transaction %d: %w", i, err)
		}

		if err := repos.transactions.WriteTransaction(ctx, txRow); err != nil {
			return fmt.Errorf("failed to write transaction %s: %w", tx.Hash, err)
		}
	}

	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	sugar.Debugw("successfully wrote transactions",
		"chainID", chainID,
		"blockNumber", blockNumber,
		"transactionCount", len(block.Transactions),
	)

	return nil
}

// corethBlockToBlockRow converts a coreth.Block to BlockRow
func corethBlockToBlockRow(block *coreth.Block, chainID uint32) *models.BlockRow {
	// Extract number from big.Int
	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	blockRow := &models.BlockRow{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Size:        block.Size,
		GasLimit:    block.GasLimit,
		GasUsed:     block.GasUsed,
		BlockTime:   time.Unix(int64(block.Timestamp), 0).UTC(),
		ExtraData:   block.ExtraData,
	}

	// Set difficulty from big.Int
	if block.Difficulty != nil {
		blockRow.Difficulty = block.Difficulty.Uint64()
		blockRow.TotalDifficulty = block.Difficulty.Uint64() // Using same value for now
	}

	// Direct string assignments - no conversions needed
	blockRow.Hash = block.Hash
	blockRow.ParentHash = block.ParentHash
	blockRow.StateRoot = block.StateRoot
	blockRow.TransactionsRoot = block.TransactionsRoot
	blockRow.ReceiptsRoot = block.ReceiptsRoot
	blockRow.Sha3Uncles = block.UncleHash
	blockRow.MixHash = block.MixHash
	blockRow.Miner = block.Miner

	// Parse nonce - convert uint64 to hex string
	blockRow.Nonce = strconv.FormatUint(block.Nonce, 16)

	// Optional fields
	if block.BaseFee != nil {
		blockRow.BaseFeePerGas = block.BaseFee.Uint64()
	}
	if block.BlobGasUsed != nil {
		blockRow.BlobGasUsed = *block.BlobGasUsed
	}
	if block.ExcessBlobGas != nil {
		blockRow.ExcessBlobGas = *block.ExcessBlobGas
	}
	if block.ParentBeaconBlockRoot != "" {
		blockRow.ParentBeaconBlockRoot = block.ParentBeaconBlockRoot
	}
	if block.MinDelayExcess != 0 {
		blockRow.MinDelayExcess = block.MinDelayExcess
	}

	return blockRow
}

// corethTransactionToTransactionRow converts a coreth.Transaction to TransactionRow
func corethTransactionToTransactionRow(tx *coreth.Transaction, block *coreth.Block, txIndex uint64) (*models.TransactionRow, error) {
	// Extract chainID from block
	if block.ChainID == nil {
		return nil, models.ErrBlockChainIDRequiredForTx
	}
	chainID := uint32(block.ChainID.Uint64())

	// Extract block number
	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	txRow := &models.TransactionRow{
		ChainID:          chainID,
		BlockNumber:      blockNumber,
		BlockHash:        block.Hash,
		BlockTime:        time.Unix(int64(block.Timestamp), 0).UTC(),
		Hash:             tx.Hash,
		From:             tx.From,
		Nonce:            tx.Nonce,
		Gas:              tx.Gas,
		Input:            tx.Input,
		Type:             tx.Type,
		TransactionIndex: txIndex,
	}

	// Handle nullable To field
	if tx.To != "" {
		txRow.To = &tx.To
	}

	// Convert big.Int values to string
	if tx.Value != nil {
		txRow.Value = tx.Value.String()
	} else {
		txRow.Value = "0"
	}

	if tx.GasPrice != nil {
		txRow.GasPrice = tx.GasPrice.String()
	} else {
		txRow.GasPrice = "0"
	}

	// Handle nullable MaxFeePerGas
	if tx.MaxFeePerGas != nil {
		maxFeeStr := tx.MaxFeePerGas.String()
		txRow.MaxFeePerGas = &maxFeeStr
	}

	// Handle nullable MaxPriorityFee
	if tx.MaxPriorityFee != nil {
		maxPriorityStr := tx.MaxPriorityFee.String()
		txRow.MaxPriorityFee = &maxPriorityStr
	}

	return txRow, nil
}
