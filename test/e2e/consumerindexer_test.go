//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/ava-labs/libevm/common"
	libevmtypes "github.com/ava-labs/libevm/core/types"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Test helper constants for creating deterministic test data
const (
	// txHashMultiplier is used for generating unique transaction hashes within blocks
	txHashMultiplier = 100

	// txHashMultiplierLarge is used for large payload tests to avoid hash collisions
	txHashMultiplierLarge = 1000

	// oneEtherInWei represents 1 ETH in wei (10^18)
	oneEtherInWei = 1000000000000000000

	RoleDLQConsumer      = metrics.RoleDLQConsumer
	RolePrimaryConsumer  = metrics.RolePrimaryConsumer
	producerFlushTimeout = 5000
)

// durationPtr returns a pointer to a time.Duration.
// This is useful for optional duration fields in config structs.
func durationPtr(d time.Duration) *time.Duration {
	return &d
}

// TestE2EConsumerIndexer validates that the consumer indexer consumes block messages
// from Kafka and persists them to ClickHouse. It assumes Docker Compose has started
// Kafka and ClickHouse.
func TestE2EConsumerIndexer(t *testing.T) {
	// ---- Config ----
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	blockchainID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("blocks_consumer_test_%d", testID)
	groupID := fmt.Sprintf("e2e-consumerindexer-%d", testID)
	blocksTable := getEnvStr("BLOCKS_TABLE", "raw_blocks_e2e")
	transactionsTable := getEnvStr("TRANSACTIONS_TABLE", "raw_transactions_e2e")
	concurrency := int64(3)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	// ---- Prepare ClickHouse ----
	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err, "clickhouse connection failed (is docker-compose up?)")
	defer chClient.Close()

	// Initialize repositories (creates tables)
	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err, "failed to create blocks repository")

	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err, "failed to create transactions repository")

	logsTable := getEnvStr("LOGS_TABLE", "raw_logs_e2e")
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err, "failed to create logs repository")

	// Clean up any existing test data
	cleanupQuery := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", blocksTable)
	err = chClient.Conn().Exec(ctx, cleanupQuery)
	require.NoError(t, err, "failed to truncate blocks table")

	cleanupQuery = fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", transactionsTable)
	err = chClient.Conn().Exec(ctx, cleanupQuery)
	require.NoError(t, err, "failed to truncate transactions table")

	cleanupQuery = fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", logsTable)
	err = chClient.Conn().Exec(ctx, cleanupQuery)
	require.NoError(t, err, "failed to truncate logs table")

	// Verify tables are empty
	var count uint64
	err = chClient.Conn().QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", blocksTable)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, uint64(0), count, "blocks table should be empty after truncate")
	err = chClient.Conn().QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", transactionsTable)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, uint64(0), count, "transactions table should be empty after truncate")

	// ---- Create test blocks and produce to Kafka ----
	testBlocks := createTestBlocks(evmChainID, blockchainID, 5)
	produceBlocksToKafka(t, kafkaBrokers, kafkaTopic, testBlocks)

	// ---- Start consumer indexer components ----
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
	})
	require.NoError(t, err)

	proc := processor.NewCorethProcessor(log, blocksRepo, transactionsRepo, logsRepo, m)

	consumerCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     groupID,
		Topic:                       kafkaTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 concurrency,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                false,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
	}

	consumer, err := kafka.NewConsumer(ctx, log, consumerCfg, proc, nil)
	require.NoError(t, err, "failed to create consumer")

	// Start consumer in background
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return consumer.Start(gctx)
	})

	// Wait for messages to be processed and written to ClickHouse
	time.Sleep(10 * time.Second)

	// ---- Verify blocks are persisted in ClickHouse ----
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer verifyCancel()

	verifyBlocksInClickHouse(t, verifyCtx, chClient, blocksTable, testBlocks)
	verifyTransactionsInClickHouse(t, verifyCtx, chClient, transactionsTable, testBlocks)

	// ---- Stop consumer gracefully ----
	cancel()
	err = g.Wait()
	require.NoError(t, err, "consumer should shutdown gracefully")

	log.Info("Consumer indexer e2e test completed successfully")
}

// TestE2EConsumerIndexerWithDLQ tests consumer indexer with DLQ enabled
func TestE2EConsumerIndexerWithDLQ(t *testing.T) {
	// ---- Config ----
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	blockchainID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("blocks_consumer_dlq_test_%d", testID)
	dlqTopic := kafkaTopic + "_dlq"
	groupID := fmt.Sprintf("e2e-consumerindexer-dlq-%d", testID)
	blocksTable := getEnvStr("BLOCKS_TABLE", "raw_blocks_e2e_dlq")
	transactionsTable := getEnvStr("TRANSACTIONS_TABLE", "raw_transactions_e2e_dlq")

	// ---- Test context ----
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	// ---- Prepare ClickHouse ----
	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err, "clickhouse connection failed")
	defer chClient.Close()

	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err)

	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err)

	logsTable := getEnvStr("LOGS_TABLE", "raw_logs_e2e_dlq")
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err)

	// Clean up tables - ensure they're empty before starting
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", blocksTable))
	require.NoError(t, err, "failed to truncate blocks table")
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", transactionsTable))
	require.NoError(t, err, "failed to truncate transactions table")
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", logsTable))
	require.NoError(t, err, "failed to truncate logs table")

	// ---- Create test blocks (mix of valid and invalid) ----
	validBlocks := createTestBlocks(evmChainID, blockchainID, 3)

	// Produce valid blocks
	produceBlocksToKafka(t, kafkaBrokers, kafkaTopic, validBlocks)

	// Produce an invalid message that will cause processing to fail
	produceInvalidMessage(t, kafkaBrokers, kafkaTopic)

	// ---- Start consumer with DLQ enabled ----
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
	})
	require.NoError(t, err)

	proc := processor.NewCorethProcessor(log, blocksRepo, transactionsRepo, logsRepo, m)

	consumerCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     groupID,
		Topic:                       kafkaTopic,
		DLQTopic:                    dlqTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 2,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                true,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
	}

	consumer, err := kafka.NewConsumer(ctx, log, consumerCfg, proc, nil)
	require.NoError(t, err)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return consumer.Start(gctx)
	})

	// Wait for processing
	time.Sleep(10 * time.Second)

	// ---- Verify valid blocks are in ClickHouse ----
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer verifyCancel()

	verifyBlocksInClickHouse(t, verifyCtx, chClient, blocksTable, validBlocks)

	// ---- Verify invalid message is in DLQ ----
	verifyMessageInDLQ(t, verifyCtx, kafkaBrokers, dlqTopic)

	// Cleanup
	cancel()
	_ = g.Wait()

	log.Info("Consumer indexer DLQ e2e test completed successfully")
}

// createTestBlocks creates test block data
func createTestBlocks(evmChainID uint64, blockchainID string, count int) []messages.EVMBlock {
	blocks := make([]messages.EVMBlock, count)

	for i := 0; i < count; i++ {
		blockNum := uint64(1000 + i)

		// Create test transactions
		txs := make([]*messages.EVMTransaction, 2)
		for j := 0; j < 2; j++ {
			tx := &messages.EVMTransaction{
				Hash:     common.HexToHash(fmt.Sprintf("0x%064d", blockNum*txHashMultiplier+uint64(j))).Hex(),
				Nonce:    uint64(j),
				From:     common.HexToAddress(fmt.Sprintf("0x%040d", 1)).Hex(),
				To:       common.HexToAddress(fmt.Sprintf("0x%040d", 2)).Hex(),
				Value:    big.NewInt(int64(oneEtherInWei)),
				Gas:      21000,
				GasPrice: big.NewInt(25000000000),
				Input:    "0x",
				Type:     libevmtypes.LegacyTxType,
			}
			txs[j] = tx
		}

		bcID := blockchainID
		evmID := new(big.Int).SetUint64(evmChainID)
		now := uint64(time.Now().Unix())
		block := messages.EVMBlock{
			EVMChainID:       evmID,
			BlockchainID:     &bcID,
			Hash:             common.HexToHash(fmt.Sprintf("0x%064d", blockNum)).Hex(),
			ParentHash:       common.HexToHash(fmt.Sprintf("0x%064d", blockNum-1)).Hex(),
			Number:           new(big.Int).SetUint64(blockNum),
			Timestamp:        now,
			TimestampMs:      now * 1000,
			Nonce:            0,
			Difficulty:       big.NewInt(1),
			GasLimit:         8000000,
			GasUsed:          42000,
			Miner:            common.HexToAddress("0x0000000000000000000000000000000000000000").Hex(),
			MixHash:          common.Hash{}.Hex(),
			StateRoot:        common.HexToHash(fmt.Sprintf("0x%064d", blockNum+1000)).Hex(),
			TransactionsRoot: common.HexToHash(fmt.Sprintf("0x%064d", blockNum+2000)).Hex(),
			ReceiptsRoot:     common.HexToHash(fmt.Sprintf("0x%064d", blockNum+3000)).Hex(),
			UncleHash:        common.Hash{}.Hex(),
			LogsBloom:        "0x00",
			ExtraData:        "0x",
			BaseFee:          big.NewInt(25000000000),
			Transactions:     txs,
		}

		blocks[i] = block
	}

	return blocks
}

// produceBlocksToKafka produces test blocks to Kafka
func produceBlocksToKafka(t *testing.T, brokers, topic string, blocks []messages.EVMBlock) {
	t.Helper()

	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         "e2e-test-producer",
	})
	require.NoError(t, err)
	defer producer.Close()

	deliveryChan := make(chan ckafka.Event, len(blocks))

	for _, block := range blocks {
		data, err := json.Marshal(block)
		require.NoError(t, err, "failed to marshal block")

		msg := &ckafka.Message{
			TopicPartition: ckafka.TopicPartition{
				Topic:     &topic,
				Partition: ckafka.PartitionAny,
			},
			Key:   []byte(fmt.Sprintf("%d", block.Number.Uint64())),
			Value: data,
		}

		err = producer.Produce(msg, deliveryChan)
		require.NoError(t, err)
	}

	// Wait for delivery reports
	for i := 0; i < len(blocks); i++ {
		e := <-deliveryChan
		m := e.(*ckafka.Message)
		require.Nil(t, m.TopicPartition.Error, "delivery failed")
	}

	pending := producer.Flush(5000)
	require.Equal(t, 0, pending, "failed to flush producer")
	t.Logf("Produced %d blocks to Kafka topic %s", len(blocks), topic)
}

// produceInvalidMessage produces an invalid message to trigger DLQ
func produceInvalidMessage(t *testing.T, brokers, topic string) {
	t.Helper()

	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         "e2e-test-producer-invalid",
	})
	require.NoError(t, err)
	defer producer.Close()

	deliveryChan := make(chan ckafka.Event, 1)

	msg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: ckafka.PartitionAny,
		},
		Key:   []byte("invalid"),
		Value: []byte("this is not a valid protobuf message"),
	}

	err = producer.Produce(msg, deliveryChan)
	require.NoError(t, err)

	e := <-deliveryChan
	m := e.(*ckafka.Message)
	require.Nil(t, m.TopicPartition.Error)

	pending := producer.Flush(producerFlushTimeout)
	require.Equal(t, 0, pending, "failed to flush producer")
	t.Log("Produced invalid message to Kafka")
}

// verifyBlocksInClickHouse verifies blocks are written to ClickHouse
func verifyBlocksInClickHouse(t *testing.T, ctx context.Context, client clickhouse.Client, tableName string, expectedBlocks []messages.EVMBlock) {
	t.Helper()

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count uint64

	deadline := time.Now().Add(5 * time.Second)
	for {
		err := client.Conn().QueryRow(ctx, query).Scan(&count)
		if err == nil && count >= uint64(len(expectedBlocks)) {
			break
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "failed to query blocks count")
			require.GreaterOrEqual(t, count, uint64(len(expectedBlocks)), "not all blocks written to ClickHouse")
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Logf("Verified %d blocks in ClickHouse table %s", count, tableName)

	// Verify specific block data
	for _, expectedBlock := range expectedBlocks {
		query := fmt.Sprintf("SELECT lower(concat('0x', hex(hash))) as hash, block_number, gas_used, timestamp_ms FROM %s WHERE block_number = %d",
			tableName, expectedBlock.Number.Uint64())

		var hash string
		var blockNumber uint64
		var gasUsed uint64
		var timestampMs uint64

		err := client.Conn().QueryRow(ctx, query).Scan(&hash, &blockNumber, &gasUsed, &timestampMs)
		require.NoError(t, err, "block %d not found", expectedBlock.Number.Uint64())
		require.Equal(t, expectedBlock.Hash, hash, "hash mismatch for block %d", blockNumber)
		require.Equal(t, expectedBlock.Number.Uint64(), blockNumber, "block_number mismatch")
		require.Equal(t, expectedBlock.GasUsed, gasUsed, "gas_used mismatch for block %d", blockNumber)
		require.Equal(t, expectedBlock.TimestampMs, timestampMs, "timestamp_ms mismatch for block %d", blockNumber)
	}
}

// verifyTransactionsInClickHouse verifies transactions are written to ClickHouse
func verifyTransactionsInClickHouse(t *testing.T, ctx context.Context, client clickhouse.Client, tableName string, blocks []messages.EVMBlock) {
	t.Helper()

	expectedTxCount := 0
	for _, block := range blocks {
		expectedTxCount += len(block.Transactions)
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count uint64

	deadline := time.Now().Add(5 * time.Second)
	for {
		err := client.Conn().QueryRow(ctx, query).Scan(&count)
		if err == nil && count >= uint64(expectedTxCount) {
			break
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "failed to query transactions count")
			require.GreaterOrEqual(t, count, uint64(expectedTxCount), "not all transactions written")
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Logf("Verified %d transactions in ClickHouse table %s", count, tableName)
}

// verifyMessageInDLQ verifies that a message exists in the DLQ topic
func verifyMessageInDLQ(t *testing.T, ctx context.Context, brokers, dlqTopic string) {
	t.Helper()

	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          fmt.Sprintf("e2e-dlq-verifier-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.Subscribe(dlqTopic, nil)
	require.NoError(t, err)

	// Poll for messages in DLQ
	deadline := time.Now().Add(10 * time.Second)
	foundDLQMessage := false

	for time.Now().Before(deadline) {
		ev := consumer.Poll(1000)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *ckafka.Message:
			t.Logf("Found message in DLQ: key=%s", string(e.Key))
			t.Logf("Message: %s", string(e.Value))
			foundDLQMessage = true
			return
		case ckafka.Error:
			if !e.IsFatal() {
				continue
			}
			require.NoError(t, e, "fatal kafka error")
		}
	}

	require.True(t, foundDLQMessage, "expected to find message in DLQ topic")
}

// TestE2EConsumerIndexerConcurrency validates concurrent processing of messages
func TestE2EConsumerIndexerConcurrency(t *testing.T) {
	// ---- Config ----
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	blockchainID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("blocks_consumer_concurrent_test_%d", testID)
	groupID := fmt.Sprintf("e2e-consumerindexer-concurrent-%d", testID)
	blocksTable := getEnvStr("BLOCKS_TABLE", "raw_blocks_e2e_concurrent")
	transactionsTable := getEnvStr("TRANSACTIONS_TABLE", "raw_transactions_e2e_concurrent")
	concurrency := int64(15) // Higher concurrency for this test

	// ---- Test context ----
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	// ---- Prepare ClickHouse ----
	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err, "clickhouse connection failed")
	defer chClient.Close()

	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err)

	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err)

	logsTable := getEnvStr("LOGS_TABLE", "raw_logs_e2e_concurrent")
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err)

	// Clean up tables - ensure they're empty before starting
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", blocksTable))
	require.NoError(t, err, "failed to truncate blocks table")
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", transactionsTable))
	require.NoError(t, err, "failed to truncate transactions table")
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", logsTable))
	require.NoError(t, err, "failed to truncate logs table")

	// ---- Create a larger batch of test blocks ----
	testBlocks := createTestBlocks(evmChainID, blockchainID, 50)
	produceBlocksToKafka(t, kafkaBrokers, kafkaTopic, testBlocks)

	// ---- Start consumer with high concurrency ----
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
	})
	require.NoError(t, err)

	proc := processor.NewCorethProcessor(log, blocksRepo, transactionsRepo, logsRepo, m)

	consumerCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     groupID,
		Topic:                       kafkaTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 concurrency,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                false,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
	}

	consumer, err := kafka.NewConsumer(ctx, log, consumerCfg, proc, nil)
	require.NoError(t, err)

	// Track start time
	startTime := time.Now()

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return consumer.Start(gctx)
	})

	// Wait for all messages to be processed
	time.Sleep(15 * time.Second)

	processingTime := time.Since(startTime)

	// ---- Verify all blocks are persisted ----
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer verifyCancel()

	verifyBlocksInClickHouse(t, verifyCtx, chClient, blocksTable, testBlocks)
	verifyTransactionsInClickHouse(t, verifyCtx, chClient, transactionsTable, testBlocks)

	// ---- Verify no duplicate blocks (check uniqueness by hash) ----
	query := fmt.Sprintf("SELECT COUNT(DISTINCT hash) FROM %s", blocksTable)
	var distinctCount uint64
	err = chClient.Conn().QueryRow(verifyCtx, query).Scan(&distinctCount)
	require.NoError(t, err)
	require.Equal(t, uint64(len(testBlocks)), distinctCount, "duplicate blocks detected")

	// Cleanup
	cancel()
	_ = g.Wait()

	log.Infow("Concurrent consumer indexer test completed",
		"blocks", len(testBlocks),
		"concurrency", concurrency,
		"processing_time", processingTime)
}

// TestE2EConsumerIndexerOffsetManagement validates offset commit and recovery
func TestE2EConsumerIndexerOffsetManagement(t *testing.T) {
	// ---- Config ----
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	blockchainID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("blocks_consumer_offset_test_%d", testID)
	groupID := fmt.Sprintf("e2e-consumerindexer-offset-%d", testID)
	blocksTable := getEnvStr("BLOCKS_TABLE", "raw_blocks_e2e_offset")
	transactionsTable := getEnvStr("TRANSACTIONS_TABLE", "raw_transactions_e2e_offset")

	// ---- Test context ----
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	// ---- Prepare ClickHouse ----
	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err)
	defer chClient.Close()

	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err)

	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err)

	logsTable := getEnvStr("LOGS_TABLE", "raw_logs_e2e_offset")
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err)

	// Clean up tables - ensure they're empty before starting
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", blocksTable))
	require.NoError(t, err, "failed to truncate blocks table")
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", transactionsTable))
	require.NoError(t, err, "failed to truncate transactions table")
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", logsTable))
	require.NoError(t, err, "failed to truncate logs table")

	// ---- Phase 1: Produce first batch and process ----
	// Get initial Kafka offset (high watermark BEFORE producing any messages)
	// This captures the baseline offset (should be 0 for a new topic)
	initialOffsets, err := getTopicOffsets(t, kafkaBrokers, kafkaTopic)
	if err != nil {
		// Topic doesn't exist yet, so all offsets start at 0
		initialOffsets = make(map[int32]int64)
		t.Logf("Topic doesn't exist yet, initialOffsets will be populated after first message")
	} else {
		t.Logf("Initial Kafka offsets (before producing): %v", initialOffsets)
	}

	firstBatch := createTestBlocks(evmChainID, blockchainID, 5)
	produceBlocksToKafka(t, kafkaBrokers, kafkaTopic, firstBatch)

	// Update initialOffsets if it was empty (topic was just created)
	if len(initialOffsets) == 0 {
		// For a newly created topic, the initial offset is 0 for all partitions
		// We need to discover how many partitions were created
		currentOffsets, err := getTopicOffsets(t, kafkaBrokers, kafkaTopic)
		require.NoError(t, err)
		for partition := range currentOffsets {
			initialOffsets[partition] = 0
		}
		t.Logf("Initial Kafka offsets (after topic creation): %v", initialOffsets)
	}

	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
	})
	require.NoError(t, err)

	proc := processor.NewCorethProcessor(log, blocksRepo, transactionsRepo, logsRepo, m)

	consumerCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     groupID, // Same group ID for both runs
		Topic:                       kafkaTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 3,
		OffsetManagerCommitInterval: 1 * time.Second, // Frequent commits
		PublishToDLQ:                false,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
	}

	consumer1, err := kafka.NewConsumer(ctx, log, consumerCfg, proc, nil)
	require.NoError(t, err)

	g1, gctx1 := errgroup.WithContext(ctx)
	g1.Go(func() error {
		return consumer1.Start(gctx1)
	})

	// Wait for first batch to be processed
	time.Sleep(8 * time.Second)

	// Stop first consumer
	cancel()
	_ = g1.Wait()

	// Get committed offset after consumer1 stops
	committedOffsets, err := getCommittedOffsets(t, kafkaBrokers, groupID, kafkaTopic)
	require.NoError(t, err)
	t.Logf("Committed offsets after consumer1: %v", committedOffsets)

	// Get current Kafka offsets (high watermark)
	currentOffsets, err := getTopicOffsets(t, kafkaBrokers, kafkaTopic)
	require.NoError(t, err)
	t.Logf("Current Kafka offsets after consumer1: %v", currentOffsets)

	// Calculate total messages available in Kafka for each partition
	var totalMsgsInKafka int64
	for partition := range currentOffsets {
		if initial, ok := initialOffsets[partition]; ok {
			msgsInPartition := currentOffsets[partition] - initial
			totalMsgsInKafka += msgsInPartition
			t.Logf("Partition %d: initial=%d, current=%d, messages=%d", partition, initial, currentOffsets[partition], msgsInPartition)
		}
	}
	t.Logf("Total messages in Kafka: %d", totalMsgsInKafka)

	// Verify first batch is in ClickHouse
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	verifyBlocksInClickHouse(t, verifyCtx, chClient, blocksTable, firstBatch)

	// Count actual rows in ClickHouse
	var rowsInClickHouse uint64
	err = chClient.Conn().QueryRow(verifyCtx, fmt.Sprintf("SELECT COUNT(*) FROM %s", blocksTable)).Scan(&rowsInClickHouse)
	require.NoError(t, err)
	t.Logf("Rows in ClickHouse after consumer1: %d", rowsInClickHouse)
	verifyCancel()

	// Verify: (messages in Kafka - consumed messages) <= rows in ClickHouse
	// This accounts for any in-flight messages or processing lag
	var totalCommitted int64
	for partition := range committedOffsets {
		if initial, ok := initialOffsets[partition]; ok {
			consumed := committedOffsets[partition] - initial
			totalCommitted += consumed
			t.Logf("Partition %d: consumed=%d messages", partition, consumed)
		}
	}
	t.Logf("Total committed messages: %d", totalCommitted)

	// The difference between Kafka messages and committed should be <= rows in ClickHouse
	// (allowing for potential duplicates or retry logic)
	require.GreaterOrEqual(t, rowsInClickHouse, uint64(totalCommitted),
		"ClickHouse rows should be >= committed messages (first batch)")

	log.Info("Phase 1 completed - first batch processed")

	// ---- Phase 2: Produce second batch and restart consumer ----
	secondBatch := createTestBlocksStartingFrom(evmChainID, blockchainID, 1005, 5)
	produceBlocksToKafka(t, kafkaBrokers, kafkaTopic, secondBatch)

	// Create new context for second consumer
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()

	// Start second consumer with same group ID (should resume from committed offset)
	consumer2, err := kafka.NewConsumer(ctx2, log, consumerCfg, proc, nil)
	require.NoError(t, err)

	g2, gctx2 := errgroup.WithContext(ctx2)
	g2.Go(func() error {
		return consumer2.Start(gctx2)
	})

	// Wait for second batch to be processed
	time.Sleep(30 * time.Second)

	// Stop second consumer
	cancel2()
	_ = g2.Wait()

	// Get committed offset after consumer2 stops
	committedOffsets2, err := getCommittedOffsets(t, kafkaBrokers, groupID, kafkaTopic)
	require.NoError(t, err)
	t.Logf("Committed offsets after consumer2: %v", committedOffsets2)

	// Get final Kafka offsets
	finalOffsets, err := getTopicOffsets(t, kafkaBrokers, kafkaTopic)
	require.NoError(t, err)
	t.Logf("Final Kafka offsets: %v", finalOffsets)

	// Verify second batch is also in ClickHouse (total should be 10)
	verifyCtx2, verifyCancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer verifyCancel2()

	allBlocks := append(firstBatch, secondBatch...)
	verifyBlocksInClickHouse(t, verifyCtx2, chClient, blocksTable, allBlocks)

	// Verify total count
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", blocksTable)
	var totalCount uint64
	err = chClient.Conn().QueryRow(verifyCtx2, query).Scan(&totalCount)
	require.NoError(t, err)
	require.Equal(t, uint64(len(allBlocks)), totalCount, "unexpected total block count")

	// Calculate total messages consumed across both phases
	var totalConsumed int64
	for partition := range committedOffsets2 {
		if initial, ok := initialOffsets[partition]; ok {
			consumed := committedOffsets2[partition] - initial
			totalConsumed += consumed
			t.Logf("Partition %d: total consumed=%d messages", partition, consumed)
		}
	}
	t.Logf("Total consumed messages (both phases): %d", totalConsumed)
	t.Logf("Total blocks in ClickHouse: %d", totalCount)

	// Verify: consumed messages should match rows in ClickHouse
	require.Equal(t, uint64(totalConsumed), totalCount,
		"Total consumed messages should equal rows in ClickHouse")

	log.Info("Offset management test completed - consumer resumed from last committed offset")
}

// TestE2EConsumerIndexerLargePayload validates handling of blocks with many transactions
func TestE2EConsumerIndexerLargePayload(t *testing.T) {
	// ---- Config ----
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	blockchainID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	kafkaTopic := fmt.Sprintf("blocks_consumer_large_test_%d", testID)
	groupID := fmt.Sprintf("e2e-consumerindexer-large-%d", testID)
	blocksTable := getEnvStr("BLOCKS_TABLE", "raw_blocks_e2e_large")
	transactionsTable := getEnvStr("TRANSACTIONS_TABLE", "raw_transactions_e2e_large")

	// ---- Test context ----
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	// ---- Prepare ClickHouse ----
	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err)
	defer chClient.Close()

	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err)

	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err)

	logsTable := getEnvStr("LOGS_TABLE", "raw_logs_e2e_large")
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err)

	// Clean up tables - ensure they're empty before starting
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", blocksTable))
	require.NoError(t, err, "failed to truncate blocks table")
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", transactionsTable))
	require.NoError(t, err, "failed to truncate transactions table")
	err = chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", logsTable))
	require.NoError(t, err, "failed to truncate logs table")

	// ---- Create blocks with many transactions ----
	testBlocks := createTestBlocksWithTransactions(evmChainID, blockchainID, 3, 50) // 3 blocks, 50 txs each
	produceBlocksToKafka(t, kafkaBrokers, kafkaTopic, testBlocks)

	// ---- Start consumer ----
	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
	})
	require.NoError(t, err)

	proc := processor.NewCorethProcessor(log, blocksRepo, transactionsRepo, logsRepo, m)

	consumerCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     groupID,
		Topic:                       kafkaTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 3,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                false,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
	}

	consumer, err := kafka.NewConsumer(ctx, log, consumerCfg, proc, nil)
	require.NoError(t, err)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return consumer.Start(gctx)
	})

	// Wait for processing
	time.Sleep(15 * time.Second)

	// ---- Verify all blocks and transactions ----
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer verifyCancel()

	verifyBlocksInClickHouse(t, verifyCtx, chClient, blocksTable, testBlocks)
	verifyTransactionsInClickHouse(t, verifyCtx, chClient, transactionsTable, testBlocks)

	// Verify exact transaction count
	expectedTxCount := 0
	for _, block := range testBlocks {
		expectedTxCount += len(block.Transactions)
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", transactionsTable)
	var actualTxCount uint64
	err = chClient.Conn().QueryRow(verifyCtx, query).Scan(&actualTxCount)
	require.NoError(t, err)
	require.Equal(t, uint64(expectedTxCount), actualTxCount, "transaction count mismatch")

	// Cleanup
	cancel()
	_ = g.Wait()

	log.Infow("Large payload test completed",
		"blocks", len(testBlocks),
		"transactions", expectedTxCount)
}

// createTestBlocksStartingFrom creates test blocks starting from a specific number
func createTestBlocksStartingFrom(evmChainID uint64, blockchainID string, startNum uint64, count int) []messages.EVMBlock {
	blocks := make([]messages.EVMBlock, count)

	for i := 0; i < count; i++ {
		blockNum := startNum + uint64(i)

		// Create test transactions
		txs := make([]*messages.EVMTransaction, 2)
		for j := 0; j < 2; j++ {
			tx := &messages.EVMTransaction{
				Hash:     common.HexToHash(fmt.Sprintf("0x%064d", blockNum*txHashMultiplier+uint64(j))).Hex(),
				Nonce:    uint64(j),
				From:     common.HexToAddress(fmt.Sprintf("0x%040d", 1)).Hex(),
				To:       common.HexToAddress(fmt.Sprintf("0x%040d", 2)).Hex(),
				Value:    big.NewInt(int64(oneEtherInWei)),
				Gas:      21000,
				GasPrice: big.NewInt(25000000000),
				Input:    "0x",
				Type:     libevmtypes.LegacyTxType,
			}
			txs[j] = tx
		}

		bcID := blockchainID
		evmID := new(big.Int).SetUint64(evmChainID)
		now := uint64(time.Now().Unix())
		block := messages.EVMBlock{
			EVMChainID:       evmID,
			BlockchainID:     &bcID,
			Hash:             common.HexToHash(fmt.Sprintf("0x%064d", blockNum)).Hex(),
			ParentHash:       common.HexToHash(fmt.Sprintf("0x%064d", blockNum-1)).Hex(),
			Number:           new(big.Int).SetUint64(blockNum),
			Timestamp:        now,
			TimestampMs:      now * 1000,
			Nonce:            0,
			Difficulty:       big.NewInt(1),
			GasLimit:         8000000,
			GasUsed:          42000,
			Miner:            common.HexToAddress("0x0000000000000000000000000000000000000000").Hex(),
			MixHash:          common.Hash{}.Hex(),
			StateRoot:        common.HexToHash(fmt.Sprintf("0x%064d", blockNum+1000)).Hex(),
			TransactionsRoot: common.HexToHash(fmt.Sprintf("0x%064d", blockNum+2000)).Hex(),
			ReceiptsRoot:     common.HexToHash(fmt.Sprintf("0x%064d", blockNum+3000)).Hex(),
			UncleHash:        common.Hash{}.Hex(),
			LogsBloom:        "0x00",
			ExtraData:        "0x",
			BaseFee:          big.NewInt(25000000000),
			Transactions:     txs,
		}

		blocks[i] = block
	}

	return blocks
}

// createTestBlocksWithTransactions creates test blocks with a specified number of transactions
func createTestBlocksWithTransactions(evmChainID uint64, blockchainID string, blockCount, txPerBlock int) []messages.EVMBlock {
	blocks := make([]messages.EVMBlock, blockCount)

	for i := 0; i < blockCount; i++ {
		blockNum := uint64(2000 + i)

		// Create many test transactions
		txs := make([]*messages.EVMTransaction, txPerBlock)
		for j := 0; j < txPerBlock; j++ {
			tx := &messages.EVMTransaction{
				Hash:     common.HexToHash(fmt.Sprintf("0x%064d", blockNum*txHashMultiplierLarge+uint64(j))).Hex(),
				Nonce:    uint64(j),
				From:     common.HexToAddress(fmt.Sprintf("0x%040d", j%10+1)).Hex(),
				To:       common.HexToAddress(fmt.Sprintf("0x%040d", j%10+2)).Hex(),
				Value:    big.NewInt(int64(oneEtherInWei)),
				Gas:      21000,
				GasPrice: big.NewInt(25000000000),
				Input:    "0x",
				Type:     libevmtypes.LegacyTxType,
			}
			txs[j] = tx
		}

		bcID := blockchainID
		evmID := new(big.Int).SetUint64(evmChainID)
		now := uint64(time.Now().Unix())
		block := messages.EVMBlock{
			EVMChainID:       evmID,
			BlockchainID:     &bcID,
			Hash:             common.HexToHash(fmt.Sprintf("0x%064d", blockNum)).Hex(),
			ParentHash:       common.HexToHash(fmt.Sprintf("0x%064d", blockNum-1)).Hex(),
			Number:           new(big.Int).SetUint64(blockNum),
			Timestamp:        now,
			TimestampMs:      now * 1000,
			Nonce:            0,
			Difficulty:       big.NewInt(1),
			GasLimit:         8000000,
			GasUsed:          uint64(21000 * txPerBlock),
			Miner:            common.HexToAddress("0x0000000000000000000000000000000000000000").Hex(),
			MixHash:          common.Hash{}.Hex(),
			StateRoot:        common.HexToHash(fmt.Sprintf("0x%064d", blockNum+1000)).Hex(),
			TransactionsRoot: common.HexToHash(fmt.Sprintf("0x%064d", blockNum+2000)).Hex(),
			ReceiptsRoot:     common.HexToHash(fmt.Sprintf("0x%064d", blockNum+3000)).Hex(),
			UncleHash:        common.Hash{}.Hex(),
			LogsBloom:        "0x00",
			ExtraData:        "0x",
			BaseFee:          big.NewInt(25000000000),
			Transactions:     txs,
		}

		blocks[i] = block
	}

	return blocks
}

// getTopicOffsets gets the high watermark (latest offset) for each partition of a topic
func getTopicOffsets(t *testing.T, brokers, topic string) (map[int32]int64, error) {
	t.Helper()

	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          fmt.Sprintf("offset-reader-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	// Get metadata for the topic
	metadata, err := consumer.GetMetadata(&topic, false, 5000)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found in metadata", topic)
	}

	offsets := make(map[int32]int64)
	for _, partition := range topicMetadata.Partitions {
		partitionID := partition.ID

		// Query high watermark (latest offset)
		low, high, err := consumer.QueryWatermarkOffsets(topic, partitionID, 5000)
		if err != nil {
			t.Logf("Warning: failed to query watermark for partition %d: %v", partitionID, err)
			continue
		}

		offsets[partitionID] = high
		t.Logf("Topic %s, Partition %d: low=%d, high=%d", topic, partitionID, low, high)
	}

	return offsets, nil
}

// getCommittedOffsets gets the committed offset for a consumer group on a topic
func getCommittedOffsets(t *testing.T, brokers, groupID, topic string) (map[int32]int64, error) {
	t.Helper()

	// Create a temporary consumer to query committed offsets
	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	// Get metadata for the topic to discover partitions
	metadata, err := consumer.GetMetadata(&topic, false, 5000)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found in metadata", topic)
	}

	// Build list of partitions
	var partitions []ckafka.TopicPartition
	for _, partition := range topicMetadata.Partitions {
		partitions = append(partitions, ckafka.TopicPartition{
			Topic:     &topic,
			Partition: partition.ID,
		})
	}

	// Get committed offsets
	committed, err := consumer.Committed(partitions, 5000)
	if err != nil {
		return nil, fmt.Errorf("failed to get committed offsets: %w", err)
	}

	offsets := make(map[int32]int64)
	for _, tp := range committed {
		if tp.Error != nil {
			t.Logf("Warning: error getting committed offset for partition %d: %v", tp.Partition, tp.Error)
			continue
		}
		offsets[tp.Partition] = int64(tp.Offset)
		t.Logf("Consumer group %s, Topic %s, Partition %d: committed offset=%d",
			groupID, topic, tp.Partition, tp.Offset)
	}

	return offsets, nil
}

// TestE2EConsumerIndexerDLQConsumerPipeline validates the full DLQ consumer goroutine:
// primary consumer fails on invalid messages and publishes to DLQ, then a DLQ consumer
// running in the same errgroup picks up from the DLQ topic and processes valid messages
// that were directly produced to it.
func TestE2EConsumerIndexerDLQConsumerPipeline(t *testing.T) {
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	blockchainID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	primaryTopic := fmt.Sprintf("blocks_dlq_pipeline_test_%d", testID)
	dlqTopic := primaryTopic + "_dlq"
	primaryGroupID := fmt.Sprintf("e2e-pipeline-primary-%d", testID)
	dlqGroupID := fmt.Sprintf("e2e-pipeline-dlq-%d", testID)
	blocksTable := "raw_blocks_e2e_pipeline"
	transactionsTable := "raw_transactions_e2e_pipeline"
	logsTable := "raw_logs_e2e_pipeline"

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err)
	defer chClient.Close()

	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err)
	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err)
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err)

	for _, tbl := range []string{blocksTable, transactionsTable, logsTable} {
		require.NoError(t, chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", tbl)))
	}

	// ---- Metrics: primary + DLQ with role labels on same registry ----
	registry := prometheus.NewRegistry()
	primaryLabels := metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
		Role:          metrics.RolePrimaryConsumer,
	}
	primaryMetrics, err := metrics.NewWithLabels(registry, primaryLabels)
	require.NoError(t, err)

	dlqLabels := metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
		Role:          metrics.RoleDLQConsumer,
	}
	dlqMetrics, err := metrics.NewWithLabels(registry, dlqLabels)
	require.NoError(t, err)

	// ---- Phase 1: Primary consumer with DLQ publishing ----
	primaryProc := processor.NewCorethProcessor(log.Named(RolePrimaryConsumer), blocksRepo, transactionsRepo, logsRepo, primaryMetrics)

	primaryCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     primaryGroupID,
		Topic:                       primaryTopic,
		DLQTopic:                    dlqTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 2,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                true,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
	}

	primaryConsumer, err := kafka.NewConsumer(ctx, log.Named(RolePrimaryConsumer), primaryCfg, primaryProc, primaryMetrics)
	require.NoError(t, err)

	// Produce valid blocks to primary topic + one invalid message
	validBlocks := createTestBlocks(evmChainID, blockchainID, 3)
	produceBlocksToKafka(t, kafkaBrokers, primaryTopic, validBlocks)
	produceInvalidMessage(t, kafkaBrokers, primaryTopic)

	// ---- Phase 2: DLQ consumer reading directly from DLQ topic ----
	// Produce valid blocks directly to the DLQ topic to simulate messages
	// that were published to DLQ and now need reprocessing.
	dlqBlocks := createTestBlocksStartingFrom(evmChainID, blockchainID, 2000, 2)
	produceBlocksToKafka(t, kafkaBrokers, dlqTopic, dlqBlocks)

	dlqProc := processor.NewCorethProcessor(log.Named(RoleDLQConsumer), blocksRepo, transactionsRepo, logsRepo, dlqMetrics)

	dlqCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     dlqGroupID,
		Topic:                       dlqTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 1,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                false,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
		Retry: kafka.RetryPolicy{
			MaxRetries: kafka.InfiniteRetries,
			BaseDelay:  500 * time.Millisecond,
			MaxDelay:   2 * time.Second,
		},
	}

	dlqConsumer, err := kafka.NewConsumer(ctx, log.Named(RoleDLQConsumer), dlqCfg, dlqProc, dlqMetrics)
	require.NoError(t, err)

	// ---- Run both consumers in an errgroup (mirrors run.go pattern) ----
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return primaryConsumer.Start(gctx)
	})

	g.Go(func() error {
		return dlqConsumer.Start(gctx)
	})

	// Wait until ClickHouse contains blocks from both primary and DLQ consumers.
	expectedTotal := uint64(len(validBlocks) + len(dlqBlocks))
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", blocksTable)
	require.Eventually(t, func() bool {
		var count uint64
		if err := chClient.Conn().QueryRow(ctx, countQuery).Scan(&count); err != nil {
			return false
		}
		return count >= expectedTotal
	}, 30*time.Second, 500*time.Millisecond, "ClickHouse should contain blocks from both primary and DLQ consumers")

	// Verify invalid message was published to DLQ by primary.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer verifyCancel()
	verifyMessageInDLQ(t, verifyCtx, kafkaBrokers, dlqTopic)

	cancel()
	err = g.Wait()
	// The DLQ consumer is expected to fail: the primary forwards the invalid
	// message to the DLQ topic, and the DLQ consumer (PublishToDLQ=false)
	// correctly stops on the NonRetryable unmarshal error.
	if err != nil {
		require.ErrorIs(t, err, processor.ErrUnmarshalBlock,
			"DLQ consumer should fail with ErrUnmarshalBlock from the forwarded invalid message")
	}

	t.Logf("DLQ consumer pipeline e2e test completed: %d primary blocks + %d DLQ blocks = %d total",
		len(validBlocks), len(dlqBlocks), expectedTotal)
}

// TestE2EConsumerIndexerMetricsIsolation verifies that primary and DLQ consumers
// register metrics with distinct role labels on the same Prometheus registry.
func TestE2EConsumerIndexerMetricsIsolation(t *testing.T) {
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	blockchainID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	primaryTopic := fmt.Sprintf("blocks_metrics_test_%d", testID)
	dlqTopic := primaryTopic + "_dlq"
	blocksTable := "raw_blocks_e2e_metrics"
	transactionsTable := "raw_transactions_e2e_metrics"
	logsTable := "raw_logs_e2e_metrics"

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err)
	defer chClient.Close()

	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err)
	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err)
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err)

	for _, tbl := range []string{blocksTable, transactionsTable, logsTable} {
		require.NoError(t, chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", tbl)))
	}

	registry := prometheus.NewRegistry()

	primaryMetrics, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
		Role:          metrics.RolePrimaryConsumer,
	})
	require.NoError(t, err)

	dlqMetrics, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
		Role:          metrics.RoleDLQConsumer,
	})
	require.NoError(t, err)

	primaryProc := processor.NewCorethProcessor(log.Named(RolePrimaryConsumer), blocksRepo, transactionsRepo, logsRepo, primaryMetrics)
	primaryCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     fmt.Sprintf("e2e-metrics-primary-%d", testID),
		Topic:                       primaryTopic,
		DLQTopic:                    dlqTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 2,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                true,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
	}

	primaryConsumer, err := kafka.NewConsumer(ctx, log.Named(RolePrimaryConsumer), primaryCfg, primaryProc, primaryMetrics)
	require.NoError(t, err)

	dlqProc := processor.NewCorethProcessor(log.Named(RoleDLQConsumer), blocksRepo, transactionsRepo, logsRepo, dlqMetrics)
	dlqCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     fmt.Sprintf("e2e-metrics-dlq-%d", testID),
		Topic:                       dlqTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 1,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                false,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
		Retry: kafka.RetryPolicy{
			MaxRetries: kafka.InfiniteRetries,
			BaseDelay:  200 * time.Millisecond,
			MaxDelay:   1 * time.Second,
		},
	}

	dlqConsumer, err := kafka.NewConsumer(ctx, log.Named(RoleDLQConsumer), dlqCfg, dlqProc, dlqMetrics)
	require.NoError(t, err)

	validBlocks := createTestBlocks(evmChainID, blockchainID, 3)
	produceBlocksToKafka(t, kafkaBrokers, primaryTopic, validBlocks)

	dlqBlocks := createTestBlocksStartingFrom(evmChainID, blockchainID, 3000, 2)
	produceBlocksToKafka(t, kafkaBrokers, dlqTopic, dlqBlocks)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return primaryConsumer.Start(gctx) })
	g.Go(func() error { return dlqConsumer.Start(gctx) })

	// ---- Verify metrics have distinct role labels ----
	foundPrimaryRole := false
	foundDLQRole := false
	require.Eventually(t, func() bool {
		families, err := registry.Gather()
		if err != nil {
			return false
		}
		foundPrimaryRole = false
		foundDLQRole = false
		for _, mf := range families {
			if mf.GetName() != "indexer_consumer_messages_received_total" {
				continue
			}
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "role" {
						switch lp.GetValue() {
						case metrics.RolePrimaryConsumer:
							foundPrimaryRole = true
						case metrics.RoleDLQConsumer:
							foundDLQRole = true
						}
					}
				}
			}
		}
		return foundPrimaryRole && foundDLQRole
	}, 30*time.Second, 500*time.Millisecond, "Expected both primary_consumer and dlq_consumer role labels in metrics")

	cancel()
	require.NoError(t, g.Wait())

	t.Log("Metrics isolation e2e test completed: both role labels present on same registry")
}

// TestE2EConsumerIndexerErrgroupShutdown validates that when the primary consumer
// encounters a fatal error, the errgroup cancels the shared context, causing the
// DLQ consumer to shut down cleanly.
func TestE2EConsumerIndexerErrgroupShutdown(t *testing.T) {
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	primaryTopic := fmt.Sprintf("blocks_errgroup_test_%d", testID)
	dlqTopic := primaryTopic + "_dlq"
	blocksTable := "raw_blocks_e2e_errgroup"
	transactionsTable := "raw_transactions_e2e_errgroup"
	logsTable := "raw_logs_e2e_errgroup"

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err)
	defer chClient.Close()

	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err)
	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err)
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err)

	for _, tbl := range []string{blocksTable, transactionsTable, logsTable} {
		require.NoError(t, chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", tbl)))
	}

	registry := prometheus.NewRegistry()
	primaryMetrics, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
		Role:          metrics.RolePrimaryConsumer,
	})
	require.NoError(t, err)

	dlqMetrics, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
		Role:          metrics.RoleDLQConsumer,
	})
	require.NoError(t, err)

	// Primary consumer: PublishToDLQ=false so processing failures go to errCh -> loopErr -> Start() returns error
	primaryProc := processor.NewCorethProcessor(log.Named(RolePrimaryConsumer), blocksRepo, transactionsRepo, logsRepo, primaryMetrics)
	primaryCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     fmt.Sprintf("e2e-errgroup-primary-%d", testID),
		Topic:                       primaryTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 1,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                false,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
	}

	primaryConsumer, err := kafka.NewConsumer(ctx, log.Named(RolePrimaryConsumer), primaryCfg, primaryProc, primaryMetrics)
	require.NoError(t, err)

	// DLQ consumer: no messages to process, just waiting
	dlqProc := processor.NewCorethProcessor(log.Named(RoleDLQConsumer), blocksRepo, transactionsRepo, logsRepo, dlqMetrics)
	dlqCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     fmt.Sprintf("e2e-errgroup-dlq-%d", testID),
		Topic:                       dlqTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 1,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                false,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
		Retry: kafka.RetryPolicy{
			MaxRetries: kafka.InfiniteRetries,
			BaseDelay:  1 * time.Second,
			MaxDelay:   5 * time.Second,
		},
	}

	dlqConsumer, err := kafka.NewConsumer(ctx, log.Named(RoleDLQConsumer), dlqCfg, dlqProc, dlqMetrics)
	require.NoError(t, err)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := primaryConsumer.Start(gctx); err != nil {
			log.Errorw("primary consumer exited with error", "error", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		if err := dlqConsumer.Start(gctx); err != nil {
			log.Errorw("DLQ consumer exited with error", "error", err)
			return err
		}
		return nil
	})

	// Produce an invalid message to trigger a processing error on the primary.
	// With PublishToDLQ=false, the error goes to errCh -> loopErr -> Start() returns error.
	produceInvalidMessage(t, kafkaBrokers, primaryTopic)

	startTime := time.Now()
	err = g.Wait()
	elapsed := time.Since(startTime)

	require.Error(t, err, "errgroup should return the primary consumer's processing error")
	t.Logf("errgroup returned error in %v: %v", elapsed, err)

	require.Less(t, elapsed, 30*time.Second,
		"Both consumers should shut down within a reasonable time after the error")

	t.Log("Errgroup shutdown e2e test completed: primary error propagated, DLQ consumer shut down cleanly")
}

// TestE2EConsumerIndexerNonRetryableBypassesRetries validates that NonRetryable
// errors (e.g. malformed JSON) bypass the retry loop, route to DLQ immediately,
// and allow the consumer to continue processing subsequent valid messages.
// A high retry policy (10 retries × 5 s base delay) is configured so that if
// retries were NOT bypassed, the test would take > 150 s and time out.
func TestE2EConsumerIndexerNonRetryableBypassesRetries(t *testing.T) {
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	blockchainID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	primaryTopic := fmt.Sprintf("blocks_nonretryable_test_%d", testID)
	dlqTopic := primaryTopic + "_dlq"
	groupID := fmt.Sprintf("e2e-nonretryable-%d", testID)
	blocksTable := "raw_blocks_e2e_nonretryable"
	transactionsTable := "raw_transactions_e2e_nonretryable"
	logsTable := "raw_logs_e2e_nonretryable"

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err)
	defer chClient.Close()

	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err)
	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err)
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err)

	for _, tbl := range []string{blocksTable, transactionsTable, logsTable} {
		require.NoError(t, chClient.Conn().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", tbl)))
	}

	// Produce: [valid×2, invalid×3, valid×2] — 7 messages total.
	allBlocks := createTestBlocks(evmChainID, blockchainID, 4)
	produceBlocksToKafka(t, kafkaBrokers, primaryTopic, allBlocks[:2])
	for i := 0; i < 3; i++ {
		produceInvalidMessage(t, kafkaBrokers, primaryTopic)
	}
	produceBlocksToKafka(t, kafkaBrokers, primaryTopic, allBlocks[2:])

	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
		Role:          metrics.RolePrimaryConsumer,
	})
	require.NoError(t, err)

	proc := processor.NewCorethProcessor(log, blocksRepo, transactionsRepo, logsRepo, m)

	consumerCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     groupID,
		Topic:                       primaryTopic,
		DLQTopic:                    dlqTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 2,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                true,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
		Retry: kafka.RetryPolicy{
			MaxRetries: 10,
			BaseDelay:  5 * time.Second,
			MaxDelay:   10 * time.Second,
		},
	}

	consumer, err := kafka.NewConsumer(ctx, log, consumerCfg, proc, nil)
	require.NoError(t, err)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return consumer.Start(gctx)
	})

	// Wait for all 4 valid blocks to land in ClickHouse.
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", blocksTable)
	require.Eventually(t, func() bool {
		var count uint64
		if err := chClient.Conn().QueryRow(ctx, countQuery).Scan(&count); err != nil {
			return false
		}
		return count >= uint64(len(allBlocks))
	}, 30*time.Second, 500*time.Millisecond,
		"all 4 valid blocks should be in ClickHouse (NonRetryable must not block the consumer)")

	verifyBlocksInClickHouse(t, ctx, chClient, blocksTable, allBlocks)

	// Verify that all 3 invalid messages were routed to DLQ.
	dlqCount := countMessagesInTopic(t, kafkaBrokers, dlqTopic, 10*time.Second)
	require.Equal(t, 3, dlqCount, "all 3 invalid messages should be in the DLQ")

	cancel()
	require.NoError(t, g.Wait())

	log.Infow("NonRetryable bypass test completed",
		"valid_blocks", len(allBlocks),
		"dlq_messages", dlqCount)
}

// TestE2EConsumerIndexerFatalStopsConsumer validates that a Fatal error
// (e.g. ClickHouse UNKNOWN_TABLE) stops the consumer immediately, bypasses
// both retries and DLQ publishing, and propagates through the errgroup.
func TestE2EConsumerIndexerFatalStopsConsumer(t *testing.T) {
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113))
	blockchainID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	testID := time.Now().UnixNano()
	primaryTopic := fmt.Sprintf("blocks_fatal_test_%d", testID)
	dlqTopic := primaryTopic + "_dlq"
	groupID := fmt.Sprintf("e2e-fatal-%d", testID)
	blocksTable := fmt.Sprintf("raw_blocks_e2e_fatal_%d", testID)
	transactionsTable := fmt.Sprintf("raw_transactions_e2e_fatal_%d", testID)
	logsTable := fmt.Sprintf("raw_logs_e2e_fatal_%d", testID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err)
	defer chClient.Close()

	// Create repos (this auto-creates the tables via migrations).
	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", blocksTable)
	require.NoError(t, err)
	transactionsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", transactionsTable)
	require.NoError(t, err)
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", logsTable)
	require.NoError(t, err)

	// DROP the blocks table so WriteBlock returns UNKNOWN_TABLE (code 60) → Fatal.
	require.NoError(t, chClient.Conn().Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", blocksTable)))

	// Produce a valid block — it will parse successfully but fail at the
	// ClickHouse write step, triggering the Fatal classification.
	validBlocks := createTestBlocks(evmChainID, blockchainID, 1)
	produceBlocksToKafka(t, kafkaBrokers, primaryTopic, validBlocks)

	registry := prometheus.NewRegistry()
	m, err := metrics.NewWithLabels(registry, metrics.Labels{
		EVMChainID:    evmChainID,
		Environment:   "test",
		Region:        "local",
		CloudProvider: "local",
		Role:          metrics.RolePrimaryConsumer,
	})
	require.NoError(t, err)

	proc := processor.NewCorethProcessor(log, blocksRepo, transactionsRepo, logsRepo, m)

	consumerCfg := kafka.ConsumerConfig{
		BootstrapServers:            kafkaBrokers,
		GroupID:                     groupID,
		Topic:                       primaryTopic,
		DLQTopic:                    dlqTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 1,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                true,
		EnableLogs:                  false,
		SessionTimeout:              durationPtr(10 * time.Second),
		MaxPollInterval:             durationPtr(30 * time.Second),
		Retry: kafka.RetryPolicy{
			MaxRetries: 10,
			BaseDelay:  5 * time.Second,
			MaxDelay:   10 * time.Second,
		},
	}

	consumer, err := kafka.NewConsumer(ctx, log, consumerCfg, proc, nil)
	require.NoError(t, err)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return consumer.Start(gctx)
	})

	startTime := time.Now()
	err = g.Wait()
	elapsed := time.Since(startTime)

	// Fatal error should stop the consumer and propagate through errgroup.
	require.Error(t, err, "consumer should return a Fatal error")
	t.Logf("consumer stopped in %v with error: %v", elapsed, err)

	// The consumer should stop fast — no retries for Fatal errors.
	require.Less(t, elapsed, 30*time.Second,
		"Fatal error should stop the consumer quickly without retries")

	// Fatal errors must NOT publish to DLQ, even though PublishToDLQ=true.
	dlqCount := countMessagesInTopic(t, kafkaBrokers, dlqTopic, 5*time.Second)
	require.Equal(t, 0, dlqCount,
		"Fatal errors must not publish to DLQ")

	// Clean up the tables created by this test.
	for _, tbl := range []string{transactionsTable, logsTable} {
		_ = chClient.Conn().Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
	}

	log.Infow("Fatal error test completed",
		"elapsed", elapsed,
		"dlq_messages", dlqCount)
}

// countMessagesInTopic consumes all available messages from a Kafka topic and
// returns the count. It stops after three consecutive empty polls or when
// timeout is reached.
func countMessagesInTopic(t *testing.T, brokers, topic string, timeout time.Duration) int {
	t.Helper()

	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          fmt.Sprintf("e2e-counter-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer consumer.Close()

	if err := consumer.Subscribe(topic, nil); err != nil {
		return 0
	}

	count := 0
	emptyPolls := 0
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		ev := consumer.Poll(1000)
		if ev == nil {
			emptyPolls++
			if emptyPolls >= 3 {
				break
			}
			continue
		}

		switch ev.(type) {
		case *ckafka.Message:
			count++
			emptyPolls = 0
		case ckafka.Error:
			continue
		}
	}

	t.Logf("Counted %d messages in topic %s", count, topic)
	return count
}
