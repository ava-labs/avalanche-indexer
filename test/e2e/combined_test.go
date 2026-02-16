//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/checkpoint"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	stream "github.com/ava-labs/avalanche-indexer/pkg/kafka"
	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/scheduler"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/rpc"
)

// TestE2ECombinedBlockfetcherConsumerIndexer spins up the realtime blockfetcher
// and a consumer indexer that persists data to ClickHouse, then verifies that:
// - Kafka receives realtime blocks
// - ClickHouse tables contain corresponding rows for blocks/transactions/logs
// - Checkpoint reflects the max processed block (+1 for LUB)
func TestE2ECombinedBlockfetcherConsumerIndexer(t *testing.T) {
	// ---- Config (overridable via env) ----
	evmChainID := uint64(getEnvUint64("CHAIN_ID", 43113)) // Fuji
	bcID := getEnvStr("BC_ID", "11111111111111111111111111111111LpoYY")
	rpcURL := getEnvStr("RPC_URL", "wss://api.avax-test.network/ext/bc/C/ws")

	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := getEnvStr("KAFKA_TOPIC", "blocks_combined")
	kafkaClientID := getEnvStr("KAFKA_CLIENT_ID", "blockfetcher-combined-e2e")

	checkpointTable := getEnvStr("CHECKPOINT_TABLE_NAME", "checkpoints_combined")
	rawBlocksTable := getEnvStr("CLICKHOUSE_RAW_BLOCKS_TABLE_NAME", "raw_blocks")
	rawTxTable := getEnvStr("CLICKHOUSE_RAW_TRANSACTIONS_TABLE_NAME", "raw_transactions")
	rawLogsTable := getEnvStr("CLICKHOUSE_RAW_LOGS_TABLE_NAME", "raw_logs")

	concurrency := int64(3)
	backfill := int64(1)
	blocksCap := 100
	maxFailures := 10
	checkpointInterval := 2 * time.Second

	// ---- Test context ----
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	// ---- Prepare ClickHouse ----
	chClient, err := clickhouse.New(clickhouseTestConfig, log)
	require.NoError(t, err, "clickhouse connection failed (is docker-compose up?)")
	defer chClient.Close()

	// ---- Seed checkpoint near latest finalized height ----
	repo, err := checkpoint.NewRepository(chClient, "default", "default", checkpointTable)
	require.NoError(t, err)

	rpcClient, err := rpc.DialContext(ctx, rpcURL)
	require.NoError(t, err, "rpc dial failed (check RPC_URL)")
	defer rpcClient.Close()
	latest, err := customethclient.New(rpcClient).BlockNumber(ctx)
	require.NoError(t, err, "failed to get latest block height")
	const safetyMargin = 5
	startHeight := latest
	if latest > safetyMargin {
		startHeight = latest - safetyMargin
	}
	seed := &checkpoint.Checkpoint{
		ChainID:   evmChainID,
		Lowest:    startHeight,
		Timestamp: time.Now().Unix(),
	}
	err = repo.WriteCheckpoint(ctx, seed)
	require.NoError(t, err, "failed to seed checkpoint row")

	// ---- Ensure Kafka topic exists ----
	adminCfg := ckafka.ConfigMap{"bootstrap.servers": kafkaBrokers}
	adminClient, err := ckafka.NewAdminClient(&adminCfg)
	require.NoError(t, err)
	defer adminClient.Close()
	err = stream.EnsureTopic(ctx, adminClient, stream.TopicConfig{
		Name:              kafkaTopic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, log)
	require.NoError(t, err)

	// ---- Create and start consumer indexer (processor + consumer) ----
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	require.NoError(t, err)

	blocksRepo, err := evmrepo.NewBlocks(ctx, chClient, "default", "default", rawBlocksTable)
	require.NoError(t, err)
	txsRepo, err := evmrepo.NewTransactions(ctx, chClient, "default", "default", rawTxTable)
	require.NoError(t, err)
	logsRepo, err := evmrepo.NewLogs(ctx, chClient, "default", "default", rawLogsTable)
	require.NoError(t, err)
	proc := processor.NewCorethProcessor(log, blocksRepo, txsRepo, logsRepo, m)

	consumerCfg := stream.ConsumerConfig{
		Topic:                       kafkaTopic,
		DLQTopic:                    "", // not used in test
		BootstrapServers:            kafkaBrokers,
		GroupID:                     fmt.Sprintf("combined-indexer-%d", time.Now().UnixNano()),
		AutoOffsetReset:             "earliest",
		Concurrency:                 4,
		OffsetManagerCommitInterval: 2 * time.Second,
		PublishToDLQ:                false,
	}
	indexerConsumer, err := stream.NewConsumer(ctx, log, consumerCfg, proc)
	require.NoError(t, err)

	// ---- Start blockfetcher components (producer, worker, manager, subscriber, scheduler) ----
	kCfg := &ckafka.ConfigMap{
		"bootstrap.servers":      kafkaBrokers,
		"client.id":              kafkaClientID,
		"acks":                   "all",
		"linger.ms":              5,
		"batch.size":             16384,
		"compression.type":       "lz4",
		"enable.idempotence":     true,
		"go.logs.channel.enable": false,
	}
	producer, err := stream.NewProducer(ctx, kCfg, log)
	require.NoError(t, err)
	defer producer.Close(15 * time.Second)

	client, err := customethclient.DialContext(ctx, rpcURL)
	if err != nil {
		require.Fail(t, "failed to dial rpc", err)
	}
	defer client.Close()

	w, err := worker.NewCorethWorker(client, producer, kafkaTopic, evmChainID, bcID, log, m, 10*time.Second)
	require.NoError(t, err)
	state, err := slidingwindow.NewState(seed.Lowest, latest)
	require.NoError(t, err)
	mgr, err := slidingwindow.NewManager(log, state, w, concurrency, backfill, blocksCap, maxFailures, nil)
	require.NoError(t, err)
	sub := subscriber.NewCoreth(log, customethclient.New(rpcClient))

	// ---- Test observer consumer to capture produced Kafka messages ----
	testConsumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"group.id":          fmt.Sprintf("combined-e2e-observer-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer testConsumer.Close()
	require.NoError(t, testConsumer.Subscribe(kafkaTopic, nil))

	// ---- Run goroutines ----
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return sub.Subscribe(gctx, blocksCap, mgr) })
	g.Go(func() error { return mgr.Run(gctx) })
	g.Go(func() error {
		select {
		case <-gctx.Done():
			return gctx.Err()
		case err := <-producer.Errors():
			return err
		}
	})
	g.Go(func() error { return scheduler.Start(gctx, state, repo, checkpointInterval, evmChainID) })
	g.Go(func() error { return indexerConsumer.Start(gctx) })

	// ---- Collect realtime Kafka messages ----
	minMsgs := 5
	received := 0
	kafkaByNumber := make(map[uint64][]byte)
	kafkaBlocks := make(map[uint64]kafkamsg.EVMBlock)
	var receivedOrder []uint64
	for received < minMsgs {
		ev := testConsumer.Poll(2000)
		if ev == nil {
			if gctx.Err() != nil {
				break
			}
			continue
		}
		switch e := ev.(type) {
		case *ckafka.Message:
			var n uint64
			if len(e.Key) > 0 {
				_, _ = fmt.Sscanf(string(e.Key), "%d", &n)
			}
			if n != 0 {
				if _, exists := kafkaByNumber[n]; !exists {
					receivedOrder = append(receivedOrder, n)
				}
				kafkaByNumber[n] = e.Value
				// decode once to help ClickHouse verification
				var blk kafkamsg.EVMBlock
				require.NoError(t, json.Unmarshal(e.Value, blk), "decode kafka block %d", n)
				kafkaBlocks[n] = blk
			}
			received++
		case ckafka.Error:
			if e.IsFatal() || e.Code() == ckafka.ErrAllBrokersDown {
				require.NoError(t, e, "fatal kafka error")
			}
		default:
			// ignore
		}
		if gctx.Err() != nil {
			break
		}
	}

	// ---- Shutdown everything ----
	// Basic sanity: we saw some realtime blocks
	require.GreaterOrEqual(t, received, minMsgs, "did not observe realtime blocks on Kafka")

	// Perform verification BEFORE shutting down producers/consumers to avoid cancelling in-flight writes
	verifyCtx, vcancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer vcancel()
	verifyBlocksPersistedInClickHouse(t, verifyCtx, chClient, rawBlocksTable, rawTxTable, rawLogsTable, kafkaBlocks, receivedOrder)
	verifyCheckpointFromMaxProcessed(t, verifyCtx, repo, evmChainID, kafkaByNumber)

	// Now cancel and wait goroutines to exit
	cancel()
	_ = g.Wait()
}

func verifyBlocksPersistedInClickHouse(
	t *testing.T,
	ctx context.Context,
	ch clickhouse.Client,
	blocksTable, txTable, logsTable string,
	kafkaBlocks map[uint64]kafkamsg.EVMBlock,
	order []uint64,
) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	waitUntil := func(cond func() bool) {
		for !cond() {
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	for _, n := range order {
		blk, ok := kafkaBlocks[n]
		if !ok {
			continue
		}

		// Wait for block row to appear
		var blockCount uint64
		waitUntil(func() bool {
			blockCount = queryCount(t, ctx, ch,
				fmt.Sprintf("SELECT count() FROM %s WHERE block_number = ?", blocksTable),
				n,
			)
			return blockCount >= 1
		})
		require.GreaterOrEqual(t, blockCount, uint64(1), "block row not found for %d", n)

		// Verify key fields for the block row
		dbHash := queryFixedString(t, ctx, ch,
			fmt.Sprintf("SELECT hash FROM %s WHERE block_number = ? LIMIT 1", blocksTable),
			n,
		)
		dbParentHash := queryFixedString(t, ctx, ch,
			fmt.Sprintf("SELECT parent_hash FROM %s WHERE block_number = ? LIMIT 1", blocksTable),
			n,
		)
		dbEVMChainID := queryString(t, ctx, ch,
			fmt.Sprintf("SELECT toString(evm_chain_id) FROM %s WHERE block_number = ? LIMIT 1", blocksTable),
			n,
		)
		dbBlockchainID := queryString(t, ctx, ch,
			fmt.Sprintf("SELECT blockchain_id FROM %s WHERE block_number = ? LIMIT 1", blocksTable),
			n,
		)

		hashBytes := mustHexToFixed32(t, blk.Hash)
		parentHashBytes := mustHexToFixed32(t, blk.ParentHash)
		require.Equal(t, hashBytes, dbHash, "hash mismatch for %d", n)
		require.Equal(t, parentHashBytes, dbParentHash, "parent_hash mismatch for %d", n)
		if blk.EVMChainID != nil {
			require.Equal(t, blk.EVMChainID.String(), dbEVMChainID, "evm_chain_id mismatch for %d", n)
		}
		if blk.BlockchainID != nil {
			require.Equal(t, *blk.BlockchainID, dbBlockchainID, "blockchain_id mismatch for %d", n)
		}

		// Verify transactions count (best-effort; allow >=)
		expTxCount := len(blk.Transactions)
		if expTxCount > 0 {
			var gotTxCount uint64
			waitUntil(func() bool {
				gotTxCount = queryCount(t, ctx, ch,
					fmt.Sprintf("SELECT count() FROM %s WHERE block_number = ?", txTable),
					n,
				)
				return gotTxCount >= uint64(expTxCount)
			})
			require.GreaterOrEqual(t, int(gotTxCount), expTxCount, "transactions persisted fewer than expected for %d", n)
		}

		// Verify logs count derived from receipts (allow >=)
		expLogs := 0
		for _, tx := range blk.Transactions {
			if tx != nil && tx.Receipt != nil {
				expLogs += len(tx.Receipt.Logs)
			}
		}
		if expLogs > 0 {
			var gotLogs uint64
			waitUntil(func() bool {
				gotLogs = queryCount(t, ctx, ch,
					fmt.Sprintf("SELECT count() FROM %s WHERE block_number = ?", logsTable),
					n,
				)
				return gotLogs >= uint64(expLogs)
			})
			require.GreaterOrEqual(t, int(gotLogs), expLogs, "logs persisted fewer than expected for %d", n)
		}
	}
}
