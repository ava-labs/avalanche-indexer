//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"golang.org/x/sync/errgroup"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/snapshot"
	stream "github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/scheduler"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/subscriber"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	corethtypes "github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/rpc"
	"github.com/stretchr/testify/require"
)

// TestE2EBlockfetcherRealTime validates that blockfetcher ingests realtime blocks
// by producing them to Kafka. It assumes Docker Compose has started Kafka and ClickHouse.
func TestE2EBlockfetcherRealTime(t *testing.T) {
	t.Parallel()
	// ---- Config (can be overridden via env to match local setup) ----
	chainID := uint64(getEnvUint64("CHAIN_ID", 43113)) // Fuji testnet
	rpcURL := getEnvStr("RPC_URL", "wss://api.avax-test.network/ext/bc/C/ws")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := getEnvStr("KAFKA_TOPIC", "blocks_realtime")
	kafkaClientID := getEnvStr("KAFKA_CLIENT_ID", "blockfetcher-e2e")
	clickhouseTable := getEnvStr("SNAPSHOT_TABLE_NAME", "test_db.snapshots")
	concurrency := uint64(3)
	backfill := uint64(1)
	blocksCap := 100
	maxFailures := 3
	// Keep interval short to validate ClickHouse writes if desired (not asserted).
	snapshotInterval := 2 * time.Second

	// ---- Test context ----
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	// ---- Prepare ClickHouse (create DB/table if needed, seed snapshot at latest height) ----
	chCfg := clickhouse.Load()
	chClient, err := clickhouse.New(chCfg, log)
	require.NoError(t, err, "clickhouse connection failed (is docker-compose up?)")
	defer chClient.Close()

	repo := snapshot.NewRepository(chClient, clickhouseTable)
	err = repo.CreateTableIfNotExists(ctx)
	require.NoError(t, err, "failed to check existence or create snapshots table")

	// Get latest block height from RPC to seed snapshot.
	rpcClient, err := rpc.DialContext(ctx, rpcURL)
	require.NoError(t, err, "rpc dial failed (check RPC_URL)")
	defer rpcClient.Close()
	latest, err := customethclient.New(rpcClient).BlockNumber(ctx)
	require.NoError(t, err, "failed to get latest block height")

	seed := &snapshot.Snapshot{
		ChainID:   chainID,
		Lowest:    latest,
		Timestamp: time.Now().Unix(),
	}
	err = repo.WriteSnapshot(ctx, seed)
	require.NoError(t, err, "failed to seed snapshot row")

	// ---- Kafka consumer to observe realtime blocks ----
	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"group.id":          fmt.Sprintf("e2e-blockfetcher-%d", time.Now().UnixNano()),
		"auto.offset.reset": "latest",
	})
	require.NoError(t, err)
	defer consumer.Close()
	require.NoError(t, consumer.Subscribe(kafkaTopic, nil))

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

	w, err := worker.NewCorethWorker(ctx, rpcURL, producer, kafkaTopic, log, nil)
	require.NoError(t, err)

	state, err := slidingwindow.NewState(seed.Lowest, latest)
	require.NoError(t, err)
	mgr, err := slidingwindow.NewManager(log, state, w, concurrency, backfill, blocksCap, maxFailures, nil)
	require.NoError(t, err)

	sub := subscriber.NewCoreth(log, customethclient.New(rpcClient))

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
	g.Go(func() error { return scheduler.Start(gctx, state, repo, snapshotInterval, chainID) })

	minMsgs := 5
	received := 0
	kafkaByNumber := make(map[uint64][]byte)
	var receivedOrder []uint64
	for received < minMsgs {
		ev := consumer.Poll(1000)
		if ev == nil {
			if gctx.Err() != nil {
				break
			}
			continue
		}
		switch e := ev.(type) {
		case *ckafka.Message:
			// Parse block number from message key (decimal string)
			var n uint64
			if len(e.Key) > 0 {
				_, _ = fmt.Sscanf(string(e.Key), "%d", &n)
			}
			if n != 0 {
				if _, exists := kafkaByNumber[n]; !exists {
					receivedOrder = append(receivedOrder, n)
				}
				kafkaByNumber[n] = e.Value
			}
			received++
		case ckafka.Error:
			// Non-fatal errors can occur; surface fatal/all-brokers-down.
			if e.IsFatal() || e.Code() == ckafka.ErrAllBrokersDown {
				require.NoError(t, e, "fatal kafka error")
			}
		default:
			// ignore other events
		}
		if gctx.Err() != nil {
			break
		}
	}

	require.GreaterOrEqual(t, received, minMsgs, "did not receive realtime block(s) from Kafka")

	// Verify Kafka payloads match RPC for the received block numbers (in arrival order).
	verifyBlocksFromRPC(t, ctx, rpcURL, kafkaByNumber, receivedOrder)

	// Verify snapshot reflects max processed block (+1 for lowest_unprocessed_block).
	verifySnapshotFromMaxProcessed(t, ctx, repo, chainID, kafkaByNumber)

	// Shutdown gracefully
	cancel()
	_ = g.Wait()
}

// TestE2EBlockfetcherBackfill runs backfill over a small recent range and verifies
// produced Kafka blocks match RPC responses using verifyBlocksFromRPC.
func TestE2EBlockfetcherBackfill(t *testing.T) {
	t.Parallel()
	// ---- Config ----
	chainID := uint64(getEnvUint64("CHAIN_ID", 43113)) // Fuji by default
	rpcURL := getEnvStr("RPC_URL", "wss://api.avax-test.network/ext/bc/C/ws")
	kafkaBrokers := getEnvStr("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := getEnvStr("KAFKA_TOPIC", "blocks_backfill")
	kafkaClientID := "blockfetcher-e2e-backfill"
	clickhouseTable := getEnvStr("SNAPSHOT_TABLE_NAME", "test_db.snapshots")
	concurrency := uint64(4)
	backfill := uint64(2)
	blocksCap := 50
	maxFailures := 3
	snapshotInterval := 2 * time.Second

	// ---- Test context ----
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)
	defer log.Desugar().Sync() //nolint:errcheck

	// Determine recent range [start..end]
	rpcClient, err := rpc.DialContext(ctx, rpcURL)
	require.NoError(t, err, "rpc dial failed (check RPC_URL)")
	defer rpcClient.Close()
	latest, err := customethclient.New(rpcClient).BlockNumber(ctx)
	require.NoError(t, err, "failed to get latest block height")
	const span uint64 = 6
	var start uint64
	if latest > span {
		start = latest - span
	} else {
		start = 1
	}
	end := latest

	// ---- Prepare ClickHouse (create DB/table if needed) ----
	chCfg := clickhouse.Load()
	chClient, err := clickhouse.New(chCfg, log)
	require.NoError(t, err, "clickhouse connection failed (is docker-compose up?)")
	defer chClient.Close()

	repo := snapshot.NewRepository(chClient, clickhouseTable)
	err = repo.CreateTableIfNotExists(ctx)
	if err != nil {
		require.NoError(t, err, "failed to check existence or create snapshots table")
	}

	// ---- Kafka consumer to observe backfilled blocks ----
	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"group.id":          fmt.Sprintf("e2e-blockfetcher-backfill-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer consumer.Close()
	require.NoError(t, consumer.Subscribe(kafkaTopic, nil))

	// ---- Start producer/worker/manager (no realtime subscriber) ----
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

	w, err := worker.NewCorethWorker(ctx, rpcURL, producer, kafkaTopic, log, nil)
	require.NoError(t, err)

	state, err := slidingwindow.NewState(start, end)
	require.NoError(t, err)
	mgr, err := slidingwindow.NewManager(log, state, w, concurrency, backfill, blocksCap, maxFailures, nil)
	require.NoError(t, err)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return mgr.Run(gctx) })
	g.Go(func() error {
		select {
		case <-gctx.Done():
			return gctx.Err()
		case err := <-producer.Errors():
			return err
		}
	})
	g.Go(func() error { return scheduler.Start(gctx, state, repo, snapshotInterval, chainID) })

	// Collect exactly the expected backfill range
	expectedCount := int(end - start + 1)
	kafkaByNumber := make(map[uint64][]byte, expectedCount)
	var numbers []uint64
	for len(kafkaByNumber) < expectedCount {
		ev := consumer.Poll(1000)
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
			// Only track if within our backfill window
			if n >= start && n <= end {
				if _, ok := kafkaByNumber[n]; !ok {
					numbers = append(numbers, n)
				}
				kafkaByNumber[n] = e.Value
			}
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
	require.Equal(t, expectedCount, len(kafkaByNumber), "missing backfilled blocks from Kafka")

	// Verify messages with RPC
	verifyBlocksFromRPC(t, ctx, rpcURL, kafkaByNumber, numbers)

	// Verify snapshot reflects max processed block (+1 for lowest_unprocessed_block).
	verifySnapshotFromMaxProcessed(t, ctx, repo, chainID, kafkaByNumber)

	// Shutdown
	cancel()
	_ = g.Wait()
}

// verifySnapshotFromMaxProcessed finds the max processed height and checks snapshot lowest==max+1.
func verifySnapshotFromMaxProcessed(t *testing.T, ctx context.Context, repo snapshot.Repository, chainID uint64, kafkaByNumber map[uint64][]byte) {
	t.Helper()
	if len(kafkaByNumber) == 0 {
		return
	}
	var max uint64
	for n := range kafkaByNumber {
		if n > max {
			max = n
		}
	}
	verifySnapshotLowestEquals(t, ctx, repo, chainID, max+1)
}

// verifySnapshotLowestEquals polls ClickHouse snapshot for the chain until lowest==expected or timeout.
func verifySnapshotLowestEquals(t *testing.T, ctx context.Context, repo snapshot.Repository, chainID uint64, expected uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, err := repo.ReadSnapshot(ctx, chainID)
		if err == nil && s != nil && s.Lowest == expected {
			return
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "read snapshot failed")
			require.NotNil(t, s, "snapshot nil")
			require.Equal(t, expected, s.Lowest, "snapshot lowest mismatch")
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// verifyBlocksFromRPC fetches blocks by number from RPC and compares to Kafka payloads.
func verifyBlocksFromRPC(t *testing.T, ctx context.Context, rpcURL string, kafkaByNumber map[uint64][]byte, numbers []uint64) {
	if len(numbers) == 0 {
		return
	}
	client, err := rpc.DialContext(ctx, rpcURL)
	require.NoError(t, err, "dial rpc for verification")
	defer client.Close()
	ec := customethclient.New(client)

	// Compare only for numbers we have payloads for.
	for _, n := range numbers {
		val, ok := kafkaByNumber[n]
		if !ok {
			continue
		}
		// Decode Kafka payload
		var got corethtypes.Block
		require.NoError(t, got.Unmarshal(val), "decode kafka block %d", n)

		// Fetch from RPC and convert to our Block type
		bn := new(big.Int).SetUint64(n)
		lb, err := ec.BlockByNumber(ctx, bn)
		require.NoError(t, err, "fetch rpc block %d", n)
		expPtr, err := corethtypes.BlockFromLibevm(lb)
		require.NoError(t, err, "convert rpc block %d", n)
		exp := *expPtr

		// Compare a set of critical fields for robustness
		require.Equal(t, exp.Hash, got.Hash, "hash %d", n)
		require.Equal(t, exp.ParentHash, got.ParentHash, "parentHash %d", n)
		require.Equal(t, exp.Number.Uint64(), got.Number.Uint64(), "number %d", n)
		require.Equal(t, exp.GasLimit, got.GasLimit, "gasLimit %d", n)
		require.Equal(t, exp.GasUsed, got.GasUsed, "gasUsed %d", n)
		require.Equal(t, exp.StateRoot, got.StateRoot, "stateRoot %d", n)
		require.Equal(t, exp.TransactionsRoot, got.TransactionsRoot, "txRoot %d", n)
		require.Equal(t, exp.ReceiptsRoot, got.ReceiptsRoot, "receiptsRoot %d", n)
		// Optional fields (presence may vary across forks)
		if exp.BaseFee != nil || got.BaseFee != nil {
			require.NotNil(t, exp.BaseFee, "exp baseFee nil for %d", n)
			require.NotNil(t, got.BaseFee, "got baseFee nil for %d", n)
			require.Equal(t, exp.BaseFee.String(), got.BaseFee.String(), "baseFee %d", n)
		}
	}
}

// Helpers
func getEnvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvUint64(key string, def uint64) uint64 {
	if v := os.Getenv(key); v != "" {
		var out uint64
		_, _ = fmt.Sscanf(v, "%d", &out)
		if out != 0 {
			return out
		}
	}
	return def
}
