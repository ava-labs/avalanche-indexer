package evmrepo

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// BatchInserter handles batched inserts to ClickHouse for both blocks and transactions
// It ensures blocks and transactions are flushed together for data consistency
type BatchInserter struct {
	conn          driver.Conn
	log           *zap.SugaredLogger
	blocksTable   string
	txsTable      string
	maxBatchSize  int
	flushInterval time.Duration

	// Block batching
	blockBatch    driver.Batch
	blockBatchMux sync.Mutex
	blockCount    int

	// Transaction batching
	txBatch    driver.Batch
	txBatchMux sync.Mutex
	txCount    int

	// Combined flush tracking
	lastFlush time.Time
	flushMux  sync.Mutex

	// Flush ticker
	flushTicker *time.Ticker
	stopCh      chan struct{}
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewBatchInserter creates a new combined batch inserter for blocks and transactions
func NewBatchInserter(
	ctx context.Context,
	conn driver.Conn,
	log *zap.SugaredLogger,
	blocksTable string,
	txsTable string,
	maxBatchSize int,
	flushInterval time.Duration,
) *BatchInserter {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	bi := &BatchInserter{
		conn:          conn,
		log:           log,
		blocksTable:   blocksTable,
		txsTable:      txsTable,
		maxBatchSize:  maxBatchSize,
		flushInterval: flushInterval,
		lastFlush:     time.Now(),
		flushTicker:   time.NewTicker(flushInterval),
		stopCh:        make(chan struct{}),
		ctx:           ctxWithCancel,
		cancel:        cancel,
	}

	// Start background flush goroutine
	bi.wg.Add(1)
	go bi.flushLoop()

	return bi
}

// flushLoop periodically flushes batches based on flushInterval
func (bi *BatchInserter) flushLoop() {
	defer bi.wg.Done()
	for {
		select {
		case <-bi.ctx.Done():
			return
		case <-bi.stopCh:
			return
		case <-bi.flushTicker.C:
			// Check if we need to flush based on time
			bi.flushMux.Lock()
			shouldFlush := time.Since(bi.lastFlush) >= bi.flushInterval
			bi.flushMux.Unlock()

			if shouldFlush {
				if err := bi.FlushAll(bi.ctx); err != nil {
					bi.log.Errorw("failed to flush batches in flush loop", "error", err)
				}
			}
		}
	}
}

// InitBlockBatch initializes a new block batch
func (bi *BatchInserter) InitBlockBatch(ctx context.Context) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			blockchain_id, evm_chain_id, block_number, hash, parent_hash, block_time, miner,
			difficulty, total_difficulty, size, gas_limit, gas_used,
			base_fee_per_gas, block_gas_cost, state_root, transactions_root, receipts_root,
			extra_data, block_extra_data, ext_data_hash, ext_data_gas_used,
			mix_hash, nonce, sha3_uncles, uncles,
			blob_gas_used, excess_blob_gas, parent_beacon_block_root, min_delay_excess
		)
	`, bi.blocksTable)

	batch, err := bi.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare block batch: %w", err)
	}
	bi.blockBatch = batch
	bi.blockCount = 0
	return nil
}

// InitTxBatch initializes a new transaction batch
func (bi *BatchInserter) InitTxBatch(ctx context.Context) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			blockchain_id, evm_chain_id, block_number, block_hash, block_time, hash,
			from_address, to_address, nonce, value, gas, gas_price,
			max_fee_per_gas, max_priority_fee, input, type, transaction_index
		)
	`, bi.txsTable)

	batch, err := bi.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare tx batch: %w", err)
	}
	bi.txBatch = batch
	bi.txCount = 0
	return nil
}

// AddBlock adds a block to the batch (thread-safe)
func (bi *BatchInserter) AddBlock(ctx context.Context, block *BlockRow) error {
	bi.blockBatchMux.Lock()

	if bi.blockBatch == nil {
		if err := bi.InitBlockBatch(ctx); err != nil {
			bi.blockBatchMux.Unlock()
			return err
		}
	}

	// Convert data for ClickHouse
	// Note: For batch inserts, we must pass *big.Int directly for UInt256, not strings
	var blockchainID interface{}
	if block.BlockchainID != nil {
		blockchainID = *block.BlockchainID
	} else {
		blockchainID = ""
	}

	// For batch inserts, pass *big.Int directly (not string)
	var evmChainID *big.Int
	if block.EVMChainID != nil {
		evmChainID = block.EVMChainID
	} else {
		evmChainID = big.NewInt(0)
	}

	var blockNumber uint64
	if block.BlockNumber != nil {
		blockNumber = block.BlockNumber.Uint64()
	}

	// For batch inserts, pass *big.Int directly (not string)
	var difficulty *big.Int
	if block.Difficulty != nil {
		difficulty = block.Difficulty
	} else {
		difficulty = big.NewInt(0)
	}
	var totalDifficulty *big.Int
	if block.TotalDifficulty != nil {
		totalDifficulty = block.TotalDifficulty
	} else {
		totalDifficulty = big.NewInt(0)
	}
	var baseFee *big.Int
	if block.BaseFeePerGas != nil {
		baseFee = block.BaseFeePerGas
	} else {
		baseFee = big.NewInt(0)
	}
	var blockGasCost *big.Int
	if block.BlockGasCost != nil {
		blockGasCost = block.BlockGasCost
	} else {
		blockGasCost = big.NewInt(0)
	}

	var nonceStr interface{}
	if block.Nonce == "" {
		nonceStr = nil
	} else {
		nonceStr = block.Nonce
	}

	var parentBeaconBlockRootStr interface{}
	if block.ParentBeaconBlockRoot == "" {
		parentBeaconBlockRootStr = nil
	} else {
		parentBeaconBlockRootStr = block.ParentBeaconBlockRoot
	}

	err := bi.blockBatch.Append(
		blockchainID,
		evmChainID,
		blockNumber,
		block.Hash,
		block.ParentHash,
		block.BlockTime,
		block.Miner,
		difficulty,
		totalDifficulty,
		block.Size,
		block.GasLimit,
		block.GasUsed,
		baseFee,
		blockGasCost,
		block.StateRoot,
		block.TransactionsRoot,
		block.ReceiptsRoot,
		block.ExtraData,
		block.BlockExtraData,
		block.ExtDataHash,
		block.ExtDataGasUsed,
		block.MixHash,
		nonceStr,
		block.Sha3Uncles,
		block.Uncles,
		block.BlobGasUsed,
		block.ExcessBlobGas,
		parentBeaconBlockRootStr,
		block.MinDelayExcess,
	)
	if err != nil {
		bi.blockBatchMux.Unlock()
		return fmt.Errorf("failed to append block: %w", err)
	}

	bi.blockCount++
	shouldFlush := bi.blockCount >= bi.maxBatchSize
	bi.blockBatchMux.Unlock()

	// Flush if needed (FlushAll will acquire both locks)
	if shouldFlush {
		return bi.FlushAll(ctx)
	}

	return nil
}

// AddTransaction adds a transaction to the batch (thread-safe)
func (bi *BatchInserter) AddTransaction(ctx context.Context, tx *TransactionRow) error {
	bi.txBatchMux.Lock()

	if bi.txBatch == nil {
		if err := bi.InitTxBatch(ctx); err != nil {
			bi.txBatchMux.Unlock()
			return err
		}
	}

	// Convert data for ClickHouse
	// Note: For batch inserts, we must pass *big.Int directly for UInt256, not strings
	var blockchainID interface{}
	if tx.BlockchainID != nil {
		blockchainID = *tx.BlockchainID
	} else {
		blockchainID = ""
	}

	// For batch inserts, pass *big.Int directly (not string)
	var evmChainID *big.Int
	if tx.EVMChainID != nil {
		evmChainID = tx.EVMChainID
	} else {
		evmChainID = big.NewInt(0)
	}

	// For batch inserts, pass *big.Int directly (not string)
	var value *big.Int
	if tx.Value != nil {
		value = tx.Value
	} else {
		value = big.NewInt(0)
	}
	var gasPrice *big.Int
	if tx.GasPrice != nil {
		gasPrice = tx.GasPrice
	} else {
		gasPrice = big.NewInt(0)
	}
	// Nullable fields can be nil or *big.Int
	var maxFeePerGas interface{}
	if tx.MaxFeePerGas != nil {
		maxFeePerGas = tx.MaxFeePerGas
	} else {
		maxFeePerGas = nil
	}
	var maxPriorityFee interface{}
	if tx.MaxPriorityFee != nil {
		maxPriorityFee = tx.MaxPriorityFee
	} else {
		maxPriorityFee = nil
	}

	err := bi.txBatch.Append(
		blockchainID,
		evmChainID,
		tx.BlockNumber,
		tx.BlockHash,
		tx.BlockTime,
		tx.Hash,
		tx.From,
		tx.To,
		tx.Nonce,
		value,
		tx.Gas,
		gasPrice,
		maxFeePerGas,
		maxPriorityFee,
		tx.Input,
		tx.Type,
		tx.TransactionIndex,
	)
	if err != nil {
		bi.txBatchMux.Unlock()
		return fmt.Errorf("failed to append transaction: %w", err)
	}

	bi.txCount++
	shouldFlush := bi.txCount >= bi.maxBatchSize
	bi.txBatchMux.Unlock()

	// Flush if needed (FlushAll will acquire both locks)
	if shouldFlush {
		return bi.FlushAll(ctx)
	}

	return nil
}

// flushBlocksLocked sends the block batch to ClickHouse (must be called with blockBatchMux lock held)
func (bi *BatchInserter) flushBlocksLocked(ctx context.Context) error {
	if bi.blockBatch == nil || bi.blockCount == 0 {
		return nil
	}

	count := bi.blockCount
	if err := bi.blockBatch.Send(); err != nil {
		// Log the error prominently - flush failures mean data loss
		bi.log.Errorw("CRITICAL: failed to send block batch to ClickHouse - data may be lost",
			"error", err,
			"count", count,
			"table", bi.blocksTable,
		)
		// Reset batch state even on failure so we can continue
		bi.blockBatch = nil
		bi.blockCount = 0
		return fmt.Errorf("failed to send block batch: %w", err)
	}

	bi.log.Debugw("flushed blocks to ClickHouse", "count", count)
	bi.blockBatch = nil
	bi.blockCount = 0
	return nil
}

// flushTransactionsLocked sends the transaction batch to ClickHouse (must be called with txBatchMux lock held)
func (bi *BatchInserter) flushTransactionsLocked(ctx context.Context) error {
	if bi.txBatch == nil || bi.txCount == 0 {
		return nil
	}

	count := bi.txCount
	if err := bi.txBatch.Send(); err != nil {
		// Log the error prominently - flush failures mean data loss
		bi.log.Errorw("Failed to send transaction batch to ClickHouse - data may be lost",
			"error", err,
			"count", count,
			"table", bi.txsTable,
		)
		// Reset batch state even on failure so we can continue
		bi.txBatch = nil
		bi.txCount = 0
		return fmt.Errorf("Failed to send transaction batch: %w", err)
	}

	bi.log.Debugw("flushed transactions to ClickHouse", "count", count)
	bi.txBatch = nil
	bi.txCount = 0
	return nil
}

// FlushAll flushes both blocks and transactions together for data consistency
// This is the preferred method to ensure blocks and their transactions are written together
func (bi *BatchInserter) FlushAll(ctx context.Context) error {
	// Acquire both locks to ensure atomic flush
	bi.blockBatchMux.Lock()
	bi.txBatchMux.Lock()
	defer bi.txBatchMux.Unlock()
	defer bi.blockBatchMux.Unlock()

	return bi.flushAllLocked(ctx)
}

// flushAllLocked flushes both batches (must be called with both locks held)
func (bi *BatchInserter) flushAllLocked(ctx context.Context) error {
	// Flush blocks first (locks already held, so call locked version directly)
	blockErr := bi.flushBlocksLocked(ctx)

	// Flush transactions (even if block flush failed, we still try to flush transactions)
	txErr := bi.flushTransactionsLocked(ctx)

	// Update last flush time
	bi.flushMux.Lock()
	bi.lastFlush = time.Now()
	bi.flushMux.Unlock()

	// Return error if either failed
	if blockErr != nil {
		return blockErr
	}
	return txErr
}

// Close gracefully shuts down the batch inserter, flushing any remaining batches
func (bi *BatchInserter) Close(ctx context.Context) error {
	bi.cancel()
	bi.flushTicker.Stop()
	close(bi.stopCh)
	bi.wg.Wait()

	// Final flush
	return bi.FlushAll(ctx)
}
