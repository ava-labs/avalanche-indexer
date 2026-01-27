package processor

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"go.uber.org/zap"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// CorethProcessor unmarshals and logs Coreth blocks from Kafka messages.
// If repositories are provided, persists blocks and transactions to ClickHouse.
// Safe for concurrent use.
type CorethProcessor struct {
	log        *zap.SugaredLogger
	blocksRepo evmrepo.Blocks
	txsRepo    evmrepo.Transactions
	logsRepo   evmrepo.Logs
	metrics    *metrics.Metrics
}

// NewCorethProcessor creates a new CorethProcessor with the given logger.
// If repositories are provided, blocks and transactions will be persisted to ClickHouse.
func NewCorethProcessor(
	log *zap.SugaredLogger,
	blocksRepo evmrepo.Blocks,
	txsRepo evmrepo.Transactions,
	logsRepo evmrepo.Logs,
	metrics *metrics.Metrics,
) *CorethProcessor {
	return &CorethProcessor{
		log:        log,
		blocksRepo: blocksRepo,
		txsRepo:    txsRepo,
		logsRepo:   logsRepo,
		metrics:    metrics,
	}
}

// Process unmarshals msg.Value into a Coreth Block and logs its details.
// Returns an error if msg or msg.Value is nil, or if unmarshaling fails.
// Records processing duration and errors to metrics if configured.
func (p *CorethProcessor) Process(ctx context.Context, msg *cKafka.Message) error {
	start := time.Now()

	if msg == nil || msg.Value == nil {
		p.metrics.IncError("coreth_nil_message")
		return errors.New("received nil message or empty value")
	}

	var block kafkamsg.CorethBlock
	if err := block.Unmarshal(msg.Value); err != nil {
		p.metrics.IncError("coreth_unmarshal_error")
		return fmt.Errorf("failed to unmarshal coreth block: %w", err)
	}

	// Validate block (BlockchainID is required) - do this even if not persisting
	if block.BlockchainID == nil {
		return evmrepo.ErrBlockChainIDRequired
	}

	p.log.Debugw("processing coreth block",
		"evmChainID", block.EVMChainID,
		"bcID", block.BlockchainID,
		"blockNumber", block.Number,
		"hash", block.Hash,
	)

	// Persist block to ClickHouse if repository is configured
	if p.blocksRepo != nil {
		blockRow, err := CorethBlockToBlockRow(&block)
		if err != nil {
			p.metrics.IncError("coreth_parse_error")
			return fmt.Errorf("failed to parse block for storage: %w", err)
		}

		if err := p.blocksRepo.WriteBlock(ctx, blockRow); err != nil {
			p.metrics.IncError("coreth_write_error")
			return fmt.Errorf("failed to write block to ClickHouse: %w", err)
		}

		p.log.Debugw("successfully persisted block to ClickHouse",
			"evmChainID", blockRow.EVMChainID,
			"blockchainID", blockRow.BlockchainID,
			"blockNumber", blockRow.BlockNumber,
			"hash", blockRow.Hash,
		)
	}

	// Record successful processing duration
	p.metrics.ObserveBlockProcessingDuration(time.Since(start).Seconds())
	// Persist transactions to ClickHouse if repository is configured
	if p.txsRepo != nil && len(block.Transactions) > 0 {
		if err := p.processTransactions(ctx, &block); err != nil {
			return fmt.Errorf("failed to process transactions: %w", err)
		}
	}

	// Persist logs to ClickHouse if repository is configured
	if p.logsRepo != nil && len(block.Transactions) > 0 {
		if err := p.processLogs(ctx, &block); err != nil {
			return fmt.Errorf("failed to process logs: %w", err)
		}
	}

	return nil
}

// CorethBlockToBlockRow converts a kafkamsg.CorethBlock to BlockRow
// Exported for testing purposes
func CorethBlockToBlockRow(block *kafkamsg.CorethBlock) (*evmrepo.BlockRow, error) {
	// Validate blockchain ID
	if block.BlockchainID == nil {
		return nil, evmrepo.ErrBlockChainIDRequired
	}

	// Set BlockchainID and EVMChainID (default EVMChainID to 0 if not set)
	blockchainID := block.BlockchainID
	evmChainID := block.EVMChainID
	if evmChainID == nil {
		evmChainID = big.NewInt(0)
	}

	// Default BlockNumber to 0 if not set
	blockNumber := block.Number
	if blockNumber == nil {
		blockNumber = big.NewInt(0)
	}

	// Set difficulty from big.Int (keep as *big.Int)
	var difficulty, totalDifficulty *big.Int
	if block.Difficulty != nil {
		difficulty = block.Difficulty
		// TotalDifficulty: for now use Difficulty, but this should be cumulative in production
		totalDifficulty = new(big.Int).Set(block.Difficulty)
	} else {
		difficulty = big.NewInt(0)
		totalDifficulty = big.NewInt(0)
	}

	blockRow := &evmrepo.BlockRow{
		BlockchainID:    blockchainID,
		EVMChainID:      evmChainID,
		BlockNumber:     blockNumber,
		Hash:            block.Hash,
		ParentHash:      block.ParentHash,
		BlockTime:       time.Unix(int64(block.Timestamp), 0).UTC(),
		Miner:           block.Miner,
		Difficulty:      difficulty,
		TotalDifficulty: totalDifficulty,
		Size:            block.Size,
		GasLimit:        block.GasLimit,
		GasUsed:         block.GasUsed,
		BaseFeePerGas:   block.BaseFee,
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

	// Optional fields - keep as *big.Int
	if block.BaseFee != nil {
		blockRow.BaseFeePerGas = block.BaseFee
	} else {
		blockRow.BaseFeePerGas = big.NewInt(0)
	}
	// BlockGasCost defaults to 0 for now (not in kafkamsg.CorethBlock yet)
	blockRow.BlockGasCost = big.NewInt(0)
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

	return blockRow, nil
}

// CorethTransactionToTransactionRow converts a coreth.Transaction to TransactionRow
// Exported for testing purposes
func CorethTransactionToTransactionRow(
	tx *kafkamsg.CorethTransaction,
	block *kafkamsg.CorethBlock,
	txIndex uint64,
) (*evmrepo.TransactionRow, error) {
	// Extract blockchain ID from block
	if block.BlockchainID == nil {
		return nil, evmrepo.ErrBlockChainIDRequiredForTx
	}

	// Extract block number
	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	// Set BlockchainID and EVMChainID from block (default EVMChainID to 0 if not set)
	blockchainID := block.BlockchainID
	evmChainID := block.EVMChainID
	if evmChainID == nil {
		evmChainID = big.NewInt(0)
	}

	txRow := &evmrepo.TransactionRow{
		BlockchainID:     blockchainID,
		EVMChainID:       evmChainID,
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
		Success:          0, // TODO: Extract from transaction receipt when available in CorethBlock
	}

	// Handle nullable To field
	if tx.To != "" {
		txRow.To = &tx.To
	}

	// Set big.Int values directly (keep as *big.Int)
	if tx.Value != nil {
		txRow.Value = tx.Value
	} else {
		txRow.Value = big.NewInt(0)
	}

	if tx.GasPrice != nil {
		txRow.GasPrice = tx.GasPrice
	} else {
		txRow.GasPrice = big.NewInt(0)
	}

	// Handle nullable MaxFeePerGas
	if tx.MaxFeePerGas != nil {
		txRow.MaxFeePerGas = tx.MaxFeePerGas
	}

	// Handle nullable MaxPriorityFee
	if tx.MaxPriorityFee != nil {
		txRow.MaxPriorityFee = tx.MaxPriorityFee
	}

	return txRow, nil
}

// processTransactions converts transactions from a kafkamsg.CorethBlock to TransactionRow and writes
// them to ClickHouse
func (p *CorethProcessor) processTransactions(
	ctx context.Context,
	block *kafkamsg.CorethBlock,
) error {
	// Convert and write each transaction
	// TODO: Add batching (in a future PR)
	totalLogs := 0
	for i, tx := range block.Transactions {
		txRow, err := CorethTransactionToTransactionRow(tx, block, uint64(i))
		if err != nil {
			return fmt.Errorf("failed to convert transaction %d: %w", i, err)
		}

		if err := p.txsRepo.WriteTransaction(ctx, txRow); err != nil {
			return fmt.Errorf("failed to write transaction %s: %w", tx.Hash, err)
		}

		// Count logs from this transaction's receipt
		if tx.Receipt != nil {
			totalLogs += len(tx.Receipt.Logs)
		}
	}

	// Record logs processed metric
	p.metrics.AddLogsProcessed(totalLogs)

	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	p.log.Debugw("successfully wrote transactions",
		"blockchainID", block.BlockchainID,
		"evmChainID", block.EVMChainID,
		"blockNumber", blockNumber,
		"transactionCount", len(block.Transactions),
		"logCount", totalLogs,
	)

	return nil
}

// processLogs extracts logs from transaction receipts and writes them to ClickHouse
func (p *CorethProcessor) processLogs(
	ctx context.Context,
	block *kafkamsg.CorethBlock,
) error {
	totalLogs := 0
	for _, tx := range block.Transactions {
		if tx.Receipt == nil || len(tx.Receipt.Logs) == 0 {
			continue
		}

		for _, log := range tx.Receipt.Logs {
			logRow, err := CorethLogToLogRow(log, block)
			if err != nil {
				return fmt.Errorf("failed to convert log: %w", err)
			}

			if err := p.logsRepo.WriteLog(ctx, logRow); err != nil {
				return fmt.Errorf("failed to write log (tx: %s, index: %d): %w", tx.Hash, log.Index, err)
			}
			totalLogs++
		}
	}

	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	p.log.Debugw("successfully wrote logs",
		"blockchainID", block.BlockchainID,
		"evmChainID", block.EVMChainID,
		"blockNumber", blockNumber,
		"logCount", totalLogs,
	)

	return nil
}

// CorethLogToLogRow converts a CorethLog to LogRow
// Exported for testing purposes
func CorethLogToLogRow(
	log *kafkamsg.CorethLog,
	block *kafkamsg.CorethBlock,
) (*evmrepo.LogRow, error) {
	if block.BlockchainID == nil {
		return nil, evmrepo.ErrBlockChainIDRequired
	}

	// Set BlockchainID and EVMChainID from block (default EVMChainID to 0 if not set)
	blockchainID := block.BlockchainID
	evmChainID := block.EVMChainID
	if evmChainID == nil {
		evmChainID = big.NewInt(0)
	}

	// Convert topics from []common.Hash to individual topic fields
	var topic0, topic1, topic2, topic3 *string
	if len(log.Topics) > 0 {
		t := log.Topics[0].Hex()
		topic0 = &t
	}
	if len(log.Topics) > 1 {
		t := log.Topics[1].Hex()
		topic1 = &t
	}
	if len(log.Topics) > 2 {
		t := log.Topics[2].Hex()
		topic2 = &t
	}
	if len(log.Topics) > 3 {
		t := log.Topics[3].Hex()
		topic3 = &t
	}

	// Determine removed flag
	var removed uint8
	if log.Removed {
		removed = 1
	}

	return &evmrepo.LogRow{
		BlockchainID: blockchainID,
		EVMChainID:   evmChainID,
		BlockNumber:  log.BlockNumber,
		BlockHash:    log.BlockHash.Hex(),
		BlockTime:    time.Unix(int64(block.Timestamp), 0).UTC(),
		TxHash:       log.TxHash.Hex(),
		TxIndex:      uint32(log.TxIndex),
		Address:      log.Address.Hex(),
		Topic0:       topic0,
		Topic1:       topic1,
		Topic2:       topic2,
		Topic3:       topic3,
		Data:         log.Data,
		LogIndex:     uint32(log.Index),
		Removed:      removed,
	}, nil
}

// Compile-time check that CorethProcessor implements Processor.
var _ Processor = (*CorethProcessor)(nil)
