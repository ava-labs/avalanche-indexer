package processor

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/types/coreth"
	"go.uber.org/zap"

	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// CorethProcessor unmarshals and logs Coreth blocks from Kafka messages.
// If repositories are provided, persists blocks and transactions to ClickHouse.
// Safe for concurrent use.
type CorethProcessor struct {
	log        *zap.SugaredLogger
	blocksRepo evmrepo.Blocks
	txsRepo    evmrepo.Transactions
}

// NewCorethProcessor creates a new CorethProcessor with the given logger.
// If repositories are provided, blocks and transactions will be persisted to ClickHouse.
func NewCorethProcessor(
	log *zap.SugaredLogger,
	blocksRepo evmrepo.Blocks,
	txsRepo evmrepo.Transactions,
) *CorethProcessor {
	return &CorethProcessor{
		log:        log,
		blocksRepo: blocksRepo,
		txsRepo:    txsRepo,
	}
}

// Process unmarshals msg.Value into a Coreth Block and logs its details.
// Returns an error if msg or msg.Value is nil, or if unmarshaling fails.
// Currently only logs block data - actual indexing/storage logic is TODO.
func (p *CorethProcessor) Process(ctx context.Context, msg *cKafka.Message) error {
	if msg == nil || msg.Value == nil {
		return errors.New("received nil message or empty value")
	}

	var block coreth.Block
	if err := block.Unmarshal(msg.Value); err != nil {
		return fmt.Errorf("failed to unmarshal coreth block: %w", err)
	}

	// Validate block (BlockchainID is required) - do this even if not persisting
	if block.BlockchainID == nil {
		return evmrepo.ErrBlockChainIDRequired
	}

	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	p.log.Debugw("processing coreth block",
		"evmChainID", block.EVMChainID,
		"blockchainID", block.BlockchainID,
		"blockNumber", blockNumber,
		"hash", block.Hash,
	)

	// Persist block to ClickHouse if repository is configured
	if p.blocksRepo != nil {
		blockRow, err := CorethBlockToBlockRow(&block)
		if err != nil {
			return fmt.Errorf("failed to convert block for storage: %w", err)
		}

		if err := p.blocksRepo.WriteBlock(ctx, blockRow); err != nil {
			return fmt.Errorf("failed to write block to ClickHouse: %w", err)
		}

		p.log.Debugw("successfully persisted block to ClickHouse",
			"evmChainID", blockRow.EVMChainID,
			"blockchainID", blockRow.BlockchainID,
			"blockNumber", blockRow.BlockNumber,
			"hash", blockRow.Hash,
		)
	}

	// Persist transactions to ClickHouse if repository is configured
	if p.txsRepo != nil && len(block.Transactions) > 0 {
		if err := p.processTransactions(ctx, &block); err != nil {
			return fmt.Errorf("failed to process transactions: %w", err)
		}
	}

	return nil
}

// CorethBlockToBlockRow converts a coreth.Block to BlockRow
// Exported for testing purposes
func CorethBlockToBlockRow(block *coreth.Block) (*evmrepo.BlockRow, error) {
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

	blockRow := &evmrepo.BlockRow{
		BlockchainID:    blockchainID,
		EVMChainID:      evmChainID,
		BlockNumber:     blockNumber,
		Hash:            block.Hash,
		ParentHash:      block.ParentHash,
		BlockTime:       time.Unix(int64(block.Timestamp), 0).UTC(),
		Miner:           block.Miner,
		Difficulty:      block.Difficulty,
		TotalDifficulty: new(big.Int).Set(block.Difficulty),
		Size:            block.Size,
		GasLimit:        block.GasLimit,
		GasUsed:         block.GasUsed,
		BaseFeePerGas:   block.BaseFee,
	}

	// Set difficulty from big.Int (keep as *big.Int)
	if block.Difficulty != nil {
		blockRow.Difficulty = block.Difficulty
		// TotalDifficulty: for now use Difficulty, but this should be cumulative in production
		blockRow.TotalDifficulty = new(big.Int).Set(block.Difficulty)
	} else {
		blockRow.Difficulty = big.NewInt(0)
		blockRow.TotalDifficulty = big.NewInt(0)
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
	// BlockGasCost defaults to 0 for now (not in coreth.Block yet)
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
	tx *coreth.Transaction,
	block *coreth.Block,
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

// processTransactions converts transactions from a coreth.Block to TransactionRow and writes
// them to ClickHouse
func (p *CorethProcessor) processTransactions(
	ctx context.Context,
	block *coreth.Block,
) error {
	// Convert and write each transaction
	// TODO: Add batching (in a future PR)
	for i, tx := range block.Transactions {
		txRow, err := CorethTransactionToTransactionRow(tx, block, uint64(i))
		if err != nil {
			return fmt.Errorf("failed to convert transaction %d: %w", i, err)
		}

		if err := p.txsRepo.WriteTransaction(ctx, txRow); err != nil {
			return fmt.Errorf("failed to write transaction %s: %w", tx.Hash, err)
		}
	}

	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	p.log.Debugw("successfully wrote transactions",
		"blockchainID", block.BlockchainID,
		"evmChainID", block.EVMChainID,
		"blockNumber", blockNumber,
		"transactionCount", len(block.Transactions),
	)

	return nil
}

// Compile-time check that CorethProcessor implements Processor.
var _ Processor = (*CorethProcessor)(nil)
