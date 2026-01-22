package evmrepo

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Blocks provides methods to write blocks to ClickHouse
type Blocks interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteBlock(ctx context.Context, block *BlockRow) error
}

type blocks struct {
	client        clickhouse.Client
	tableName     string
	batchInserter *BatchInserter // Optional: if set, uses batching; if nil, uses direct inserts
}

// NewBlocks creates a new raw blocks repository and initializes the table
// If batchInserter is provided, writes will be batched; otherwise, direct inserts are used
func NewBlocks(ctx context.Context, client clickhouse.Client, tableName string, batchInserter *BatchInserter) (Blocks, error) {
	repo := &blocks{
		client:        client,
		tableName:     tableName,
		batchInserter: batchInserter,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize blocks table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the raw_blocks table if it doesn't exist
func (r *blocks) CreateTableIfNotExists(ctx context.Context) error {
	query := CreateBlocksTableQuery(r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create blocks table: %w", err)
	}
	return nil
}

// WriteBlock inserts a raw block into ClickHouse
// If batchInserter is set, adds to batch; otherwise, performs direct insert
func (r *blocks) WriteBlock(ctx context.Context, block *BlockRow) error {
	// Use batching if batch inserter is available
	if r.batchInserter != nil {
		return r.batchInserter.AddBlock(ctx, block)
	}

	// Otherwise, use direct insert (for testing or table creation scenarios)
	query := BlockInsertQuery(r.tableName)

	// For nullable nonce - convert empty string to nil
	var nonceStr interface{}
	if block.Nonce == "" {
		nonceStr = nil
	} else {
		nonceStr = block.Nonce
	}

	// For nullable parent_beacon_block_root - convert empty string to nil
	var parentBeaconBlockRootStr interface{}
	if block.ParentBeaconBlockRoot == "" {
		parentBeaconBlockRootStr = nil
	} else {
		parentBeaconBlockRootStr = block.ParentBeaconBlockRoot
	}

	// Convert BlockchainID (string) and EVMChainID (*big.Int) for ClickHouse
	var blockchainID interface{}
	if block.BlockchainID != nil {
		blockchainID = *block.BlockchainID
	} else {
		blockchainID = ""
	}
	// Convert *big.Int to string for ClickHouse UInt256 fields
	// ClickHouse accepts UInt256 as string representation
	evmChainIDStr := "0"
	if block.EVMChainID != nil {
		evmChainIDStr = block.EVMChainID.String()
	}

	// Convert BlockNumber from *big.Int to uint64 for ClickHouse UInt64
	var blockNumber uint64
	if block.BlockNumber != nil {
		blockNumber = block.BlockNumber.Uint64()
	}

	// Convert *big.Int to string for ClickHouse UInt256 fields
	// ClickHouse accepts UInt256 as string representation
	difficultyStr := "0"
	if block.Difficulty != nil {
		difficultyStr = block.Difficulty.String()
	}
	totalDifficultyStr := "0"
	if block.TotalDifficulty != nil {
		totalDifficultyStr = block.TotalDifficulty.String()
	}
	baseFeeStr := "0"
	if block.BaseFeePerGas != nil {
		baseFeeStr = block.BaseFeePerGas.String()
	}
	blockGasCostStr := "0"
	if block.BlockGasCost != nil {
		blockGasCostStr = block.BlockGasCost.String()
	}

	err := r.client.Conn().Exec(ctx, query,
		blockchainID,
		evmChainIDStr,
		blockNumber,
		block.Hash,
		block.ParentHash,
		block.BlockTime,
		block.Miner,
		difficultyStr,
		totalDifficultyStr,
		block.Size,
		block.GasLimit,
		block.GasUsed,
		baseFeeStr,
		blockGasCostStr,
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
		return fmt.Errorf("failed to write block: %w", err)
	}
	return nil
}
