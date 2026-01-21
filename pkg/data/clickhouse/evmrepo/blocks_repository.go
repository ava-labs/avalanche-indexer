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
	client    clickhouse.Client
	tableName string
}

// NewBlocks creates a new raw blocks repository and initializes the table
func NewBlocks(ctx context.Context, client clickhouse.Client, tableName string) (Blocks, error) {
	repo := &blocks{
		client:    client,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize blocks table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the raw_blocks table if it doesn't exist
func (r *blocks) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			blockchain_id String,
			evm_chain_id UInt256,
			block_number UInt64,
			hash String,
			parent_hash String,
			block_time DateTime64(3, 'UTC'),
			miner String,
			difficulty UInt256,
			total_difficulty UInt256,
			size UInt64,
			gas_limit UInt64,
			gas_used UInt64,
			base_fee_per_gas UInt256,
			block_gas_cost UInt256,
			state_root String,
			transactions_root String,
			receipts_root String,
			extra_data String,
			block_extra_data String,
			ext_data_hash String,
			ext_data_gas_used UInt32,
			mix_hash String,
			nonce Nullable(String),
			sha3_uncles String,
			uncles Array(String),
			blob_gas_used UInt64,
			excess_blob_gas UInt64,
			parent_beacon_block_root Nullable(String),
			min_delay_excess UInt64
		)
		ENGINE = MergeTree
		ORDER BY (blockchain_id, block_time, block_number)
		SETTINGS index_granularity = 8192
	`, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create blocks table: %w", err)
	}
	return nil
}

// WriteBlock inserts a raw block into ClickHouse
func (r *blocks) WriteBlock(ctx context.Context, block *BlockRow) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			blockchain_id, evm_chain_id, block_number, hash, parent_hash, block_time, miner,
			difficulty, total_difficulty, size, gas_limit, gas_used,
			base_fee_per_gas, block_gas_cost, state_root, transactions_root, receipts_root,
			extra_data, block_extra_data, ext_data_hash, ext_data_gas_used,
			mix_hash, nonce, sha3_uncles, uncles,
			blob_gas_used, excess_blob_gas, parent_beacon_block_root, min_delay_excess
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.tableName)

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
