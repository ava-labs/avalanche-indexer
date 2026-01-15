package models

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// BlocksRepository provides methods to write blocks to ClickHouse
type BlocksRepository interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteBlock(ctx context.Context, block *ClickhouseBlock) error
}

type blocksRepository struct {
	client    clickhouse.Client
	tableName string
}

// NewBlocksRepository creates a new raw blocks repository
func NewBlocksRepository(client clickhouse.Client, tableName string) BlocksRepository {
	return &blocksRepository{
		client:    client,
		tableName: tableName,
	}
}

// CreateTableIfNotExists creates the raw_blocks table if it doesn't exist
func (r *blocksRepository) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			chain_id UInt32,
			block_number UInt64,
			hash String,
			parent_hash String,
			block_time DateTime64(3, 'UTC'),
			miner String,
			difficulty UInt64,
			total_difficulty UInt64,
			size UInt64,
			gas_limit UInt64,
			gas_used UInt64,
			base_fee_per_gas UInt64,
			block_gas_cost UInt64,
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
		ORDER BY (chain_id, block_time, block_number)
		SETTINGS index_granularity = 8192
	`, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create blocks table: %w", err)
	}
	return nil
}

// WriteBlock inserts a raw block into ClickHouse
func (r *blocksRepository) WriteBlock(ctx context.Context, block *ClickhouseBlock) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			chain_id, block_number, hash, parent_hash, block_time, miner,
			difficulty, total_difficulty, size, gas_limit, gas_used,
			base_fee_per_gas, block_gas_cost, state_root, transactions_root, receipts_root,
			extra_data, block_extra_data, ext_data_hash, ext_data_gas_used,
			mix_hash, nonce, sha3_uncles, uncles,
			blob_gas_used, excess_blob_gas, parent_beacon_block_root, min_delay_excess
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

	err := r.client.Conn().Exec(ctx, query,
		block.ChainID,
		block.BlockNumber,
		block.Hash,
		block.ParentHash,
		block.BlockTime,
		block.Miner,
		block.Difficulty,
		block.TotalDifficulty,
		block.Size,
		block.GasLimit,
		block.GasUsed,
		block.BaseFeePerGas,
		block.BlockGasCost,
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
