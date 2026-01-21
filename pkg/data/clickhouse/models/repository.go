package models

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Repository provides methods to write data to ClickHouse
type Repository interface {
	WriteBlock(ctx context.Context, block *ClickhouseBlock) error
}

type repository struct {
	client    clickhouse.Client
	tableName string
}

// NewRepository creates a new raw blocks repository
func NewRepository(client clickhouse.Client, tableName string) Repository {
	return &repository{
		client:    client,
		tableName: tableName,
	}
}

// WriteBlock inserts a raw block into ClickHouse
func (r *repository) WriteBlock(ctx context.Context, block *ClickhouseBlock) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			evm_chain_id, blockchain_id, block_number, hash, parent_hash, block_time, miner,
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

	err := r.client.Conn().Exec(ctx, query,
		block.EVMChainID,
		block.BlockchainID,
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
