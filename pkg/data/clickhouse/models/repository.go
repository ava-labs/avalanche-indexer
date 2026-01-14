package models

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Repository provides methods to write data to ClickHouse
type Repository interface {
	WriteBlock(ctx context.Context, block *RawBlock) error
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
func (r *repository) WriteBlock(ctx context.Context, block *RawBlock) error {
	// Commenting out tail-end fields to isolate the LowCardinality issue
	query := fmt.Sprintf(`
		INSERT INTO %s (
			chain_id, block_number, hash, parent_hash, block_time, miner,
			difficulty, total_difficulty, size, gas_limit, gas_used,
			base_fee_per_gas, block_gas_cost, state_root, transactions_root, receipts_root,
			extra_data, mix_hash, nonce, sha3_uncles
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.tableName)

	// Convert FixedString byte arrays to strings (each byte becomes a character)
	hashStr := string(block.Hash[:])
	parentHashStr := string(block.ParentHash[:])
	minerStr := string(block.Miner[:])
	stateRootStr := string(block.StateRoot[:])
	transactionsRootStr := string(block.TransactionsRoot[:])
	receiptsRootStr := string(block.ReceiptsRoot[:])
	mixHashStr := string(block.MixHash[:])
	sha3UnclesStr := string(block.Sha3Uncles[:])

	// For LowCardinality(Nullable(FixedString(8))) nonce - convert to nil if all zeros
	var nonceStr interface{}
	if block.Nonce == [8]byte{} {
		nonceStr = nil
	} else {
		nonceStr = string(block.Nonce[:])
	}

	err := r.client.Conn().Exec(ctx, query,
		block.ChainID,
		block.BlockNumber,
		hashStr,
		parentHashStr,
		block.BlockTime,
		minerStr,
		block.Difficulty,
		block.TotalDifficulty,
		block.Size,
		block.GasLimit,
		block.GasUsed,
		block.BaseFeePerGas,
		block.BlockGasCost,
		stateRootStr,
		transactionsRootStr,
		receiptsRootStr,
		block.ExtraData,
		mixHashStr,
		nonceStr,
		sha3UnclesStr,
	)
	if err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}
	return nil
}
