package models

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// TransactionsRepository provides methods to write transactions to ClickHouse
type TransactionsRepository interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteTransaction(ctx context.Context, tx *ClickhouseTransaction) error
}

type transactionsRepository struct {
	client    clickhouse.Client
	tableName string
}

// NewTransactionsRepository creates a new raw transactions repository
func NewTransactionsRepository(client clickhouse.Client, tableName string) TransactionsRepository {
	return &transactionsRepository{
		client:    client,
		tableName: tableName,
	}
}

// CreateTableIfNotExists creates the raw_transactions table if it doesn't exist
func (r *transactionsRepository) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			chain_id UInt32,
			block_number UInt64,
			block_hash String,
			block_time DateTime64(3, 'UTC'),
			hash String,
			from_address String,
			to_address Nullable(String),
			nonce UInt64,
			value String,
			gas UInt64,
			gas_price String,
			max_fee_per_gas Nullable(String),
			max_priority_fee Nullable(String),
			input String,
			type UInt8,
			transaction_index UInt64
		)
		ENGINE = MergeTree
		ORDER BY (chain_id, block_time, hash)
		SETTINGS index_granularity = 8192
	`, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}
	return nil
}

// WriteTransaction inserts a raw transaction into ClickHouse
func (r *transactionsRepository) WriteTransaction(ctx context.Context, tx *ClickhouseTransaction) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			chain_id, block_number, block_hash, block_time, hash,
			from_address, to_address, nonce, value, gas, gas_price,
			max_fee_per_gas, max_priority_fee, input, type, transaction_index
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.tableName)

	err := r.client.Conn().Exec(ctx, query,
		tx.ChainID,
		tx.BlockNumber,
		tx.BlockHash,
		tx.BlockTime,
		tx.Hash,
		tx.From,
		tx.To,
		tx.Nonce,
		tx.Value,
		tx.Gas,
		tx.GasPrice,
		tx.MaxFeePerGas,
		tx.MaxPriorityFee,
		tx.Input,
		tx.Type,
		tx.TransactionIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to write transaction: %w", err)
	}
	return nil
}
