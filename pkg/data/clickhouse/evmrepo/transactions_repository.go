package evmrepo

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Transactions provides methods to write transactions to ClickHouse
type Transactions interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteTransaction(ctx context.Context, tx *TransactionRow) error
}

type transactions struct {
	client    clickhouse.Client
	tableName string
}

// NewTransactions creates a new raw transactions repository and initializes the table
func NewTransactions(ctx context.Context, client clickhouse.Client, tableName string) (Transactions, error) {
	repo := &transactions{
		client:    client,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize transactions table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the raw_transactions table if it doesn't exist
func (r *transactions) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			bc_id UInt32,
			evm_id UInt32,
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
		ORDER BY (bc_id, block_time, hash)
		SETTINGS index_granularity = 8192
	`, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}
	return nil
}

// WriteTransaction inserts a raw transaction into ClickHouse
func (r *transactions) WriteTransaction(ctx context.Context, tx *TransactionRow) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			bc_id, evm_id, block_number, block_hash, block_time, hash,
			from_address, to_address, nonce, value, gas, gas_price,
			max_fee_per_gas, max_priority_fee, input, type, transaction_index
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.tableName)

	// Convert *big.Int to uint32 for ClickHouse
	var bcID uint32
	if tx.BcID != nil {
		bcID = uint32(tx.BcID.Uint64())
	}
	var evmID uint32
	if tx.EvmID != nil {
		evmID = uint32(tx.EvmID.Uint64())
	}

	err := r.client.Conn().Exec(ctx, query,
		bcID,
		evmID,
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
