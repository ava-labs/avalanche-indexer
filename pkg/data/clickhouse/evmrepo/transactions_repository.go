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
			blockchain_id String,
			evm_chain_id UInt32,
			block_number UInt64,
			block_hash String,
			block_time DateTime64(3, 'UTC'),
			hash String,
			from_address String,
			to_address Nullable(String),
			nonce UInt64,
			value UInt256,
			gas UInt64,
			gas_price UInt256,
			max_fee_per_gas Nullable(UInt256),
			max_priority_fee Nullable(UInt256),
			input String,
			type UInt8,
			transaction_index UInt64
		)
		ENGINE = MergeTree
		ORDER BY (blockchain_id, block_time, hash)
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
			blockchain_id, evm_chain_id, block_number, block_hash, block_time, hash,
			from_address, to_address, nonce, value, gas, gas_price,
			max_fee_per_gas, max_priority_fee, input, type, transaction_index
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.tableName)

	// Convert BlockchainID (string) and EVMChainID (*big.Int) for ClickHouse
	var blockchainID interface{}
	if tx.BlockchainID != nil {
		blockchainID = *tx.BlockchainID
	} else {
		blockchainID = ""
	}
	var evmChainID uint32
	if tx.EVMChainID != nil {
		evmChainID = uint32(tx.EVMChainID.Uint64())
	}

	// Convert *big.Int to string for ClickHouse UInt256 fields
	// ClickHouse accepts UInt256 as string representation
	valueStr := "0"
	if tx.Value != nil {
		valueStr = tx.Value.String()
	}
	gasPriceStr := "0"
	if tx.GasPrice != nil {
		gasPriceStr = tx.GasPrice.String()
	}
	var maxFeePerGasStr interface{}
	if tx.MaxFeePerGas != nil {
		maxFeePerGasStr = tx.MaxFeePerGas.String()
	} else {
		maxFeePerGasStr = nil
	}
	var maxPriorityFeeStr interface{}
	if tx.MaxPriorityFee != nil {
		maxPriorityFeeStr = tx.MaxPriorityFee.String()
	} else {
		maxPriorityFeeStr = nil
	}

	err := r.client.Conn().Exec(ctx, query,
		blockchainID,
		evmChainID,
		tx.BlockNumber,
		tx.BlockHash,
		tx.BlockTime,
		tx.Hash,
		tx.From,
		tx.To,
		tx.Nonce,
		valueStr,
		tx.Gas,
		gasPriceStr,
		maxFeePerGasStr,
		maxPriorityFeeStr,
		tx.Input,
		tx.Type,
		tx.TransactionIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to write transaction: %w", err)
	}
	return nil
}
