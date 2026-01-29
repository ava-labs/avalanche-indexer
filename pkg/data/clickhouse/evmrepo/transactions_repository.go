package evmrepo

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
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
			evm_chain_id UInt256,
			block_number UInt64,
			block_hash FixedString(32),
			block_time DateTime64(3, 'UTC'),
			hash FixedString(32),
			from_address FixedString(20),
			to_address Nullable(FixedString(20)),
			nonce UInt64,
			value UInt256,
			gas UInt64,
			gas_price UInt256,
			max_fee_per_gas Nullable(UInt256),
			max_priority_fee Nullable(UInt256),
			input String,
			type UInt8,
			transaction_index UInt64,
			success UInt8,
			month INTEGER
		)
		ENGINE = MergeTree
		PARTITION BY (toString(evm_chain_id), month)
		ORDER BY (block_time, hash)
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
			max_fee_per_gas, max_priority_fee, input, type, transaction_index, success, month
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.tableName)

	// Convert BlockchainID (string) and EVMChainID (*big.Int) for ClickHouse
	var blockchainID interface{}
	if tx.BlockchainID != nil {
		blockchainID = *tx.BlockchainID
	} else {
		blockchainID = ""
	}
	// Convert *big.Int to string for ClickHouse UInt256 fields
	// ClickHouse accepts UInt256 as string representation
	evmChainIDStr := "0"
	if tx.EVMChainID != nil {
		evmChainIDStr = tx.EVMChainID.String()
	}

	// Convert hex strings to bytes for FixedString fields
	blockHashBytes, err := utils.HexToBytes32(tx.BlockHash)
	if err != nil {
		return fmt.Errorf("failed to convert block_hash to bytes: %w", err)
	}
	hashBytes, err := utils.HexToBytes32(tx.Hash)
	if err != nil {
		return fmt.Errorf("failed to convert hash to bytes: %w", err)
	}
	fromBytes, err := utils.HexToBytes20(tx.From)
	if err != nil {
		return fmt.Errorf("failed to convert from_address to bytes: %w", err)
	}

	// For nullable to_address - convert empty string to nil, otherwise convert to bytes then string
	var toBytes interface{}
	if tx.To == nil || *tx.To == "" {
		toBytes = nil
	} else {
		to, err := utils.HexToBytes20(*tx.To)
		if err != nil {
			return fmt.Errorf("failed to convert to_address to bytes: %w", err)
		}
		toBytes = string(to[:])
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

	// Calculate month as YYYYMM from block_time
	month := utils.MonthFromTime(tx.BlockTime)

	err = r.client.Conn().Exec(ctx, query,
		blockchainID,
		evmChainIDStr,
		tx.BlockNumber,
		string(blockHashBytes[:]),
		tx.BlockTime,
		string(hashBytes[:]),
		string(fromBytes[:]),
		toBytes,
		tx.Nonce,
		valueStr,
		tx.Gas,
		gasPriceStr,
		maxFeePerGasStr,
		maxPriorityFeeStr,
		tx.Input,
		tx.Type,
		tx.TransactionIndex,
		tx.Success,
		month,
	)
	if err != nil {
		return fmt.Errorf("failed to write transaction: %w", err)
	}
	return nil
}
