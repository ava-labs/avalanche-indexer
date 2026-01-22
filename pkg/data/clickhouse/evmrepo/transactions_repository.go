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
	client        clickhouse.Client
	tableName     string
	batchInserter *BatchInserter // Optional: if set, uses batching; if nil, uses direct inserts
}

// NewTransactions creates a new raw transactions repository and initializes the table
// If batchInserter is provided, writes will be batched; otherwise, direct inserts are used
func NewTransactions(ctx context.Context, client clickhouse.Client, tableName string, batchInserter *BatchInserter) (Transactions, error) {
	repo := &transactions{
		client:        client,
		tableName:     tableName,
		batchInserter: batchInserter,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize transactions table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the raw_transactions table if it doesn't exist
func (r *transactions) CreateTableIfNotExists(ctx context.Context) error {
	query := CreateTransactionsTableQuery(r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}
	return nil
}

// WriteTransaction inserts a raw transaction into ClickHouse
// If batchInserter is set, adds to batch; otherwise, performs direct insert
func (r *transactions) WriteTransaction(ctx context.Context, tx *TransactionRow) error {
	// Use batching if batch inserter is available
	if r.batchInserter != nil {
		return r.batchInserter.AddTransaction(ctx, tx)
	}

	// Otherwise, use direct insert (for testing or table creation scenarios)
	query := TransactionInsertQuery(r.tableName)

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
		evmChainIDStr,
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
