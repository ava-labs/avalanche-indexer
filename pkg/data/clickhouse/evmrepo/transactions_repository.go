package evmrepo

import (
	"context"
	"fmt"

	_ "embed"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// Transactions provides methods to write transactions to ClickHouse
type Transactions interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteTransaction(ctx context.Context, tx *TransactionRow) error
	DeleteTransactions(ctx context.Context, chainID uint64) error
}

//go:embed queries/transaction/create-transactions-table-local.sql
var createTransactionsTableLocalQuery string

//go:embed queries/transaction/create-transactions-table.sql
var createTransactionsTableQuery string

//go:embed queries/transaction/write-transaction.sql
var writeTransactionQuery string

//go:embed queries/transaction/delete-transactions.sql
var deleteTransactionsQuery string

type transactions struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

// NewTransactions creates a new raw transactions repository and initializes the table
func NewTransactions(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (Transactions, error) {
	repo := &transactions{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize transactions table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the raw_transactions table if it doesn't exist
func (r *transactions) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createTransactionsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create transactions local table: %w", err)
	}

	query = fmt.Sprintf(createTransactionsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}
	return nil
}

// WriteTransaction inserts a raw transaction into ClickHouse
func (r *transactions) WriteTransaction(ctx context.Context, tx *TransactionRow) error {
	query := fmt.Sprintf(writeTransactionQuery, r.database, r.tableName)

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
	)
	if err != nil {
		return fmt.Errorf("failed to write transaction: %w", err)
	}
	return nil
}

func (r *transactions) DeleteTransactions(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteTransactionsQuery, r.database, r.tableName, r.cluster)
	if err := r.client.Conn().Exec(ctx, query, chainID); err != nil {
		return fmt.Errorf("failed to delete transactions: %w", err)
	}
	return nil
}
