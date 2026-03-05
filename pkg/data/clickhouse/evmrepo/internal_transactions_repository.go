package evmrepo

import (
	"context"
	"fmt"

	_ "embed"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// InternalTransactions provides methods to write internal transactions to ClickHouse
type InternalTransactions interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteInternalTransaction(ctx context.Context, tx *InternalTransactionRow) error
	DeleteInternalTransactions(ctx context.Context, chainID uint64) error
}

//go:embed queries/internal_transaction/create-internal-transactions-table-local.sql
var createInternalTransactionsTableLocalQuery string

//go:embed queries/internal_transaction/create-internal-transactions-table.sql
var createInternalTransactionsTableQuery string

//go:embed queries/internal_transaction/write-internal-transaction.sql
var writeInternalTransactionQuery string

//go:embed queries/internal_transaction/delete-internal-transactions.sql
var deleteInternalTransactionsQuery string

type internalTransactions struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

// NewInternalTransactions creates a new internal transactions repository and initializes the table
func NewInternalTransactions(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (InternalTransactions, error) {
	repo := &internalTransactions{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize internal_transactions table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the internal_transactions table if it doesn't exist
func (r *internalTransactions) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createInternalTransactionsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create internal_transactions local table: %w", err)
	}

	query = fmt.Sprintf(createInternalTransactionsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create internal_transactions table: %w", err)
	}
	return nil
}

// WriteInternalTransaction inserts an internal transaction into ClickHouse
func (r *internalTransactions) WriteInternalTransaction(ctx context.Context, tx *InternalTransactionRow) error {
	query := fmt.Sprintf(writeInternalTransactionQuery, r.database, r.tableName)

	// Convert BlockchainID
	var blockchainID interface{}
	if tx.BlockchainID != nil {
		blockchainID = *tx.BlockchainID
	} else {
		blockchainID = ""
	}

	// Convert EVMChainID to string for ClickHouse UInt256
	evmChainIDStr := "0"
	if tx.EVMChainID != nil {
		evmChainIDStr = tx.EVMChainID.String()
	}

	// Convert transaction hash hex string to bytes
	txHashBytes, err := utils.HexToBytes32(tx.TransactionHash)
	if err != nil {
		return fmt.Errorf("failed to convert transaction_hash to bytes: %w", err)
	}

	err = r.client.Conn().Exec(ctx, query,
		blockchainID,
		evmChainIDStr,
		tx.BlockNumber,
		tx.BlockTime,
		string(txHashBytes[:]),
		tx.Type,
		string(tx.From[:]),
		string(tx.To[:]),
		tx.Value,
		tx.Gas,
		tx.GasUsed,
		tx.Revert,
		tx.Error,
		tx.RevertReason,
		tx.Input,
		tx.Output,
		tx.CallIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to write internal transaction: %w", err)
	}
	return nil
}

func (r *internalTransactions) DeleteInternalTransactions(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteInternalTransactionsQuery, r.database, r.tableName, r.cluster)
	if err := r.client.Conn().Exec(ctx, query, chainID); err != nil {
		return fmt.Errorf("failed to delete internal transactions: %w", err)
	}
	return nil
}
