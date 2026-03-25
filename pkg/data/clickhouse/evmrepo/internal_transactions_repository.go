package evmrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	_ "embed"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// InternalTransactions provides methods to write internal transactions to ClickHouse
type InternalTransactions interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteInternalTransaction(ctx context.Context, tx *InternalTransactionRow) error
	BatchInsertInternalTransactions(ctx context.Context, txs []*InternalTransactionRow) error
	DeleteInternalTransactions(ctx context.Context, chainID uint64) error
}

//go:embed queries/internal_transaction/create-internal-transactions-table-local.sql
var createInternalTransactionsTableLocalQuery string

//go:embed queries/internal_transaction/create-internal-transactions-table.sql
var createInternalTransactionsTableQuery string

//go:embed queries/internal_transaction/write-internal-transaction.sql
var writeInternalTransactionQuery string

//go:embed queries/internal_transaction/batch-insert-internal-transactions.sql
var batchInsertInternalTransactionsQuery string

//go:embed queries/internal_transaction/delete-internal-transactions.sql
var deleteInternalTransactionsQuery string

type internalTransactions struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chInternalTransactionRow struct {
	blockchainID    interface{}
	evmChainID      string
	blockNumber     uint64
	blockTime       time.Time
	timestampMs     uint64
	transactionHash string
	transactionType string
	fromAddress     string
	toAddress       string
	value           string
	gas             string
	gasUsed         string
	revert          bool
	error           string
	revertReason    string
	input           string
	output          string
	callIndex       string
}

func convertIntTxnRowToIntChTransactionRow(tx *InternalTransactionRow) (*chInternalTransactionRow, error) {
	if tx == nil {
		return nil, errors.New("internal transaction is nil")
	}

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
		return nil, fmt.Errorf("failed to convert transaction_hash to bytes: %w", err)
	}

	return &chInternalTransactionRow{
		blockchainID:    blockchainID,
		evmChainID:      evmChainIDStr,
		blockNumber:     tx.BlockNumber,
		blockTime:       tx.BlockTime,
		timestampMs:     tx.TimestampMs,
		transactionHash: string(txHashBytes[:]),
		transactionType: tx.Type,
		fromAddress:     string(tx.From[:]),
		toAddress:       string(tx.To[:]),
		value:           tx.Value,
		gas:             tx.Gas,
		gasUsed:         tx.GasUsed,
		revert:          tx.Revert,
		error:           tx.Error,
		revertReason:    tx.RevertReason,
		input:           tx.Input,
		output:          tx.Output,
		callIndex:       tx.CallIndex,
	}, nil
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

// CreateTableIfNotExists creates the internal_transactions table if it doesn't exist,
// then runs all numbered migrations from queries/migrations/internal_transaction/ to ensure
// the schema is up to date for existing tables.
func (r *internalTransactions) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createInternalTransactionsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create internal_transactions local table: %w", err)
	}

	query = fmt.Sprintf(createInternalTransactionsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create internal_transactions table: %w", err)
	}

	if err := clickhouse.RunMigrations(ctx, r.client.Conn(), internalTransactionsMigrationsFS, "queries/migrations/internal_transaction", r.database, r.tableName, r.cluster); err != nil {
		return fmt.Errorf("failed to run internal transactions migrations: %w", err)
	}

	return nil
}

// WriteInternalTransaction inserts an internal transaction into ClickHouse
func (r *internalTransactions) WriteInternalTransaction(ctx context.Context, tx *InternalTransactionRow) error {
	query := fmt.Sprintf(writeInternalTransactionQuery, r.database, r.tableName)

	row, err := convertIntTxnRowToIntChTransactionRow(tx)
	if err != nil {
		return fmt.Errorf("failed to convert internal transaction row of block %d and txHash %s to row: %w", tx.BlockNumber, tx.TransactionHash, err)
	}

	err = r.client.Conn().Exec(ctx, query,
		row.blockchainID,
		row.evmChainID,
		row.blockNumber,
		row.blockTime,
		row.timestampMs,
		row.transactionHash,
		row.transactionType,
		row.fromAddress,
		row.toAddress,
		row.value,
		row.gas,
		row.gasUsed,
		row.revert,
		row.error,
		row.revertReason,
		row.input,
		row.output,
		row.callIndex,
	)
	if err != nil {
		return fmt.Errorf("failed to write internal transaction of block %d and txHash %s: %w", tx.BlockNumber, tx.TransactionHash, err)
	}
	return nil
}

func (r *internalTransactions) BatchInsertInternalTransactions(ctx context.Context, txs []*InternalTransactionRow) error {
	if len(txs) == 0 {
		return nil
	}

	query := fmt.Sprintf(batchInsertInternalTransactionsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, tx := range txs {
		row, err := convertIntTxnRowToIntChTransactionRow(tx)
		if err != nil {
			return fmt.Errorf("failed to convert internal transaction row of block %d and txHash %s to row: %w", tx.BlockNumber, tx.TransactionHash, err)
		}
		err = batch.Append(
			row.blockchainID,
			row.evmChainID,
			row.blockNumber,
			row.blockTime,
			row.timestampMs,
			row.transactionHash,
			row.transactionType,
			row.fromAddress,
			row.toAddress,
			row.value,
			row.gas,
			row.gasUsed,
			row.revert,
			row.error,
			row.revertReason,
			row.input,
			row.output,
			row.callIndex,
		)
		if err != nil {
			return fmt.Errorf("failed to append internal transaction of block %d and txHash %s: %w", tx.BlockNumber, tx.TransactionHash, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
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
