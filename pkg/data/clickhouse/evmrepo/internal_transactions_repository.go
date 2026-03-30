package evmrepo

import (
	"context"
	"errors"
	"fmt"
	"math/big"
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

// chInternalTransactionRow holds ClickHouse-ready values; `ch` tags match batch INSERT columns for AppendStruct.
type chInternalTransactionRow struct {
	BlockchainID    interface{} `ch:"blockchain_id"`
	EVMChainID      *big.Int    `ch:"evm_chain_id"`
	BlockNumber     uint64      `ch:"block_number"`
	BlockTime       time.Time   `ch:"block_time"`
	TimestampMs     uint64      `ch:"timestamp_ms"`
	TransactionHash string      `ch:"transaction_hash"`
	TransactionType string      `ch:"type"`
	FromAddress     string      `ch:"from_address"`
	ToAddress       string      `ch:"to_address"`
	Value           string      `ch:"value"`
	Gas             string      `ch:"gas"`
	GasUsed         string      `ch:"gas_used"`
	Revert          bool        `ch:"revert"`
	ErrorText       string      `ch:"error"`
	RevertReason    string      `ch:"revert_reason"`
	Input           string      `ch:"input"`
	Output          string      `ch:"output"`
	CallIndex       string      `ch:"call_index"`
}

func convertInternalTxnRowToChInternalTxnRow(tx *InternalTransactionRow) (*chInternalTransactionRow, error) {
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
	evmChainIDBigInt := big.NewInt(0)
	if tx.EVMChainID != nil {
		evmChainIDBigInt = tx.EVMChainID
	}

	// Convert transaction hash hex string to bytes
	txHashBytes, err := utils.HexToBytes32(tx.TransactionHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert transaction_hash to bytes: %w", err)
	}

	return &chInternalTransactionRow{
		BlockchainID:    blockchainID,
		EVMChainID:      evmChainIDBigInt,
		BlockNumber:     tx.BlockNumber,
		BlockTime:       tx.BlockTime,
		TimestampMs:     tx.TimestampMs,
		TransactionHash: string(txHashBytes[:]),
		TransactionType: tx.Type,
		FromAddress:     string(tx.From[:]),
		ToAddress:       string(tx.To[:]),
		Value:           tx.Value,
		Gas:             tx.Gas,
		GasUsed:         tx.GasUsed,
		Revert:          tx.Revert,
		ErrorText:       tx.Error,
		RevertReason:    tx.RevertReason,
		Input:           tx.Input,
		Output:          tx.Output,
		CallIndex:       tx.CallIndex,
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
	if tx == nil {
		return nil
	}

	query := fmt.Sprintf(writeInternalTransactionQuery, r.database, r.tableName)

	row, err := convertInternalTxnRowToChInternalTxnRow(tx)
	if err != nil {
		return fmt.Errorf("failed to convert internal transaction row of block %d and txHash %s to row: %w", tx.BlockNumber, tx.TransactionHash, err)
	}

	evmChainIDStr := "0"
	if row.EVMChainID != nil {
		evmChainIDStr = row.EVMChainID.String()
	}

	err = r.client.Conn().Exec(ctx, query,
		row.BlockchainID,
		evmChainIDStr,
		row.BlockNumber,
		row.BlockTime,
		row.TimestampMs,
		row.TransactionHash,
		row.TransactionType,
		row.FromAddress,
		row.ToAddress,
		row.Value,
		row.Gas,
		row.GasUsed,
		row.Revert,
		row.ErrorText,
		row.RevertReason,
		row.Input,
		row.Output,
		row.CallIndex,
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
		if tx == nil {
			continue
		}

		row, err := convertInternalTxnRowToChInternalTxnRow(tx)
		if err != nil {
			return fmt.Errorf("failed to convert internal transaction row of block %d and txHash %s to row: %w", tx.BlockNumber, tx.TransactionHash, err)
		}
		if err := batch.AppendStruct(row); err != nil {
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
