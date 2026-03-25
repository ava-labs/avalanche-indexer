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

// Transactions provides methods to write transactions to ClickHouse
type Transactions interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteTransaction(ctx context.Context, tx *TransactionRow) error
	BatchInsertTransactions(ctx context.Context, txs []*TransactionRow) error
	DeleteTransactions(ctx context.Context, chainID uint64) error
}

//go:embed queries/transaction/create-transactions-table-local.sql
var createTransactionsTableLocalQuery string

//go:embed queries/transaction/create-transactions-table.sql
var createTransactionsTableQuery string

//go:embed queries/transaction/write-transaction.sql
var writeTransactionQuery string

//go:embed queries/transaction/batch-insert-transactions.sql
var batchInsertTransactionsQuery string

//go:embed queries/transaction/delete-transactions.sql
var deleteTransactionsQuery string

type transactions struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chTransactionRow struct {
	blockchainID     interface{}
	evmChainID       *big.Int
	blockNumber      uint64
	blockHash        string
	blockTime        time.Time
	timestampMs      uint64
	hash             string
	from             string
	to               interface{}
	nonce            uint64
	value            *big.Int
	gas              uint64
	gasPrice         *big.Int
	maxFeePerGas     *big.Int
	maxPriorityFee   *big.Int
	input            string
	txType           uint8
	transactionIndex uint64
	success          uint8
	numLogs          uint32
}

func convertTransactionRowToChTransactionRow(tx *TransactionRow) (*chTransactionRow, error) {
	if tx == nil {
		return nil, errors.New("transaction is nil")
	}

	// Convert BlockchainID (string) and EVMChainID (*big.Int) for ClickHouse
	var blockchainID interface{}
	if tx.BlockchainID != nil {
		blockchainID = *tx.BlockchainID
	} else {
		blockchainID = ""
	}

	evmChainIDBigInt := big.NewInt(0)
	if tx.EVMChainID != nil {
		evmChainIDBigInt = tx.EVMChainID
	}

	// Convert hex strings to bytes for FixedString fields
	blockHashBytes, err := utils.HexToBytes32(tx.BlockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert block_hash to bytes: %w", err)
	}
	hashBytes, err := utils.HexToBytes32(tx.Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert hash to bytes: %w", err)
	}
	fromBytes, err := utils.HexToBytes20(tx.From)
	if err != nil {
		return nil, fmt.Errorf("failed to convert from_address to bytes: %w", err)
	}

	// For nullable to_address - convert empty string to nil, otherwise convert to bytes then string
	var toBytes interface{}
	if tx.To == nil || *tx.To == "" {
		toBytes = nil
	} else {
		to, err := utils.HexToBytes20(*tx.To)
		if err != nil {
			return nil, fmt.Errorf("failed to convert to_address to bytes: %w", err)
		}
		toBytes = string(to[:])
	}

	valueBigInt := big.NewInt(0)
	if tx.Value != nil {
		valueBigInt = tx.Value
	}
	gasPriceBigInt := big.NewInt(0)
	if tx.GasPrice != nil {
		gasPriceBigInt = tx.GasPrice
	}

	return &chTransactionRow{
		blockchainID:     blockchainID,
		evmChainID:       evmChainIDBigInt,
		blockNumber:      tx.BlockNumber,
		blockHash:        string(blockHashBytes[:]),
		blockTime:        tx.BlockTime,
		timestampMs:      tx.TimestampMs,
		hash:             string(hashBytes[:]),
		from:             string(fromBytes[:]),
		to:               toBytes,
		nonce:            tx.Nonce,
		value:            valueBigInt,
		gas:              tx.Gas,
		gasPrice:         gasPriceBigInt,
		maxFeePerGas:     tx.MaxFeePerGas,
		maxPriorityFee:   tx.MaxPriorityFee,
		input:            tx.Input,
		txType:           tx.Type,
		transactionIndex: tx.TransactionIndex,
		success:          tx.Success,
		numLogs:          tx.NumLogs,
	}, nil
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

// CreateTableIfNotExists creates the raw_transactions table if it doesn't exist,
// then runs all numbered migrations from queries/migrations/transaction/ to ensure
// the schema is up to date for existing tables.
func (r *transactions) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createTransactionsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create transactions local table: %w", err)
	}

	query = fmt.Sprintf(createTransactionsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create transactions table: %w", err)
	}

	if err := clickhouse.RunMigrations(ctx, r.client.Conn(), transactionsMigrationsFS, "queries/migrations/transaction", r.database, r.tableName, r.cluster); err != nil {
		return fmt.Errorf("failed to run transactions migrations: %w", err)
	}

	return nil
}

// WriteTransaction inserts a raw transaction into ClickHouse
func (r *transactions) WriteTransaction(ctx context.Context, tx *TransactionRow) error {
	query := fmt.Sprintf(writeTransactionQuery, r.database, r.tableName)
	row, err := convertTransactionRowToChTransactionRow(tx)
	if err != nil {
		return fmt.Errorf("failed to convert transaction row of block %d and txHash %s to row: %w", tx.BlockNumber, tx.Hash, err)
	}

	evmChainIDStr := "0"
	if tx.EVMChainID != nil {
		evmChainIDStr = tx.EVMChainID.String()
	}

	valueStr := "0"
	if tx.Value != nil {
		valueStr = tx.Value.String()
	}

	gasPriceStr := "0"
	if tx.GasPrice != nil {
		gasPriceStr = tx.GasPrice.String()
	}

	err = r.client.Conn().Exec(ctx, query,
		row.blockchainID,
		evmChainIDStr,
		row.blockNumber,
		row.blockHash,
		row.blockTime,
		row.timestampMs,
		row.hash,
		row.from,
		row.to,
		row.nonce,
		valueStr,
		row.gas,
		gasPriceStr,
		row.maxFeePerGas,
		row.maxPriorityFee,
		row.input,
		row.txType,
		row.transactionIndex,
		row.success,
		row.numLogs,
	)
	if err != nil {
		return fmt.Errorf("failed to write transaction of block %d and txHash %s: %w", tx.BlockNumber, tx.Hash, err)
	}
	return nil
}

func (r *transactions) BatchInsertTransactions(ctx context.Context, txs []*TransactionRow) error {
	if len(txs) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertTransactionsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, tx := range txs {
		row, err := convertTransactionRowToChTransactionRow(tx)
		if err != nil {
			return fmt.Errorf("failed to convert transaction row of block %s and txHash %s to row: %w", tx.BlockHash, tx.Hash, err)
		}
		err = batch.Append(
			row.blockchainID,
			row.evmChainID,
			row.blockNumber,
			row.blockHash,
			row.blockTime,
			row.timestampMs,
			row.hash,
			row.from,
			row.to,
			row.nonce,
			row.value,
			row.gas,
			row.gasPrice,
			row.maxFeePerGas,
			row.maxPriorityFee,
			row.input,
			row.txType,
			row.transactionIndex,
			row.success,
			row.numLogs,
		)
		if err != nil {
			return fmt.Errorf("failed to append transaction of block %d and txHash %s: %w", tx.BlockNumber, tx.Hash, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
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
