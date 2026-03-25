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

// Logs provides methods to write logs to ClickHouse
type Logs interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteLog(ctx context.Context, log *LogRow) error
	BatchInsertLogs(ctx context.Context, logs []*LogRow) error
	DeleteLogs(ctx context.Context, chainID uint64) error
}

//go:embed queries/log/create-logs-table-local.sql
var createLogsTableLocalQuery string

//go:embed queries/log/create-logs-table.sql
var createLogsTableQuery string

//go:embed queries/log/write-log.sql
var writeLogQuery string

//go:embed queries/log/batch-insert-logs.sql
var batchInsertLogsQuery string

//go:embed queries/log/delete-logs.sql
var deleteLogsQuery string

type logs struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chLogRow struct {
	blockchainID interface{}
	evmChainID   *big.Int
	blockNumber  uint64
	blockHash    string
	blockTime    time.Time
	timestampMs  uint64
	txHash       string
	txIndex      uint32
	address      string
	topic0       *string
	topic1       *string
	topic2       *string
	topic3       *string
	data         string
	logIndex     uint32
	removed      bool
}

func convertLogRowToChLogRow(log *LogRow) (*chLogRow, error) {
	if log == nil {
		return nil, errors.New("log is nil")
	}

	// Convert BlockchainID
	var blockchainID interface{}
	if log.BlockchainID != nil {
		blockchainID = *log.BlockchainID
	} else {
		blockchainID = ""
	}

	// Convert EVMChainID to string for ClickHouse UInt256
	evmChainIDBigInt := big.NewInt(0)
	if log.EVMChainID != nil {
		evmChainIDBigInt = log.EVMChainID
	}

	// Convert hex strings to bytes for FixedString fields
	blockHashBytes, err := utils.HexToBytes32(log.BlockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert block_hash to bytes: %w", err)
	}

	txHashBytes, err := utils.HexToBytes32(log.TxHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert tx_hash to bytes: %w", err)
	}

	addressBytes, err := utils.HexToBytes20(log.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to convert address to bytes: %w", err)
	}

	// Convert topic hex strings to bytes for Nullable FixedString fields
	topic0, err := convertTopic0ToBytes(log.Topic0)
	if err != nil {
		return nil, fmt.Errorf("failed to convert topic0 to bytes: %w", err)
	}
	topic1, err := convertTopicToBytes(log.Topic1)
	if err != nil {
		return nil, fmt.Errorf("failed to convert topic1 to bytes: %w", err)
	}
	topic2, err := convertTopicToBytes(log.Topic2)
	if err != nil {
		return nil, fmt.Errorf("failed to convert topic2 to bytes: %w", err)
	}
	topic3, err := convertTopicToBytes(log.Topic3)
	if err != nil {
		return nil, fmt.Errorf("failed to convert topic3 to bytes: %w", err)
	}

	return &chLogRow{
		blockchainID: blockchainID,
		evmChainID:   evmChainIDBigInt,
		blockNumber:  log.BlockNumber,
		blockHash:    string(blockHashBytes[:]),
		blockTime:    log.BlockTime,
		timestampMs:  log.TimestampMs,
		txHash:       string(txHashBytes[:]),
		txIndex:      log.TxIndex,
		address:      string(addressBytes[:]),
		topic0:       topic0,
		topic1:       topic1,
		topic2:       topic2,
		topic3:       topic3,
		data:         string(log.Data),
		logIndex:     log.LogIndex,
		removed:      log.Removed,
	}, nil
}

// NewLogs creates a new raw logs repository and initializes the table
func NewLogs(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (Logs, error) {
	repo := &logs{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize logs table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the raw_logs table if it doesn't exist,
// then runs all numbered migrations from queries/migrations/log/ to ensure
// the schema is up to date for existing tables.
func (r *logs) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createLogsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create logs local table: %w", err)
	}

	query = fmt.Sprintf(createLogsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create logs table: %w", err)
	}

	if err := clickhouse.RunMigrations(ctx, r.client.Conn(), logsMigrationsFS, "queries/migrations/log", r.database, r.tableName, r.cluster); err != nil {
		return fmt.Errorf("failed to run logs migrations: %w", err)
	}

	return nil
}

// WriteLog inserts a raw log into ClickHouse
func (r *logs) WriteLog(ctx context.Context, log *LogRow) error {
	query := fmt.Sprintf(writeLogQuery, r.database, r.tableName)

	row, err := convertLogRowToChLogRow(log)
	if err != nil {
		return fmt.Errorf("failed to convert log row of tx %s on index %d to ch row: %w", log.TxHash, log.LogIndex, err)
	}

	evmChainIDStr := "0"
	if log.EVMChainID != nil {
		evmChainIDStr = log.EVMChainID.String()
	}

	err = r.client.Conn().Exec(ctx, query,
		row.blockchainID,
		evmChainIDStr,
		row.blockNumber,
		row.blockHash,
		row.blockTime,
		row.timestampMs,
		row.txHash,
		row.txIndex,
		row.address,
		row.topic0,
		row.topic1,
		row.topic2,
		row.topic3,
		row.data,
		row.logIndex,
		row.removed,
	)
	if err != nil {
		return fmt.Errorf("failed to write log of tx %s on index %d: %w", log.TxHash, log.LogIndex, err)
	}
	return nil
}

func (r *logs) BatchInsertLogs(ctx context.Context, logs []*LogRow) error {
	if len(logs) == 0 {
		return nil
	}

	query := fmt.Sprintf(batchInsertLogsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, log := range logs {
		row, err := convertLogRowToChLogRow(log)
		if err != nil {
			return fmt.Errorf("failed to convert log row of tx %s on index %d to ch row: %w", log.TxHash, log.LogIndex, err)
		}

		err = batch.Append(
			row.blockchainID,
			row.evmChainID,
			row.blockNumber,
			row.blockHash,
			row.blockTime,
			row.timestampMs,
			row.txHash,
			row.txIndex,
			row.address,
			row.topic0,
			row.topic1,
			row.topic2,
			row.topic3,
			row.data,
			row.logIndex,
			row.removed,
		)
		if err != nil {
			return fmt.Errorf("failed to append log of tx %s on index %d: %w", log.TxHash, log.LogIndex, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	return nil
}

// convertTopicToBytes converts a nullable topic hex string to bytes for ClickHouse
func convertTopicToBytes(topic *string) (*string, error) {
	if topic == nil {
		return nil, nil
	}
	topicBytes, err := utils.HexToBytes32(*topic)
	if err != nil {
		return nil, err
	}
	result := string(topicBytes[:])
	return &result, nil
}

// convertTopic0ToBytes converts a topic0 string to bytes for ClickHouse (empty string = NULL)
func convertTopic0ToBytes(topic string) (*string, error) {
	if topic == "" {
		return nil, nil
	}
	topicBytes, err := utils.HexToBytes32(topic)
	if err != nil {
		return nil, err
	}
	result := string(topicBytes[:])
	return &result, nil
}

func (r *logs) DeleteLogs(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteLogsQuery, r.database, r.tableName, r.cluster)
	if err := r.client.Conn().Exec(ctx, query, chainID); err != nil {
		return fmt.Errorf("failed to delete logs: %w", err)
	}
	return nil
}
