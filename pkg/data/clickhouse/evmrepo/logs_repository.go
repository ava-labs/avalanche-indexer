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

// chLogRow holds ClickHouse-ready values; `ch` tags match batch INSERT columns for AppendStruct.
type chLogRow struct {
	BlockchainID interface{} `ch:"blockchain_id"`
	EVMChainID   *big.Int    `ch:"evm_chain_id"`
	BlockNumber  uint64      `ch:"block_number"`
	BlockHash    string      `ch:"block_hash"`
	BlockTime    time.Time   `ch:"block_time"`
	TimestampMs  uint64      `ch:"timestamp_ms"`
	TxHash       string      `ch:"tx_hash"`
	TxIndex      uint32      `ch:"tx_index"`
	Address      string      `ch:"address"`
	Topic0       *string     `ch:"topic0"`
	Topic1       *string     `ch:"topic1"`
	Topic2       *string     `ch:"topic2"`
	Topic3       *string     `ch:"topic3"`
	Data         string      `ch:"data"`
	LogIndex     uint32      `ch:"log_index"`
	Removed      bool        `ch:"removed"`
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
		BlockchainID: blockchainID,
		EVMChainID:   evmChainIDBigInt,
		BlockNumber:  log.BlockNumber,
		BlockHash:    string(blockHashBytes[:]),
		BlockTime:    log.BlockTime,
		TimestampMs:  log.TimestampMs,
		TxHash:       string(txHashBytes[:]),
		TxIndex:      log.TxIndex,
		Address:      string(addressBytes[:]),
		Topic0:       topic0,
		Topic1:       topic1,
		Topic2:       topic2,
		Topic3:       topic3,
		Data:         string(log.Data),
		LogIndex:     log.LogIndex,
		Removed:      log.Removed,
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
	if log == nil {
		return nil
	}

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
		row.BlockchainID,
		evmChainIDStr,
		row.BlockNumber,
		row.BlockHash,
		row.BlockTime,
		row.TimestampMs,
		row.TxHash,
		row.TxIndex,
		row.Address,
		row.Topic0,
		row.Topic1,
		row.Topic2,
		row.Topic3,
		row.Data,
		row.LogIndex,
		row.Removed,
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
		if log == nil {
			continue
		}

		row, err := convertLogRowToChLogRow(log)
		if err != nil {
			return fmt.Errorf("failed to convert log row of tx %s on index %d to ch row: %w", log.TxHash, log.LogIndex, err)
		}
		if err := batch.AppendStruct(row); err != nil {
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
