package evmrepo

import (
	"context"
	"fmt"

	_ "embed"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// Logs provides methods to write logs to ClickHouse
type Logs interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteLog(ctx context.Context, log *LogRow) error
	DeleteLogs(ctx context.Context, chainID uint64) error
}

//go:embed queries/log/create-logs-table-local.sql
var createLogsTableLocalQuery string

//go:embed queries/log/create-logs-table.sql
var createLogsTableQuery string

//go:embed queries/log/write-log.sql
var writeLogQuery string

//go:embed queries/log/delete-logs.sql
var deleteLogsQuery string

type logs struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
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

// CreateTableIfNotExists creates the raw_logs table if it doesn't exist
func (r *logs) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createLogsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create logs local table: %w", err)
	}

	query = fmt.Sprintf(createLogsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create logs table: %w", err)
	}
	return nil
}

// WriteLog inserts a raw log into ClickHouse
func (r *logs) WriteLog(ctx context.Context, log *LogRow) error {
	query := fmt.Sprintf(writeLogQuery, r.database, r.tableName)

	// Convert BlockchainID
	var blockchainID interface{}
	if log.BlockchainID != nil {
		blockchainID = *log.BlockchainID
	} else {
		blockchainID = ""
	}

	// Convert EVMChainID to string for ClickHouse UInt256
	evmChainIDStr := log.EVMChainID.String()

	// Convert hex strings to bytes for FixedString fields
	blockHashBytes, err := utils.HexToBytes32(log.BlockHash)
	if err != nil {
		return fmt.Errorf("failed to convert block_hash to bytes: %w", err)
	}

	txHashBytes, err := utils.HexToBytes32(log.TxHash)
	if err != nil {
		return fmt.Errorf("failed to convert tx_hash to bytes: %w", err)
	}

	addressBytes, err := utils.HexToBytes20(log.Address)
	if err != nil {
		return fmt.Errorf("failed to convert address to bytes: %w", err)
	}

	// Convert topic hex strings to bytes for Nullable FixedString fields
	topic0, err := convertTopic0ToBytes(log.Topic0)
	if err != nil {
		return fmt.Errorf("failed to convert topic0 to bytes: %w", err)
	}
	topic1, err := convertTopicToBytes(log.Topic1)
	if err != nil {
		return fmt.Errorf("failed to convert topic1 to bytes: %w", err)
	}
	topic2, err := convertTopicToBytes(log.Topic2)
	if err != nil {
		return fmt.Errorf("failed to convert topic2 to bytes: %w", err)
	}
	topic3, err := convertTopicToBytes(log.Topic3)
	if err != nil {
		return fmt.Errorf("failed to convert topic3 to bytes: %w", err)
	}

	err = r.client.Conn().Exec(ctx, query,
		blockchainID,
		evmChainIDStr,
		log.BlockNumber,
		string(blockHashBytes[:]),
		log.BlockTime,
		string(txHashBytes[:]),
		log.TxIndex,
		string(addressBytes[:]),
		topic0,
		topic1,
		topic2,
		topic3,
		string(log.Data),
		log.LogIndex,
		log.Removed,
	)
	if err != nil {
		return fmt.Errorf("failed to write log: %w", err)
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
