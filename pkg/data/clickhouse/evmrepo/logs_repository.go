package evmrepo

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// Logs provides methods to write logs to ClickHouse
type Logs interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteLog(ctx context.Context, log *LogRow) error
}

type logs struct {
	client    clickhouse.Client
	tableName string
}

// NewLogs creates a new raw logs repository and initializes the table
func NewLogs(ctx context.Context, client clickhouse.Client, tableName string) (Logs, error) {
	repo := &logs{
		client:    client,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize logs table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the raw_logs table if it doesn't exist
func (r *logs) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			blockchain_id String,
			evm_chain_id UInt256,
			block_number UInt64,
			block_hash FixedString(32),
			block_time DateTime64(3, 'UTC'),
			tx_hash FixedString(32),
			tx_index UInt32,
			address FixedString(20),
			topics Array(FixedString(32)),
			data String,
			log_index UInt32,
			removed UInt8
		)
		ENGINE = MergeTree
		ORDER BY (blockchain_id, block_time, tx_hash, log_index)
		SETTINGS index_granularity = 8192
	`, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create logs table: %w", err)
	}
	return nil
}

// WriteLog inserts a raw log into ClickHouse
func (r *logs) WriteLog(ctx context.Context, log *LogRow) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			blockchain_id, evm_chain_id, block_number, block_hash, block_time,
			tx_hash, tx_index, address, topics, data, log_index, removed
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.tableName)

	// Convert BlockchainID
	var blockchainID interface{}
	if log.BlockchainID != nil {
		blockchainID = *log.BlockchainID
	} else {
		blockchainID = ""
	}

	// Convert EVMChainID to string for ClickHouse UInt256
	evmChainIDStr := "0"
	if log.EVMChainID != nil {
		evmChainIDStr = log.EVMChainID.String()
	}

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

	// Convert topics array to []string of raw bytes
	topicsBytes := make([]string, len(log.Topics))
	for i, topic := range log.Topics {
		topicBytes, err := utils.HexToBytes32(topic)
		if err != nil {
			return fmt.Errorf("failed to convert topic %d to bytes: %w", i, err)
		}
		topicsBytes[i] = string(topicBytes[:])
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
		topicsBytes,
		string(log.Data),
		log.LogIndex,
		log.Removed,
	)
	if err != nil {
		return fmt.Errorf("failed to write log: %w", err)
	}
	return nil
}
