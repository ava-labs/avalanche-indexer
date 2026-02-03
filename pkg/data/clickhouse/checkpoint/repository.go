package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Checkpoint Repository is used to write and read the checkpoint of the sliding window state
// to the persistent storage (ClickHouse).
type Repository interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteCheckpoint(ctx context.Context, checkpoint *Checkpoint) error
	ReadCheckpoint(ctx context.Context, chainID uint64) (*Checkpoint, error)
}

type repository struct {
	client    clickhouse.Client
	tableName string
}

func NewRepository(client clickhouse.Client, tableName string) Repository {
	return &repository{client: client, tableName: tableName}
}

// CreateTableIfNotExists creates the checkpoints table if it doesn't exist.
// Schema:
//   - chain_id: UInt64 (primary key)
//   - lowest_unprocessed_block: UInt64
//   - timestamp: Int64 (used by ReplacingMergeTree for deduplication)
func (r *repository) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		(
			chain_id UInt64,
			lowest_unprocessed_block UInt64,
			timestamp Int64
		)
		ENGINE = ReplacingMergeTree(timestamp)
		ORDER BY chain_id
	`, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create checkpoints table: %w", err)
	}
	return nil
}

func (r *repository) WriteCheckpoint(ctx context.Context, checkpoint *Checkpoint) error {
	query := fmt.Sprintf("INSERT INTO %s (chain_id, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?)", r.tableName)
	err := r.client.Conn().
		Exec(ctx, query, checkpoint.ChainID, checkpoint.Lowest, checkpoint.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}
	return nil
}

func (r *repository) ReadCheckpoint(ctx context.Context, chainID uint64) (*Checkpoint, error) {
	var checkpoint Checkpoint
	query := fmt.Sprintf("SELECT * FROM %s WHERE chain_id = %d", r.tableName, chainID)
	err := r.client.Conn().
		QueryRow(ctx, query).
		Scan(&checkpoint.ChainID, &checkpoint.Lowest, &checkpoint.Timestamp)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &checkpoint, nil
}
