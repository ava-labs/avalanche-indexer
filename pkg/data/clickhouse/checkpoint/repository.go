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
	cluster   string
	database  string
	tableName string
}

func NewRepository(
	client clickhouse.Client,
	cluster, database, tableName string,
) (Repository, error) {
	repo := &repository{client: client, cluster: cluster, database: database, tableName: tableName}
	if err := repo.CreateTableIfNotExists(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to create checkpoints table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the checkpoints table if it doesn't exist.
// Schema:
//   - chain_id: UInt64 (primary key)
//   - lowest_unprocessed_block: UInt64
//   - timestamp: Int64 (used by ReplacingMergeTree for deduplication)
func (r *repository) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s_local
		ON CLUSTER %s
		(
			chain_id UInt64,
			lowest_unprocessed_block UInt64,
			timestamp Int64
		)
		ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/%s_local', '{replica}')
		ORDER BY chain_id
		SETTINGS index_granularity = 8192
	`, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create checkpoints local table: %w", err)
	}

	query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s
		ON CLUSTER %s
		(
			chain_id UInt64,
			lowest_unprocessed_block UInt64,
			timestamp Int64
		)
		ENGINE = Distributed(%s, %s, %s_local, sipHash64(chain_id))
		`, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create checkpoints table: %w", err)
	}

	return nil
}

func (r *repository) WriteCheckpoint(ctx context.Context, checkpoint *Checkpoint) error {
	query := fmt.Sprintf(
		"INSERT INTO %s.%s (chain_id, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?)",
		r.database, r.tableName,
	)
	err := r.client.Conn().
		Exec(ctx, query, checkpoint.ChainID, checkpoint.Lowest, checkpoint.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}
	return nil
}

func (r *repository) ReadCheckpoint(ctx context.Context, chainID uint64) (*Checkpoint, error) {
	var checkpoint Checkpoint
	query := fmt.Sprintf(
		"SELECT * FROM %s.%s WHERE chain_id = ? ORDER BY timestamp DESC LIMIT 1",
		r.database, r.tableName,
	)
	err := r.client.Conn().
		QueryRow(ctx, query, chainID).
		Scan(&checkpoint.ChainID, &checkpoint.Lowest, &checkpoint.Timestamp)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &checkpoint, nil
}
