package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "embed"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Checkpoint Repository is used to write and read the checkpoint of the sliding window state
// to the persistent storage (ClickHouse).
type Repository interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteCheckpoint(ctx context.Context, checkpoint *Checkpoint) error
	ReadCheckpoint(ctx context.Context, chainID uint64) (*Checkpoint, error)
}

//go:embed queries/create-table-local.sql
var createTableLocalQuery string

//go:embed queries/create-table.sql
var createTableQuery string

//go:embed queries/write-checkpoint.sql
var writeCheckpointQuery string

//go:embed queries/read-checkpoint.sql
var readCheckpointQuery string

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
	query := fmt.Sprintf(createTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create checkpoints local table: %w", err)
	}

	query = fmt.Sprintf(createTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create checkpoints table: %w", err)
	}

	return nil
}

func (r *repository) WriteCheckpoint(ctx context.Context, checkpoint *Checkpoint) error {
	query := fmt.Sprintf(writeCheckpointQuery, r.database, r.tableName)
	err := r.client.Conn().
		Exec(ctx, query, checkpoint.ChainID, checkpoint.Lowest, checkpoint.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}
	return nil
}

func (r *repository) ReadCheckpoint(ctx context.Context, chainID uint64) (*Checkpoint, error) {
	var checkpoint Checkpoint
	query := fmt.Sprintf(readCheckpointQuery, r.database, r.tableName)
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
