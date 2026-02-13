package checkpoint

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/checkpointer"
	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Repository is used to write and read the checkpoint of the sliding window state
// to persistent storage (ClickHouse). It implements the checkpointer.Checkpointer interface
// and adds ClickHouse-specific operations.
type Repository interface {
	checkpointer.Checkpointer
	DeleteCheckpoints(ctx context.Context, chainID uint64) error
}

var _ Repository = (*repository)(nil)
var _ checkpointer.Checkpointer = (*repository)(nil)

//go:embed queries/create-table-local.sql
var createTableLocalQuery string

//go:embed queries/create-table.sql
var createTableQuery string

//go:embed queries/write-checkpoint.sql
var writeCheckpointQuery string

//go:embed queries/read-checkpoint.sql
var readCheckpointQuery string

//go:embed queries/delete-checkpoints.sql
var deleteCheckpointsQuery string

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
	if err := repo.Initialize(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to create checkpoints table: %w", err)
	}
	return repo, nil
}

// Initialize ensures the checkpoints table exists in ClickHouse.
// Implements checkpointer.Checkpointer interface.
// Schema:
//   - chain_id: UInt64 (primary key)
//   - lowest_unprocessed_block: UInt64
//   - timestamp: Int64 (used by ReplacingMergeTree for deduplication)
func (r *repository) Initialize(ctx context.Context) error {
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

// Write persists a checkpoint to ClickHouse with the current Unix timestamp in seconds.
// Implements checkpointer.Checkpointer interface.
func (r *repository) Write(
	ctx context.Context,
	evmChainID uint64,
	lowestUnprocessed uint64,
) error {
	checkpoint := &Checkpoint{
		ChainID:   evmChainID,
		Lowest:    lowestUnprocessed,
		Timestamp: time.Now().Unix(),
	}
	query := fmt.Sprintf(writeCheckpointQuery, r.database, r.tableName)
	err := r.client.Conn().
		Exec(ctx, query, checkpoint.ChainID, checkpoint.Lowest, checkpoint.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}
	return nil
}

// Read retrieves the latest checkpoint for given EVM chain ID.
// Implements checkpointer.Checkpointer interface.
func (r *repository) Read(
	ctx context.Context,
	evmChainID uint64,
) (lowestUnprocessed uint64, exists bool, err error) {
	var checkpoint Checkpoint
	query := fmt.Sprintf(readCheckpointQuery, r.database, r.tableName)
	err = r.client.Conn().
		QueryRow(ctx, query, evmChainID).
		Scan(&checkpoint.ChainID, &checkpoint.Lowest, &checkpoint.Timestamp)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return checkpoint.Lowest, true, nil
}

func (r *repository) DeleteCheckpoints(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteCheckpointsQuery, r.database, r.tableName, r.cluster)
	if err := r.client.Conn().Exec(ctx, query, chainID); err != nil {
		return fmt.Errorf("failed to delete checkpoints: %w", err)
	}

	return nil
}
