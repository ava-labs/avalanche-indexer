package snapshot

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Snapshot Repository is used to write and read the snapshot of the sliding window state
// to the persistent storage (ClickHouse).
type Repository interface {
	WriteSnapshot(ctx context.Context, snapshot *Snapshot) error
	ReadSnapshot(ctx context.Context, chainID uint64) (*Snapshot, error)
}

type repository struct {
	client    clickhouse.Client
	tableName string
}

func NewRepository(client clickhouse.Client, tableName string) Repository {
	return &repository{client: client, tableName: tableName}
}

func (r *repository) WriteSnapshot(ctx context.Context, snapshot *Snapshot) error {
	query := fmt.Sprintf("INSERT INTO %s (chain_id, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?)", r.tableName)
	err := r.client.Conn().
		Exec(ctx, query, snapshot.ChainID, snapshot.Lowest, snapshot.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}
	return nil
}

func (r *repository) ReadSnapshot(ctx context.Context, chainID uint64) (*Snapshot, error) {
	var snapshot Snapshot
	query := fmt.Sprintf("SELECT * FROM %s FINAL WHERE chain_id = %d", r.tableName, chainID)
	err := r.client.Conn().
		QueryRow(ctx, query).
		Scan(&snapshot.ChainID, &snapshot.Lowest, &snapshot.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}
	return &snapshot, nil
}
