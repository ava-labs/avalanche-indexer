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
	ReadSnapshot(ctx context.Context) (*Snapshot, error)
}

type repository struct {
	client    clickhouse.Client
	tableName string
}

func NewRepository(client clickhouse.Client, tableName string) Repository {
	return &repository{client: client, tableName: tableName}
}

func (r *repository) WriteSnapshot(ctx context.Context, snapshot *Snapshot) error {
	query := fmt.Sprintf("INSERT INTO %s (lowest, timestamp) VALUES (?, ?)", r.tableName)
	err := r.client.Conn().
		Exec(ctx, query, snapshot.Lowest, snapshot.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}
	return nil
}

func (r *repository) ReadSnapshot(ctx context.Context) (*Snapshot, error) {
	var snapshot Snapshot
	query := fmt.Sprintf("SELECT lowest, timestamp FROM %s", r.tableName)
	err := r.client.Conn().
		QueryRow(ctx, query).
		Scan(&snapshot.Lowest, &snapshot.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}
	return &snapshot, nil
}
