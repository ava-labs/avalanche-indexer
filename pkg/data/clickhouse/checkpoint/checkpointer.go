package checkpoint

import (
	"context"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/checkpointer"
)

// Checkpointer is a ClickHouse-backed implementation of the checkpointer.Checkpointer interface.
// It wraps a Repository to provide checkpoint persistence using ClickHouse as the storage backend.
type Checkpointer struct {
	repo Repository
}

// Creates a new Checkpointer. The provided repository will be used for all checkpoint operations.
func NewCheckpointer(repo Repository) *Checkpointer {
	return &Checkpointer{repo: repo}
}

// Initialize ensures the checkpoints table exists in ClickHouse.
func (c *Checkpointer) Initialize(ctx context.Context) error {
	return c.repo.CreateTableIfNotExists(ctx)
}

// Write persists a checkpoint to ClickHouse with the current Unix timestamp in seconds.
func (c *Checkpointer) Write(
	ctx context.Context,
	evmChainID uint64,
	lowestUnprocessed uint64,
) error {
	return c.repo.WriteCheckpoint(ctx, &Checkpoint{
		ChainID:   evmChainID,
		Lowest:    lowestUnprocessed,
		Timestamp: time.Now().Unix(),
	})
}

// Read retrieves the latest checkpoint for given EVM chain ID.
func (c *Checkpointer) Read(
	ctx context.Context,
	evmChainID uint64,
) (lowestUnprocessed uint64, exists bool, err error) {
	checkpoint, err := c.repo.ReadCheckpoint(ctx, evmChainID)
	if err != nil {
		return 0, false, err
	}
	if checkpoint == nil {
		return 0, false, nil
	}
	return checkpoint.Lowest, true, nil
}

var _ checkpointer.Checkpointer = (*Checkpointer)(nil)
