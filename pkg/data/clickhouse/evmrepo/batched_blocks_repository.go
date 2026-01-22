package evmrepo

import (
	"context"
)

// BatchedBlocks wraps Blocks interface to use combined batch inserter
type BatchedBlocks struct {
	inserter *BatchInserter
}

// NewBatchedBlocks creates a new batched blocks repository
func NewBatchedBlocks(inserter *BatchInserter) *BatchedBlocks {
	return &BatchedBlocks{
		inserter: inserter,
	}
}

// CreateTableIfNotExists is a no-op for batched repository (table should already exist)
func (r *BatchedBlocks) CreateTableIfNotExists(ctx context.Context) error {
	// Table creation should be handled separately
	return nil
}

// WriteBlock adds a block to the batch
func (r *BatchedBlocks) WriteBlock(ctx context.Context, block *BlockRow) error {
	return r.inserter.AddBlock(ctx, block)
}
