package evmrepo

import (
	"context"
)

// BatchedTransactions wraps Transactions interface to use combined batch inserter
type BatchedTransactions struct {
	inserter *BatchInserter
}

// NewBatchedTransactions creates a new batched transactions repository
func NewBatchedTransactions(inserter *BatchInserter) *BatchedTransactions {
	return &BatchedTransactions{
		inserter: inserter,
	}
}

// CreateTableIfNotExists is a no-op for batched repository (table should already exist)
func (r *BatchedTransactions) CreateTableIfNotExists(ctx context.Context) error {
	// Table creation should be handled separately
	return nil
}

// WriteTransaction adds a transaction to the batch
func (r *BatchedTransactions) WriteTransaction(ctx context.Context, tx *TransactionRow) error {
	return r.inserter.AddTransaction(ctx, tx)
}
