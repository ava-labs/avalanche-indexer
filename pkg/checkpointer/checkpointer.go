package checkpointer

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
)

// Checkpointer abstracts checkpoint persistence across different data stores. Checkpoints track
// the lowest unprocessed block height for a given chain, enabling recovery and resumption of
// indexing after restarts or failures.
type Checkpointer interface {
	// Initialize ensures the underlying storage is ready (creates tables, schemas, etc.). This
	// should be idempotent and safe to call multiple times.
	Initialize(ctx context.Context) error

	// Write atomically persists a checkpoint. The EVM chain ID should be included in the key or
	// row in the data store. The timestamp used should be the current Unix timestamp in seconds.
	Write(ctx context.Context, evmChainID uint64, lowestUnprocessed uint64) error

	// Read retrieves the latest checkpoint for a chain. Returns the lowestUnprocessed block height
	// and whether a checkpoint exists. If no checkpoint exists, exists will be false and
	// lowestUnprocessed will be 0.
	Read(ctx context.Context, evmChainID uint64) (lowestUnprocessed uint64, exists bool, err error)
}

// Start periodically persists the sliding window state to durable storage.
//
// Returns nil on context cancellation (graceful shutdown), or an error if checkpoint writes
// fail after all retries.
func Start(
	ctx context.Context,
	s *slidingwindow.State,
	checkpointer Checkpointer,
	cfg Config,
	evmChainID uint64,
) error {
	t := time.NewTicker(cfg.Interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown - exit without error
			return nil

		case <-t.C:
			lowest := s.GetLowest()

			var lastErr error
			for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
				// Check if context was cancelled before attempting write
				if ctx.Err() != nil {
					return nil
				}

				writeCtx, cancel := context.WithTimeout(ctx, cfg.WriteTimeout)
				lastErr = checkpointer.Write(writeCtx, evmChainID, lowest)
				cancel()

				// Write succeeded
				if lastErr == nil {
					break
				}

				// Check if error was due to context cancellation
				if ctx.Err() != nil {
					return nil
				}

				// Don't sleep after the last attempt
				if attempt < cfg.MaxRetries {
					// Sleep with context awareness
					select {
					case <-time.After(cfg.RetryBackoff):
						// Continue to next retry
					case <-ctx.Done():
						return nil
					}
				}
			}

			// If we exhausted retries and still have an error, return it
			if lastErr != nil {
				return fmt.Errorf("failed to write checkpoint (lowest: %d) after %d retries: %w",
					lowest, cfg.MaxRetries+1, lastErr)
			}
		}
	}
}
