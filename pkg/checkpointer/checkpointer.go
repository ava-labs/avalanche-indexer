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

	// Write atomically persists a checkpoint for the specified EVM chain.
	Write(ctx context.Context, evmChainID uint64, lowestUnprocessed uint64) error

	// Read retrieves the latest checkpoint for a chain. Returns the lowestUnprocessed block height
	// and whether a checkpoint exists. If no checkpoint exists, exists will be false and
	// lowestUnprocessed will be 0.
	Read(ctx context.Context, evmChainID uint64) (lowestUnprocessed uint64, exists bool, err error)
}

// Checkpointer config
type Config struct {
	Interval     time.Duration
	WriteTimeout time.Duration
	MaxRetries   int
	RetryBackoff time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Interval:     30 * time.Second,
		WriteTimeout: 1 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 300 * time.Millisecond,
	}
}

// Start periodically persists the sliding window state to durable storage. Attempts a final
// checkpoint write on graceful shutdown (context cancellation).
//
// Parameters:
//   - ctx: Context
//   - s: Sliding window state to read current progress from
//   - checkpointer: Storage backend implementation for checkpoint persistence
//   - cfg: Configuration
//   - evmChainID: EVM chain ID
//
// Returns an error if checkpoint writes fail after all retries, or nil on graceful shutdown.
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
			// Attempt a final checkpoint write on graceful shutdown
			// Use a fresh context with timeout to avoid immediate cancellation
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			lowest := s.GetLowest()
			_ = writeCheckpointWithRetry(
				shutdownCtx,
				checkpointer,
				evmChainID,
				lowest,
				cfg,
			) // Best effort, ignore error on shutdown
			cancel()
			return nil

		case <-t.C:
			// Read state atomically and persist
			lowest := s.GetLowest()
			if err := writeCheckpointWithRetry(ctx, checkpointer, evmChainID, lowest, cfg); err != nil {
				return err
			}
		}
	}
}

func writeCheckpointWithRetry(
	ctx context.Context,
	checkpointer Checkpointer,
	evmChainID, lowest uint64,
	cfg Config,
) error {
	var err error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		ctxW, cancel := context.WithTimeout(ctx, cfg.WriteTimeout)
		err = checkpointer.Write(ctxW, evmChainID, lowest)
		cancel()

		if err == nil {
			return nil
		}

		// Don't sleep after the last attempt
		if attempt < cfg.MaxRetries {
			time.Sleep(cfg.RetryBackoff)
		}
	}
	return fmt.Errorf("failed to write checkpoint (lowest: %d): %w", lowest, err)
}
