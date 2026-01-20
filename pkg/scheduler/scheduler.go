package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/checkpoint"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
)

const (
	writeTimeout = 1 * time.Second
	maxRetries   = 3
	backoff      = 300 * time.Millisecond
)

// Start starts a scheduler that writes the Checkpoint to the repository (persistent storage)
// every interval.
func Start(
	ctx context.Context,
	s *slidingwindow.State,
	repo checkpoint.Repository,
	interval time.Duration,
	chainID uint64,
) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// read state atomically and persist
			lowest := s.GetLowest()
			var err error
			for attempt := 0; attempt <= maxRetries; attempt++ {
				ctxW, cancel := context.WithTimeout(ctx, writeTimeout)
				err = repo.WriteCheckpoint(ctxW, &checkpoint.Checkpoint{
					ChainID:   chainID,
					Lowest:    lowest,
					Timestamp: time.Now().Unix(),
				})
				cancel()
				if err == nil {
					break
				}
				if attempt < maxRetries {
					time.Sleep(backoff)
				}
			}
			if err != nil {
				return fmt.Errorf("failed to write checkpoint (lowest: %d): %w", lowest, err)
			}
		}
	}
}
