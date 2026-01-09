package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/snapshot"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
)

const (
	writeTimeout = 1 * time.Second
	maxRetries   = 3
	backoff      = 300 * time.Millisecond
)

// Start starts a scheduler that writes the Snapshot to the repository (persistent storage)
// every interval.
func Start(
	ctx context.Context,
	s *slidingwindow.State,
	repo snapshot.Repository,
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
				err = repo.WriteSnapshot(ctxW, &snapshot.Snapshot{
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
				return fmt.Errorf("failed to write snapshot (lowest: %d): %w", lowest, err)
			}
		}
	}
}
