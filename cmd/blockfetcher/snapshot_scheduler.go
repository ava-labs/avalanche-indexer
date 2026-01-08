package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/snapshot"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
)

// startSnapshotScheduler starts a scheduler that writes the snapshot to the repository (persistent storage)
// every interval.
func startSnapshotScheduler(
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
			ctxW, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := repo.WriteSnapshot(ctxW, &snapshot.Snapshot{
				ChainID:   chainID,
				Lowest:    lowest,
				Timestamp: time.Now().Unix(),
			})
			cancel()
			if err != nil {
				return fmt.Errorf("failed to write snapshot (lowest: %d): %w", lowest, err)
			}
		}
	}
}
