package slidingwindow

import (
	"context"
	"time"

	"go.uber.org/zap"
)

func StartGapWatchdog(ctx context.Context, log *zap.SugaredLogger, s *State, interval time.Duration, maxGap uint64) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			lowest := s.GetLowest()
			highest := s.GetHighest()
			// When backfill is complete, lowest may be > highest (lowest points to next unprocessed block).
			// In this case, gap is 0 (no unprocessed blocks).
			var gap uint64
			if highest >= lowest {
				gap = highest - lowest
			} else {
				gap = 0
			}
			if gap > maxGap {
				log.Warnw("gap too large", "gap", gap, "highest", highest, "lowest", lowest)
			}
		}
	}
}
