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
			// When backfill is complete, lowest may be exactly highest+1 (lowest points to next unprocessed block).
			// In this case, gap is 0 (no unprocessed blocks). Any larger difference indicates inconsistent state.
			var gap uint64
			switch {
			case highest >= lowest:
				gap = highest - lowest
			case lowest == highest+1:
				// Expected backfill-complete case: no unprocessed blocks.
				gap = 0
			default:
				// Unexpected state: lowest is more than one ahead of highest.
				gap = 0
				log.Warnw("state inconsistency in gap watchdog: lowest much greater than highest", "highest", highest, "lowest", lowest)
			}
			if gap > maxGap {
				log.Warnw("gap too large", "gap", gap, "highest", highest, "lowest", lowest)
			}
		}
	}
}
