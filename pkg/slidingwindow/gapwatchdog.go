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
			gap := highest - lowest
			if gap > maxGap {
				log.Warnw("gap too large", "gap", gap, "highest", highest, "lowest", lowest)
			}
		}
	}
}
