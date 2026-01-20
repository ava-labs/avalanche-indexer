package slidingwindow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStartWatchdog_WarnsOnLargeGap(t *testing.T) {
	t.Parallel()
	// State: lowest=10, highest=25 -> gap=15
	state, err := NewState(10, 10)
	require.NoError(t, err)
	_ = state.SetHighest(25)

	core, recorded := observer.New(zap.WarnLevel)
	log := zap.New(core).Sugar()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// interval small to tick quickly; maxGap small to trigger warning
	go StartGapWatchdog(ctx, log, state, 5*time.Millisecond, 5)

	// Wait for a few ticks
	time.Sleep(25 * time.Millisecond)
	cancel()

	// Assert at least one warning "gap too large"
	var warned bool
	for _, e := range recorded.All() {
		if e.Level == zap.WarnLevel && e.Message == "gap too large" {
			warned = true
			break
		}
	}
	require.True(t, warned, "expected watchdog to warn when gap is larger than maxGap")
}
