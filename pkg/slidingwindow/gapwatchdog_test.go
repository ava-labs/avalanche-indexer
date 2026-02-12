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

func TestStartWatchdog_NoUnderflowWhenBackfillComplete(t *testing.T) {
	t.Parallel()
	// Simulate backfill complete: lowest > highest (lowest=51742631, highest=51742630)
	// This happens when all blocks up to highest have been processed.
	state, err := NewState(51742630, 51742630)
	require.NoError(t, err)

	// Mark the block as processed and advance lowest
	err = state.MarkProcessed(51742630)
	require.NoError(t, err)
	newLowest, _ := state.AdvanceLowest()
	require.Equal(t, uint64(51742631), newLowest, "lowest should advance past highest when backfill complete")

	core, recorded := observer.New(zap.WarnLevel)
	log := zap.New(core).Sugar()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Run watchdog with maxGap that would catch underflow (huge gap)
	go StartGapWatchdog(ctx, log, state, 5*time.Millisecond, 1000)

	// Wait for a few ticks
	time.Sleep(25 * time.Millisecond)
	cancel()

	// Assert no warnings (gap should be 0, not underflow value)
	for _, e := range recorded.All() {
		if e.Level == zap.WarnLevel && e.Message == "gap too large" {
			// Extract the gap field to see what value was logged
			gapField := e.ContextMap()["gap"]
			require.Failf(t, "unexpected warning", "gap too large warning with gap=%v, this indicates underflow bug", gapField)
		}
	}
}
