package slidingwindow

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/ava-labs/libevm/common"
	"go.uber.org/zap"
)

type workerStub struct {
	err error
}

func (w workerStub) Process(_ context.Context, _ uint64) error {
	return w.err
}

var _ worker.Worker = (*workerStub)(nil)

func TestTryAcquireBackfill(t *testing.T) {
	t.Parallel()

	type prepFunc func(_ *Manager)
	type verifyFunc func(_ *testing.T, _ *Manager)

	tests := []struct {
		name       string
		prep       prepFunc
		verify     verifyFunc
		wantResult bool
	}{
		{
			name: "acquires both when capacity available",
			prep: func(_ *Manager) {
				// nothing to do; both semaphores have 1 permit
			},
			verify: func(t *testing.T, m *Manager) {
				// After success, both semaphores should have consumed one permit.
				if m.backfillSem.TryAcquire(1) {
					t.Fatalf("backfillSem had spare capacity; expected consumed permit")
				}
				if m.workerSem.TryAcquire(1) {
					t.Fatalf("workerSem had spare capacity; expected consumed permit")
				}
				// Cleanup: release the permits consumed by tryAcquireBackfill to avoid cross-test interference.
				m.backfillSem.Release(1)
				m.workerSem.Release(1)
			},
			wantResult: true,
		},
		{
			name: "fails when backfill capacity exhausted",
			prep: func(m *Manager) {
				// Exhaust the single backfill permit.
				if !m.backfillSem.TryAcquire(1) {
					t.Fatalf("failed to acquire backfill permit in prep")
				}
			},
			verify: func(_ *testing.T, m *Manager) {
				// No changes expected to worker semaphore; backfill was already exhausted.
				// Cleanup: release the pre-acquired backfill permit.
				m.backfillSem.Release(1)
			},
			wantResult: false,
		},
		{
			name: "fails and releases backfill when worker capacity exhausted",
			prep: func(m *Manager) {
				// Exhaust the worker permit, leave backfill available to test release behavior.
				if !m.workerSem.TryAcquire(1) {
					t.Fatalf("failed to acquire worker permit in prep")
				}
			},
			verify: func(t *testing.T, m *Manager) {
				// Backfill should have been released by tryAcquireBackfill after worker acquire failed.
				if !m.backfillSem.TryAcquire(1) {
					t.Fatalf("backfillSem did not release permit on worker exhaustion")
				}
				// Cleanup: release both permits acquired during prep/verify.
				m.backfillSem.Release(1)
				m.workerSem.Release(1)
			},
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, err := NewState(0, 0)
			if err != nil {
				t.Fatalf("New state error: %v", err)
			}
			m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, 1, 1, 1, 1)
			if err != nil {
				t.Fatalf("New manager error: %v", err)
			}
			if tt.prep != nil {
				tt.prep(m)
			}
			got := m.tryAcquireBackfill()
			if got != tt.wantResult {
				t.Fatalf("tryAcquireBackfill()=%t, want %t", got, tt.wantResult)
			}
			if tt.verify != nil {
				tt.verify(t, m)
			}
		})
	}
}

func TestProcess(t *testing.T) {
	t.Parallel()

	type cfg struct {
		name               string
		isBackfill         bool
		preAcquireBackfill bool
		workerErr          bool
		initialLowest      uint64
		initialHighest     uint64
		h                  uint64
		expectFailure      bool
		expectLowest       *uint64
	}

	run := func(t *testing.T, c cfg) {
		t.Helper()

		state, err := NewState(c.initialLowest, c.initialHighest)
		if err != nil {
			t.Fatalf("New state error: %v", err)
		}
		w := workerStub{}
		if c.workerErr {
			w.err = assertAnError()
		}
		m, err := NewManager(zap.NewNop().Sugar(), state, w, 1, 1, 1, 1)
		if err != nil {
			t.Fatalf("New manager error: %v", err)
		}
		if !m.workerSem.TryAcquire(1) {
			t.Fatalf("failed to acquire worker permit in prep")
		}
		if c.preAcquireBackfill {
			if !m.backfillSem.TryAcquire(1) {
				t.Fatalf("failed to acquire backfill permit in prep")
			}
		}
		m.state.SetInflight(c.h)

		ctx := context.Background()
		m.process(ctx, c.h, c.isBackfill)

		if got := m.state.IsInflight(c.h); got {
			t.Fatalf("isInflight(%d)=true, want false after process", c.h)
		}
		if !m.workerSem.TryAcquire(1) {
			t.Fatalf("worker semaphore not released by process")
		}
		m.workerSem.Release(1)
		if c.preAcquireBackfill {
			acq := m.backfillSem.TryAcquire(1)
			if c.isBackfill && !acq {
				t.Fatalf("backfill semaphore expected released, but not available")
			}
			if !c.isBackfill && acq {
				t.Fatalf("backfill semaphore should not be released on realtime path")
			}
			if acq {
				m.backfillSem.Release(1)
			}
		}
		if c.expectLowest != nil {
			if got := state.GetLowest(); got != *c.expectLowest {
				t.Fatalf("lowest mismatch: got %d, want %d", got, *c.expectLowest)
			}
		}
		select {
		case v := <-m.failureChan:
			if !c.expectFailure {
				t.Fatalf("unexpected failure signal received: %d", v)
			}
			if v != c.h {
				t.Fatalf("failureChan value=%d, want %d", v, c.h)
			}
		default:
			if c.expectFailure {
				t.Fatalf("expected failure signal, but none received")
			}
		}
		select {
		case <-m.workReady:
		default:
			t.Fatalf("expected workReady signal from defer")
		}
	}

	// helpers for expectations
	ptr := func(v uint64) *uint64 { return &v }
	advance := func(h uint64) *uint64 { x := h + 1; return &x }

	tests := []cfg{
		{
			name:               "worker error: no mark, no advance; release semaphores; signal once",
			isBackfill:         true,
			preAcquireBackfill: true,
			workerErr:          true,
			initialLowest:      0,
			initialHighest:     0,
			h:                  100,
			expectFailure:      true,
			expectLowest:       ptr(0),
		},
		{
			name:               "realtime path: backfill not released on success path flag false",
			isBackfill:         false,
			preAcquireBackfill: true,
			workerErr:          false,
			initialLowest:      10,
			initialHighest:     12,
			h:                  11,
			expectFailure:      false,
			expectLowest:       ptr(10),
		},
		{
			name:               "backfill success: mark and advance called; release both semaphores; signal at least once",
			isBackfill:         true,
			preAcquireBackfill: true,
			workerErr:          false,
			initialLowest:      100,
			initialHighest:     100,
			h:                  100,
			expectFailure:      false,
			expectLowest:       advance(100),
		},
		{
			name:               "mark processed error: no advance; failure signaled",
			isBackfill:         true,
			preAcquireBackfill: true,
			workerErr:          false,
			initialLowest:      0,
			initialHighest:     0,
			h:                  100,
			expectFailure:      true,
			expectLowest:       ptr(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run(t, tt)
		})
	}
}

// assertAnError returns a generic error suitable for mocks.
func assertAnError() error { return context.Canceled }

type headerStub struct {
	n *big.Int
}

func (h headerStub) Number() *big.Int { return h.n }
func (h headerStub) Hash() common.Hash {
	_ = h
	return common.Hash{}
}

func (h headerStub) ParentHash() common.Hash {
	_ = h
	return common.Hash{}
}

func (h headerStub) Time() uint64 {
	_ = h
	return 0
}

func (h headerStub) ChainID() uint64 {
	_ = h
	return 0
}

func TestRun_BackfillAggressiveFill(t *testing.T) {
	t.Parallel()
	// Window has one unprocessed height 5
	state, err := NewState(5, 5)
	if err != nil {
		t.Fatalf("New state error: %v", err)
	}
	m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, 1, 1, 1, 1)
	if err != nil {
		t.Fatalf("New manager error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- m.Run(ctx) }()

	done := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if state.GetLowest() >= 6 {
					done <- struct{}{}
					return
				}
			}
		}
	}()

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for backfill processing")
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for Run to exit")
	}
	// no expectations on stub
}

func TestRun_RealtimeEventFlow(t *testing.T) {
	t.Parallel()
	state, err := NewState(0, 0)
	if err != nil {
		t.Fatalf("New state error: %v", err)
	}
	// Ensure no backfill work is available
	if err := state.MarkProcessed(0); err != nil {
		t.Fatalf("MarkProcessed error: %v", err)
	}
	m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, 1, 1, 1, 1)
	if err != nil {
		t.Fatalf("New manager error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- m.Run(ctx) }()

	// Realtime header arrives
	h := uint64(10)
	m.BlockChan() <- headerStub{n: new(big.Int).SetUint64(h)}

	// Wait until processed and no longer inflight
	done := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if state.IsProcessed(h) && !m.state.IsInflight(h) {
					done <- struct{}{}
					return
				}
			}
		}
	}()

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for realtime processing")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for Run to exit")
	}
	// no expectations on stub
}
