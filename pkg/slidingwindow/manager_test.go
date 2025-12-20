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
			cache, err := NewState(0, 0)
			if err != nil {
				t.Fatalf("New cache error: %v", err)
			}
			m, err := NewManager(zap.NewNop().Sugar(), cache, workerStub{}, 1, 1, 1, 1)
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

func TestFindNextUnclaimedBlock(t *testing.T) {
	t.Parallel()
	type fields struct {
		lowest    uint64
		highest   uint64
		processed []uint64
		inflight  []uint64
	}
	type want struct {
		height uint64
		ok     bool
	}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "returns first unprocessed and not inflight",
			fields: fields{
				lowest: 5, highest: 7,
				processed: []uint64{5},
			},
			want: want{height: 6, ok: true},
		},
		{
			name: "skips inflight heights",
			fields: fields{
				lowest: 5, highest: 7,
				inflight: []uint64{5},
			},
			want: want{height: 6, ok: true},
		},
		{
			name: "all processed returns none",
			fields: fields{
				lowest: 5, highest: 7,
				processed: []uint64{5, 6, 7},
			},
			want: want{height: 0, ok: false},
		},
		{
			name: "single height available",
			fields: fields{
				lowest: 10, highest: 10,
			},
			want: want{height: 10, ok: true},
		},
		{
			name: "single height inflight returns none",
			fields: fields{
				lowest: 10, highest: 10,
				inflight: []uint64{10},
			},
			want: want{height: 0, ok: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := NewState(tt.fields.lowest, tt.fields.highest)
			if err != nil {
				t.Fatalf("New cache error: %v", err)
			}
			m, err := NewManager(zap.NewNop().Sugar(), cache, workerStub{}, 1, 1, 1, 1)
			if err != nil {
				t.Fatalf("New manager error: %v", err)
			}

			for _, h := range tt.fields.processed {
				if err := m.cache.MarkProcessed(h); err != nil {
					t.Fatalf("MarkProcessed(%d) error: %v", h, err)
				}
			}

			for _, h := range tt.fields.inflight {
				m.setInflight(h)
			}

			gotH, gotOK := m.findNextUnclaimedBlock()
			if gotH != tt.want.height || gotOK != tt.want.ok {
				t.Fatalf("findNextUnclaimedBlock()=(%d,%t), want (%d,%t)", gotH, gotOK, tt.want.height, tt.want.ok)
			}
		})
	}
}

func TestSetInflight(t *testing.T) {
	t.Parallel()

	type step struct {
		height       uint64
		value        bool
		wantInFlight bool
	}
	tests := []struct {
		name    string
		initial map[uint64]bool
		steps   []step
	}{
		{
			name:    "add new height",
			initial: map[uint64]bool{},
			steps: []step{
				{height: 10, value: true, wantInFlight: true},
			},
		},
		{
			name:    "remove existing height",
			initial: map[uint64]bool{10: true},
			steps: []step{
				{height: 10, value: false, wantInFlight: false},
			},
		},
		{
			name:    "remove non-existent height is no-op",
			initial: map[uint64]bool{},
			steps: []step{
				{height: 11, value: false, wantInFlight: false},
			},
		},
		{
			name:    "toggle add-remove-add",
			initial: map[uint64]bool{},
			steps: []step{
				{height: 12, value: true, wantInFlight: true},
				{height: 12, value: false, wantInFlight: false},
				{height: 12, value: true, wantInFlight: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Manager with minimal setup
			cache, err := NewState(0, 0)
			if err != nil {
				t.Fatalf("New cache error: %v", err)
			}
			m, err := NewManager(zap.NewNop().Sugar(), cache, workerStub{}, 1, 1, 1, 1)
			if err != nil {
				t.Fatalf("New manager error: %v", err)
			}
			// Seed initial inflight map
			for h, v := range tt.initial {
				if v {
					m.setInflight(h)
				} else {
					m.unsetInflight(h)
				}
			}
			// Execute steps
			for _, s := range tt.steps {
				if s.value {
					m.setInflight(s.height)
				} else {
					m.unsetInflight(s.height)
				}
				got := m.isInflight(s.height)
				if got != s.wantInFlight {
					t.Fatalf("after setInflight(%d,%t): isInflight=%t, want %t", s.height, s.value, got, s.wantInFlight)
				}
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

		cache, err := NewState(c.initialLowest, c.initialHighest)
		if err != nil {
			t.Fatalf("New cache error: %v", err)
		}
		w := workerStub{}
		if c.workerErr {
			w.err = assertAnError()
		}
		m, err := NewManager(zap.NewNop().Sugar(), cache, w, 1, 1, 1, 1)
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
		m.setInflight(c.h)

		ctx := context.Background()
		m.process(ctx, c.h, c.isBackfill)

		if got := m.isInflight(c.h); got {
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
			if got := cache.GetLowest(); got != *c.expectLowest {
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
	cache, err := NewState(5, 5)
	if err != nil {
		t.Fatalf("New cache error: %v", err)
	}
	m, err := NewManager(zap.NewNop().Sugar(), cache, workerStub{}, 1, 1, 1, 1)
	if err != nil {
		t.Fatalf("New manager error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- m.Run(ctx) }()

	// Signal when LUB advances past 5 (i.e., backfill processed)
	done := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if cache.GetLowest() >= 6 {
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
	cache, err := NewState(0, 0)
	if err != nil {
		t.Fatalf("New cache error: %v", err)
	}
	// Ensure no backfill work is available
	if err := cache.MarkProcessed(0); err != nil {
		t.Fatalf("MarkProcessed error: %v", err)
	}
	m, err := NewManager(zap.NewNop().Sugar(), cache, workerStub{}, 1, 1, 1, 1)
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
				if cache.IsProcessed(h) && !m.isInflight(h) {
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
