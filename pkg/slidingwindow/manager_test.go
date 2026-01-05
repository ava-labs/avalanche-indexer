package slidingwindow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"go.uber.org/zap"
)

type workerStub struct {
	err error
}

func (w workerStub) Process(_ context.Context, _ uint64) error {
	return w.err
}

var _ worker.Worker = (*workerStub)(nil)

func TestNewManager_Validation(t *testing.T) {
	t.Parallel()
	validLogger := zap.NewNop().Sugar()
	validState, err := NewState(0, 0)
	if err != nil {
		t.Fatalf("New state error: %v", err)
	}
	validWorker := workerStub{}

	type args struct {
		log               *zap.SugaredLogger
		state             *State
		worker            worker.Worker
		concurrency       uint64
		backfillPriority  uint64
		heightsChCapacity int
		maxFailures       int
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		errContains string
	}{
		{
			name: "ok: valid arguments",
			args: args{
				log:               validLogger,
				state:             validState,
				worker:            validWorker,
				concurrency:       2,
				backfillPriority:  1,
				heightsChCapacity: 8,
				maxFailures:       3,
			},
			wantErr: false,
		},
		{
			name: "error: nil logger",
			args: args{
				log:               nil,
				state:             validState,
				worker:            validWorker,
				concurrency:       1,
				backfillPriority:  1,
				heightsChCapacity: 1,
				maxFailures:       1,
			},
			wantErr:     true,
			errContains: "invalid logger",
		},
		{
			name: "error: nil state",
			args: args{
				log:               validLogger,
				state:             nil,
				worker:            validWorker,
				concurrency:       1,
				backfillPriority:  1,
				heightsChCapacity: 1,
				maxFailures:       1,
			},
			wantErr:     true,
			errContains: "invalid state",
		},
		{
			name: "error: nil worker",
			args: args{
				log:               validLogger,
				state:             validState,
				worker:            nil,
				concurrency:       1,
				backfillPriority:  1,
				heightsChCapacity: 1,
				maxFailures:       1,
			},
			wantErr:     true,
			errContains: "invalid worker",
		},
		{
			name: "error: concurrency zero",
			args: args{
				log:               validLogger,
				state:             validState,
				worker:            validWorker,
				concurrency:       0,
				backfillPriority:  1,
				heightsChCapacity: 1,
				maxFailures:       1,
			},
			wantErr:     true,
			errContains: "invalid concurrency",
		},
		{
			name: "error: backfill priority zero",
			args: args{
				log:               validLogger,
				state:             validState,
				worker:            validWorker,
				concurrency:       2,
				backfillPriority:  0,
				heightsChCapacity: 1,
				maxFailures:       1,
			},
			wantErr:     true,
			errContains: "invalid backfill priority",
		},
		{
			name: "error: backfill priority greater than concurrency",
			args: args{
				log:               validLogger,
				state:             validState,
				worker:            validWorker,
				concurrency:       2,
				backfillPriority:  3,
				heightsChCapacity: 1,
				maxFailures:       1,
			},
			wantErr:     true,
			errContains: "invalid backfill priority",
		},
		{
			name: "error: heights channel capacity zero",
			args: args{
				log:               validLogger,
				state:             validState,
				worker:            validWorker,
				concurrency:       2,
				backfillPriority:  1,
				heightsChCapacity: 0,
				maxFailures:       1,
			},
			wantErr:     true,
			errContains: "invalid new heights channel capacity",
		},
		{
			name: "error: max failures zero",
			args: args{
				log:               validLogger,
				state:             validState,
				worker:            validWorker,
				concurrency:       2,
				backfillPriority:  1,
				heightsChCapacity: 1,
				maxFailures:       0,
			},
			wantErr:     true,
			errContains: "invalid max failures",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m, err := NewManager(
				tt.args.log,
				tt.args.state,
				tt.args.worker,
				tt.args.concurrency,
				tt.args.backfillPriority,
				tt.args.heightsChCapacity,
				tt.args.maxFailures,
			)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				if m != nil {
					t.Fatalf("expected nil manager on error, got non-nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if m == nil {
				t.Fatalf("expected non-nil manager on success")
			}
		})
	}
}

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
				// nothing to do; backfill has 1 permit, worker has 2 permits
			},
			verify: func(t *testing.T, m *Manager) {
				// After success, both semaphores should have consumed one permit.
				if m.backfillSem.TryAcquire(1) {
					t.Fatalf("backfillSem had spare capacity; expected consumed permit")
				}
				// Worker semaphore should still have one remaining permit (capacity 2 total).
				if !m.workerSem.TryAcquire(1) {
					t.Fatalf("workerSem expected one remaining permit after acquisition")
				}
				// release the extra permit acquired for verification
				m.workerSem.Release(1)
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
				// Exhaust all worker permits (capacity=2), leave backfill available to test release behavior.
				if !m.workerSem.TryAcquire(2) {
					t.Fatalf("failed to acquire worker permits in prep")
				}
			},
			verify: func(t *testing.T, m *Manager) {
				// Backfill should have been released by tryAcquireBackfill after worker acquire failed.
				if !m.backfillSem.TryAcquire(1) {
					t.Fatalf("backfillSem did not release permit on worker exhaustion")
				}
				// Cleanup: release permits acquired during prep/verify.
				m.backfillSem.Release(1)
				m.workerSem.Release(2)
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
			// backfillPriority must be strictly less than concurrency now (2 > 1)
			m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, 2, 1, 1, 1)
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
		// backfillPriority must be strictly less than concurrency now (2 > 1)
		m, err := NewManager(zap.NewNop().Sugar(), state, w, 2, 1, 1, 1)
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
		// Ensure the height is within the window to allow claiming inflight.
		// Some tests intentionally start with Highest < h to trigger MarkProcessed errors later.
		// We temporarily expand Highest to claim inflight, then restore it.
		if c.h > c.initialHighest {
			if ok := m.state.SetHighest(c.h); !ok {
				t.Fatalf("failed to expand Highest for claim: %v", err)
			}
		}
		if ok := m.state.TrySetInflight(c.h); !ok {
			t.Fatalf("failed to set inflight for height %d", c.h)
		}
		// Restore original Highest if it was below h, so subsequent logic (e.g., MarkProcessed error) matches expectations.
		if c.initialHighest < c.h {
			_ = m.state.SetHighest(c.initialHighest)
		}

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
			name:               "mark processed succeeds with monotonic highest: no advance; no failure",
			isBackfill:         true,
			preAcquireBackfill: true,
			workerErr:          false,
			initialLowest:      0,
			initialHighest:     0,
			h:                  100,
			expectFailure:      false,
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

func TestRun_BackfillAggressiveFill(t *testing.T) {
	t.Parallel()
	// Window has one unprocessed height 5
	state, err := NewState(5, 5)
	if err != nil {
		t.Fatalf("New state error: %v", err)
	}
	// backfillPriority must be strictly less than concurrency now (2 > 1)
	m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, 2, 1, 1, 1)
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
	// backfillPriority must be strictly less than concurrency now (2 > 1)
	m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, 2, 1, 1, 1)
	if err != nil {
		t.Fatalf("New manager error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- m.Run(ctx) }()

	// Realtime header arrives
	h := uint64(10)
	m.SubmitHeight(h)

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
}

func TestRun_FailureChain(t *testing.T) {
	t.Parallel()
	// Window has one unprocessed height 5; worker always fails; maxFailures triggers shutdown.
	state, err := NewState(5, 5)
	if err != nil {
		t.Fatalf("New state error: %v", err)
	}
	maxFailures := 3
	m, err := NewManager(
		zap.NewNop().Sugar(),
		state,
		workerStub{err: assertAnError()},
		2,
		1,
		1,
		maxFailures,
	)
	if err != nil {
		t.Fatalf("New manager error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- m.Run(ctx) }()

	select {
	case runErr := <-errCh:
		if runErr == nil {
			t.Fatalf("expected error, got nil")
		}
		// Validate message contains sentinel text and additional context.
		msg := runErr.Error()
		if !strings.Contains(msg, "max failures exceeded for block") {
			t.Fatalf("expected sentinel error message, got: %q", msg)
		}
		if !strings.Contains(msg, "block 5") || !strings.Contains(msg, "after 3 attempts") {
			t.Fatalf("unexpected error message: %q", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for failure threshold to trigger")
	}
}
