package slidingwindow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type workerStub struct {
	err error
}

func (w workerStub) Process(_ context.Context, _ uint64) error {
	return w.err
}

var _ worker.Worker = (*workerStub)(nil)

// blockingWorker allows tests to observe when a worker starts and to delay completion.
type blockingWorker struct {
	start chan uint64
	done  chan struct{}
	err   error
}

func (w blockingWorker) Process(_ context.Context, h uint64) error {
	if w.start != nil {
		w.start <- h
	}
	if w.done != nil {
		<-w.done
	}
	return w.err
}

func TestNewManager_Validation(t *testing.T) {
	t.Parallel()
	validLogger := zap.NewNop().Sugar()
	validState, err := NewState(0, 0)
	require.NoError(t, err)
	validWorker := workerStub{}

	type args struct {
		log               *zap.SugaredLogger
		state             *State
		worker            worker.Worker
		concurrency       int64
		backfillPriority  int64
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
				nil, // metrics
			)
			if tt.wantErr {
				require.Contains(t, err.Error(), tt.errContains)
				require.Nil(t, m)
				return
			} else {
				require.NoError(t, err)
				require.NotNil(t, m)
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
					require.Fail(t, "backfillSem had spare capacity; expected consumed permit")
				}
				// Worker semaphore should still have one remaining permit (capacity 2 total).
				if !m.workerSem.TryAcquire(1) {
					require.Fail(t, "workerSem expected one remaining permit after acquisition")
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
					require.Fail(t, "failed to acquire backfill permit in prep")
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
					require.Fail(t, "failed to acquire worker permits in prep")
				}
			},
			verify: func(t *testing.T, m *Manager) {
				// Backfill should have been released by tryAcquireBackfill after worker acquire failed.
				if !m.backfillSem.TryAcquire(1) {
					require.Fail(t, "backfillSem did not release permit on worker exhaustion")
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
			require.NoError(t, err)
			// backfillPriority must be strictly less than concurrency now (2 > 1)
			m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, 2, 1, 1, 1, nil)
			require.NoError(t, err)
			if tt.prep != nil {
				tt.prep(m)
			}
			got := m.tryAcquireBackfill()
			if got != tt.wantResult {
				require.Equal(t, tt.wantResult, got)
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
		skipInflight       bool
	}

	run := func(t *testing.T, c cfg) {
		t.Helper()

		state, err := NewState(c.initialLowest, c.initialHighest)
		require.NoError(t, err)
		w := workerStub{}
		if c.workerErr {
			w.err = assertAnError()
		}
		// backfillPriority must be strictly less than concurrency now (2 > 1)
		m, err := NewManager(zap.NewNop().Sugar(), state, w, 2, 1, 1, 1, nil)
		require.NoError(t, err)
		if !m.workerSem.TryAcquire(1) {
			require.Fail(t, "failed to acquire worker permit in prep")
		}
		if c.preAcquireBackfill {
			if !m.backfillSem.TryAcquire(1) {
				require.Fail(t, "failed to acquire backfill permit in prep")
			}
		}
		if !c.skipInflight {
			// Ensure the height is within the window to allow claiming inflight.
			// Historically we expanded Highest to claim inflight, but some tests intentionally
			// want MarkProcessed to fail (h > highest). Those will set skipInflight=true.
			if c.h > c.initialHighest {
				if ok := m.state.SetHighest(c.h); !ok {
					require.Failf(t, "SetHighest", "failed to expand Highest for claim: %v", err)
				}
			}
			if ok := m.state.TrySetInflight(c.h); !ok {
				require.Failf(t, "TrySetInflight", "failed to set inflight for height %d", c.h)
			}
		}

		ctx := t.Context()
		m.process(ctx, c.h, c.isBackfill)

		if got := m.state.IsInflight(c.h); got {
			require.Failf(t, "IsInflight", "for height %d true, want false after process", c.h)
		}
		if !m.workerSem.TryAcquire(1) {
			require.Fail(t, "worker semaphore not released by process")
		}
		m.workerSem.Release(1)
		if c.preAcquireBackfill {
			acq := m.backfillSem.TryAcquire(1)
			if c.isBackfill && !acq {
				require.Fail(t, "backfill semaphore expected released, but not available")
			}
			if !c.isBackfill && acq {
				require.Fail(t, "backfill semaphore should not be released on realtime path")
			}
			if acq {
				m.backfillSem.Release(1)
			}
		}
		if c.expectLowest != nil {
			if got := state.GetLowest(); got != *c.expectLowest {
				require.Failf(t, "GetLowest", "lowest mismatch: got %d, want %d", got, *c.expectLowest)
			}
		}
		select {
		case v := <-m.failureChan:
			if !c.expectFailure {
				require.Failf(t, "failureChan", "unexpected failure signal received: %d", v)
			}
			if v != c.h {
				require.Failf(t, "failureChan", "value=%d, want %d", v, c.h)
			}
		default:
			if c.expectFailure {
				require.Fail(t, "expected failure signal, but none received")
			}
		}
		select {
		case <-m.workReady:
		default:
			require.Fail(t, "expected workReady signal from defer")
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
		{
			name:               "mark processed error: height > highest triggers failure and no advance",
			isBackfill:         true,
			preAcquireBackfill: true,
			workerErr:          false,
			initialLowest:      0,
			initialHighest:     0,
			h:                  100, // greater than highest; do not set inflight to force MarkProcessed error
			skipInflight:       true,
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
func assertAnError() error { return errors.New("synthetic failure") }

func TestRun_BackfillAggressiveFill(t *testing.T) {
	t.Parallel()
	// Window has one unprocessed height 5
	state, err := NewState(5, 5)
	require.NoError(t, err)
	// backfillPriority must be strictly less than concurrency now (2 > 1)
	m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, 2, 1, 1, 1, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
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
		require.Fail(t, "timeout waiting for backfill processing")
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			require.Failf(t, "Run", "returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for Run to exit")
	}
	// no expectations on stub
}

func TestRun_RealtimeEventFlow(t *testing.T) {
	t.Parallel()
	state, err := NewState(0, 0)
	require.NoError(t, err)
	// Ensure no backfill work is available
	if err := state.MarkProcessed(0); err != nil {
		require.Failf(t, "MarkProcessed", "error: %v", err)
	}
	// backfillPriority must be strictly less than concurrency now (2 > 1)
	m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, 2, 1, 1, 1, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
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
		require.Fail(t, "timeout waiting for realtime processing")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			require.Failf(t, "Run", "returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for Run to exit")
	}
}

func TestRun_FailureChain(t *testing.T) {
	t.Parallel()
	// Window has one unprocessed height 5; worker always fails; maxFailures triggers shutdown.
	state, err := NewState(5, 5)
	require.NoError(t, err)
	maxFailures := 3
	m, err := NewManager(
		zap.NewNop().Sugar(),
		state,
		workerStub{err: assertAnError()},
		2,
		1,
		1,
		maxFailures,
		nil, // metrics
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- m.Run(ctx) }()

	select {
	case runErr := <-errCh:
		if runErr == nil {
			require.Fail(t, "expected error, got nil")
		}
		// Validate message contains sentinel text and additional context.
		msg := runErr.Error()
		if !strings.Contains(msg, "max failures exceeded for block") {
			require.Failf(t, "Run", "expected sentinel error message, got: %q", msg)
		}
		if !strings.Contains(msg, "block 5") || !strings.Contains(msg, "after 3 attempts") {
			require.Failf(t, "Run", "unexpected error message: %q", msg)
		}
	case <-time.After(3 * time.Second):
		require.Fail(t, "timeout waiting for failure threshold to trigger")
	}
}

func TestHandleNewHeight(t *testing.T) {
	t.Parallel()

	type args struct {
		initialLowest      uint64
		initialHighest     uint64
		concurrency        int64
		backfillPri        int64
		preAcquireBackfill bool
		exhaustWorkers     int
		height             uint64
		setHighestToH      bool
		markProcessedH     bool
		useBlockingWorker  bool
	}
	type want struct {
		yieldedToBackfill bool
		startedRealtime   bool
		inflightAfter     bool
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "yield to backfill when backlog exists and backfill capacity available",
			args: args{
				initialLowest: 5, initialHighest: 6,
				concurrency: 2, backfillPri: 1,
				height: 10,
			},
			want: want{
				yieldedToBackfill: true,
				startedRealtime:   false,
				inflightAfter:     false,
			},
		},
		{
			name: "realtime proceeds when backfill capacity exhausted",
			args: args{
				initialLowest: 5, initialHighest: 6,
				concurrency: 2, backfillPri: 1,
				preAcquireBackfill: true,
				height:             10, setHighestToH: true,
				useBlockingWorker: true,
			},
			want: want{
				yieldedToBackfill: false,
				startedRealtime:   true,
				inflightAfter:     false, // cleared after completion
			},
		},
		{
			name: "drops realtime when no worker capacity",
			args: args{
				initialLowest: 0, initialHighest: 100,
				concurrency: 2, backfillPri: 1,
				exhaustWorkers: 2,
				height:         50, setHighestToH: true,
			},
			want: want{
				yieldedToBackfill: false,
				startedRealtime:   false,
				inflightAfter:     false,
			},
		},
		{
			name: "TrySetInflight failure releases worker capacity",
			args: args{
				// Use a window with no backlog and mark h as processed so TrySetInflight(h) fails.
				initialLowest: 11, initialHighest: 11,
				concurrency: 2, backfillPri: 1,
				height:         11,
				markProcessedH: true,
			},
			want: want{
				yieldedToBackfill: false,
				startedRealtime:   false,
				inflightAfter:     false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state, err := NewState(tt.args.initialLowest, tt.args.initialHighest)
			require.NoError(t, err)
			var w worker.Worker = workerStub{}
			var start chan uint64
			var done chan struct{}
			if tt.args.useBlockingWorker {
				start = make(chan uint64, 1)
				done = make(chan struct{})
				w = blockingWorker{start: start, done: done}
			}
			m, err := NewManager(zap.NewNop().Sugar(), state, w, tt.args.concurrency, tt.args.backfillPri, 1, 1, nil)
			require.NoError(t, err)

			if tt.args.preAcquireBackfill {
				if !m.backfillSem.TryAcquire(1) {
					require.Fail(t, "failed to pre-acquire backfill permit")
				}
			}
			if tt.args.exhaustWorkers > 0 {
				if !m.workerSem.TryAcquire(int64(tt.args.exhaustWorkers)) {
					require.Fail(t, "failed to exhaust workerSem")
				}
				defer m.workerSem.Release(int64(tt.args.exhaustWorkers))
			}
			if tt.args.setHighestToH {
				_ = state.SetHighest(tt.args.height)
			}
			if tt.args.markProcessedH {
				if err := state.MarkProcessed(tt.args.height); err != nil {
					require.Failf(t, "MarkProcessed", "failed to mark processed: %v", err)
				}
			}

			// Capture worker capacity before call to detect yield
			hadWorkerCapacity := m.workerSem.TryAcquire(1)
			if hadWorkerCapacity {
				m.workerSem.Release(1)
			}
			m.handleNewHeight(t.Context(), tt.args.height)

			// If yielded, there should be no inflight and worker capacity remains available.
			if tt.want.yieldedToBackfill {
				if state.IsInflight(tt.args.height) {
					require.Fail(t, "expected yield: no inflight for realtime height")
				}
				if !m.workerSem.TryAcquire(1) {
					require.Fail(t, "expected yield: worker capacity should be available")
				}
				m.workerSem.Release(1)
				return
			}

			if tt.args.useBlockingWorker && tt.want.startedRealtime {
				select {
				case got := <-start:
					if got != tt.args.height {
						require.Failf(t, "started", "height=%d, want %d", got, tt.args.height)
					}
				case <-time.After(500 * time.Millisecond):
					require.Fail(t, "timeout waiting for worker to start")
				}
				if !state.IsInflight(tt.args.height) {
					require.Failf(t, "IsInflight", "expected height %d to be inflight", tt.args.height)
				}
				// Complete worker and wait for inflight to clear
				close(done)
				deadline := time.Now().Add(1 * time.Second)
				for time.Now().Before(deadline) {
					if !state.IsInflight(tt.args.height) {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if state.IsInflight(tt.args.height) {
					require.Fail(t, "inflight not cleared after worker completion")
				}
				// Worker permit should be available again.
				if !m.workerSem.TryAcquire(1) {
					require.Fail(t, "workerSem not released after worker completion")
				}
				m.workerSem.Release(1)
			}

			// For non-started paths, ensure inflight matches expectation
			if !tt.want.startedRealtime && state.IsInflight(tt.args.height) != tt.want.inflightAfter {
				require.Failf(t, "IsInflight", "for height %d got %t, want %t", tt.args.height, state.IsInflight(tt.args.height), tt.want.inflightAfter)
			}
		})
	}
}

func TestSubmitHeight(t *testing.T) {
	t.Parallel()
	type args struct {
		initialLowest  uint64
		initialHighest uint64
		concurrency    int64
		backfillPri    int64
		queueCap       int
		submitHeights  []uint64
	}
	type want struct {
		results      []bool
		finalHighest uint64
		queueValues  []uint64
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "increase highest and enqueue when space available",
			args: args{
				initialLowest: 0, initialHighest: 0,
				concurrency: 2, backfillPri: 1, queueCap: 2,
				submitHeights: []uint64{1},
			},
			want: want{
				results:      []bool{true},
				finalHighest: 1,
				queueValues:  []uint64{1},
			},
		},
		{
			name: "increase highest but channel full returns false",
			args: args{
				initialLowest: 0, initialHighest: 0,
				concurrency: 2, backfillPri: 1, queueCap: 1,
				submitHeights: []uint64{1, 2},
			},
			want: want{
				results:      []bool{true, false},
				finalHighest: 2,           // highest still raised even if enqueue failed
				queueValues:  []uint64{1}, // only first enqueued due to full buffer
			},
		},
		{
			name: "lower or equal height does not enqueue and returns false (monotonic highest)",
			args: args{
				initialLowest: 0, initialHighest: 5,
				concurrency: 2, backfillPri: 1, queueCap: 2,
				submitHeights: []uint64{3, 5},
			},
			want: want{
				results:      []bool{false, false},
				finalHighest: 5,
				queueValues:  []uint64{}, // nothing enqueued
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state, err := NewState(tt.args.initialLowest, tt.args.initialHighest)
			require.NoError(t, err)
			m, err := NewManager(zap.NewNop().Sugar(), state, workerStub{}, tt.args.concurrency, tt.args.backfillPri, tt.args.queueCap, 1, nil)
			require.NoError(t, err)

			var gotResults []bool
			for _, h := range tt.args.submitHeights {
				gotResults = append(gotResults, m.SubmitHeight(h))
			}
			// Check results
			if len(gotResults) != len(tt.want.results) {
				require.Failf(t, "SubmitHeight", "results len=%d, want %d", len(gotResults), len(tt.want.results))
			}
			for i := range gotResults {
				if gotResults[i] != tt.want.results[i] {
					require.Failf(t, "SubmitHeight", "result[%d]=%t, want %t", i, gotResults[i], tt.want.results[i])
				}
			}
			// Check final highest
			if got := state.GetHighest(); got != tt.want.finalHighest {
				require.Failf(t, "GetHighest", "final highest=%d, want %d", got, tt.want.finalHighest)
			}
			// Drain queue and compare enqueued values
			var drained []uint64
			for {
				select {
				case h := <-m.heightChan:
					drained = append(drained, h)
				default:
					goto compare
				}
			}
		compare:
			if len(drained) != len(tt.want.queueValues) {
				require.Failf(t, "SubmitHeight", "queued len=%d, want %d (values=%v)", len(drained), len(tt.want.queueValues), drained)
			}
			for i := range drained {
				if drained[i] != tt.want.queueValues[i] {
					require.Failf(t, "SubmitHeight", "queued[%d]=%d, want %d", i, drained[i], tt.want.queueValues[i])
				}
			}
		})
	}
}
