package manager

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/internal/block-fetcher/worker"
	"github.com/ava-labs/avalanche-indexer/internal/chainclient"
	"github.com/ava-labs/avalanche-indexer/internal/types"
	"github.com/ava-labs/libevm/common"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

type slidingWindowRepoMock struct {
	mock.Mock
}

func (m *slidingWindowRepoMock) Window() (uint64, uint64) {
	args := m.Called()
	return args.Get(0).(uint64), args.Get(1).(uint64)
}

func (m *slidingWindowRepoMock) GetLUB() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *slidingWindowRepoMock) GetHIB() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *slidingWindowRepoMock) SetHIB(newHIB uint64) error {
	args := m.Called(newHIB)
	return args.Error(0)
}

func (m *slidingWindowRepoMock) ResetLUB(newLUB uint64) error {
	args := m.Called(newLUB)
	return args.Error(0)
}

func (m *slidingWindowRepoMock) HasWork() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *slidingWindowRepoMock) MarkProcessed(h uint64) error {
	args := m.Called(h)
	return args.Error(0)
}

func (m *slidingWindowRepoMock) IsProcessed(h uint64) bool {
	args := m.Called(h)
	return args.Get(0).(bool)
}

func (m *slidingWindowRepoMock) AdvanceLUB() (uint64, bool) {
	args := m.Called()
	return args.Get(0).(uint64), args.Get(1).(bool)
}

var _ types.SlidingWindowRepository = (*slidingWindowRepoMock)(nil)

type workerStub struct {
	err error
}

func (w workerStub) Process(ctx context.Context, height uint64) error {
	return w.err
}

var _ worker.Worker = (*workerStub)(nil)

func TestTryAcquireBackfill(t *testing.T) {
	t.Parallel()

	type prepFunc func(m *Manager)
	type verifyFunc func(t *testing.T, m *Manager)

	tests := []struct {
		name       string
		prep       prepFunc
		verify     verifyFunc
		wantResult bool
	}{
		{
			name: "acquires both when capacity available",
			prep: func(m *Manager) {
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
			verify: func(t *testing.T, m *Manager) {
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
			m, err := New(zap.NewNop().Sugar(), &slidingWindowRepoMock{}, workerStub{}, 1, 1, 1)
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
		lub       uint64
		hib       uint64
		processed map[uint64]bool
		inflight  map[uint64]bool
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
				lub: 5, hib: 7,
				processed: map[uint64]bool{5: true, 6: false, 7: false},
			},
			want: want{height: 6, ok: true},
		},
		{
			name: "skips inflight heights",
			fields: fields{
				lub: 5, hib: 7,
				processed: map[uint64]bool{5: false, 6: false, 7: false},
				inflight:  map[uint64]bool{5: true},
			},
			want: want{height: 6, ok: true},
		},
		{
			name: "all processed returns none",
			fields: fields{
				lub: 5, hib: 7,
				processed: map[uint64]bool{5: true, 6: true, 7: true},
			},
			want: want{height: 0, ok: false},
		},
		{
			name: "single height available",
			fields: fields{
				lub: 10, hib: 10,
				processed: map[uint64]bool{10: false},
			},
			want: want{height: 10, ok: true},
		},
		{
			name: "single height inflight returns none",
			fields: fields{
				lub: 10, hib: 10,
				processed: map[uint64]bool{10: false},
				inflight:  map[uint64]bool{10: true},
			},
			want: want{height: 0, ok: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &slidingWindowRepoMock{}
			repo.On("Window").Return(tt.fields.lub, tt.fields.hib)
			// Setup IsProcessed expectations for every height in [lub, hib]
			for h := tt.fields.lub; h <= tt.fields.hib; h++ {
				val := false
				if processed, ok := tt.fields.processed[h]; ok {
					val = processed
				}
				repo.On("IsProcessed", h).Return(val)
			}

			m, err := New(zap.NewNop().Sugar(), repo, workerStub{}, 1, 1, 1)
			if err != nil {
				t.Fatalf("New manager error: %v", err)
			}
			// Mark inflight heights if any
			for h, v := range tt.fields.inflight {
				m.setInflight(h, v)
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
			m, err := New(zap.NewNop().Sugar(), &slidingWindowRepoMock{}, workerStub{}, 1, 1, 1)
			if err != nil {
				t.Fatalf("New manager error: %v", err)
			}
			// Seed initial inflight map
			for h, v := range tt.initial {
				m.setInflight(h, v)
			}
			// Execute steps
			for _, s := range tt.steps {
				m.setInflight(s.height, s.value)
				got := m.isInflight(s.height)
				if got != s.wantInFlight {
					t.Fatalf("after setInflight(%d,%t): isInflight=%t, want %t", s.height, s.value, got, s.wantInFlight)
				}
			}
		})
	}
}

type chainClientStub struct {
	block types.Block
	err   error
}

func (s chainClientStub) BlockByNumber(ctx context.Context, height uint64) (types.Block, error) {
	return s.block, s.err
}

var _ chainclient.ChainClient = (*chainClientStub)(nil)

func TestProcess(t *testing.T) {
	t.Parallel()

	evmBlock := &types.EVMBlock{}
	type args struct {
		isBackfill         bool
		preAcquireBackfill bool
		workerErr          bool
		advanceChanged     bool
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "worker error: no mark, no advance; release semaphores; signal once",
			args: args{
				isBackfill:         true,
				preAcquireBackfill: true,
				workerErr:          true,
			},
		},
		{
			name: "realtime path: backfill not released on success path flag false",
			args: args{
				isBackfill:         false,
				preAcquireBackfill: true,
				workerErr:          false,
				advanceChanged:     false,
			},
		},
		{
			name: "backfill success: mark and advance called; release both semaphores; signal at least once",
			args: args{
				isBackfill:         true,
				preAcquireBackfill: true,
				workerErr:          false,
				advanceChanged:     true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock repo expectations
			repo := &slidingWindowRepoMock{}
			h := uint64(100)
			if tt.args.workerErr {
				// No calls expected to MarkProcessed or AdvanceLUB
			} else {
				repo.On("MarkProcessed", h).Return(nil).Once()
				repo.On("AdvanceLUB").Return(uint64(0), tt.args.advanceChanged).Once()
			}

			// Worker stub
			cc := chainClientStub{}
			if tt.args.workerErr {
				cc.err = context.Canceled
			} else {
				cc.block = evmBlock
			}
			w := worker.New(cc)

			m, err := New(zap.NewNop().Sugar(), repo, w, 1, 1, 1)
			if err != nil {
				t.Fatalf("New manager error: %v", err)
			}

			// Simulate acquired semaphores like in runtime before calling process.
			if !m.workerSem.TryAcquire(1) {
				t.Fatalf("failed to pre-acquire worker semaphore")
			}
			if tt.args.preAcquireBackfill {
				if !m.backfillSem.TryAcquire(1) {
					t.Fatalf("failed to pre-acquire backfill semaphore")
				}
			}
			// Mark inflight true before processing
			m.setInflight(h, true)

			// Call process synchronously
			m.process(context.Background(), h, tt.args.isBackfill)

			// Repo expectations
			repo.AssertExpectations(t)

			// inflight cleared
			if got := m.isInflight(h); got {
				t.Fatalf("isInflight(%d)=true, want false after process", h)
			}

			// worker semaphore must be released
			if !m.workerSem.TryAcquire(1) {
				t.Fatalf("worker semaphore not released by process")
			}
			// cleanup release
			m.workerSem.Release(1)

			// backfill semaphore released only when isBackfill==true
			if tt.args.preAcquireBackfill {
				acq := m.backfillSem.TryAcquire(1)
				if tt.args.isBackfill && !acq {
					t.Fatalf("backfill semaphore expected released, but not available")
				}
				if !tt.args.isBackfill && acq {
					t.Fatalf("backfill semaphore should not be released on realtime path")
				}
				if acq {
					// cleanup release
					m.backfillSem.Release(1)
				}
			}

			// backfillReady must have at least one signal (from defer)
			select {
			case <-m.backfillReady:
				// ok
			default:
				t.Fatalf("expected backfillReady signal from defer")
			}
		})
	}
}

type headerStub struct {
	n *big.Int
}

func (h headerStub) Number() *big.Int        { return h.n }
func (h headerStub) Hash() common.Hash       { return common.Hash{} }
func (h headerStub) ParentHash() common.Hash { return common.Hash{} }
func (h headerStub) Time() uint64            { return 0 }
func (h headerStub) ChainID() uint64         { return 0 }

func TestRun_BackfillAggressiveFill(t *testing.T) {
	t.Parallel()

	repo := &slidingWindowRepoMock{}
	// Window has one unprocessed height 5
	repo.On("Window").Return(uint64(5), uint64(5))
	repo.On("IsProcessed", uint64(5)).Return(false)
	// MarkProcessed and AdvanceLUB expected once
	done := make(chan struct{})
	repo.On("MarkProcessed", uint64(5)).Return(nil).Run(func(args mock.Arguments) {
		close(done)
	}).Once()
	repo.On("AdvanceLUB").Return(uint64(0), false).Once()

	// Chain client returns a trivial block
	evmBlock := &types.EVMBlock{}
	cc := chainClientStub{block: evmBlock}

	w := worker.New(cc)
	m, err := New(zap.NewNop().Sugar(), repo, w, 1, 1, 1)
	if err != nil {
		t.Fatalf("New manager error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for backfill processing")
	}
	cancel()
	repo.AssertExpectations(t)
	// no expectations on stub
}

func TestRun_RealtimeEventFlow(t *testing.T) {
	t.Parallel()

	repo := &slidingWindowRepoMock{}
	// No backfill work (processed)
	repo.On("Window").Return(uint64(0), uint64(0))
	repo.On("IsProcessed", uint64(0)).Return(true)

	h := uint64(10)
	// HIB update path
	repo.On("GetHIB").Return(uint64(5)).Once()
	repo.On("SetHIB", h).Return(nil).Once()
	// Not processed/inflight
	repo.On("IsProcessed", h).Return(false)
	// Mark + advance
	done := make(chan struct{})
	repo.On("MarkProcessed", h).Return(nil).Run(func(args mock.Arguments) {
		close(done)
	}).Once()
	repo.On("AdvanceLUB").Return(uint64(0), false).Once()

	// Chain client returns a trivial block
	evmBlock := &types.EVMBlock{}
	cc := chainClientStub{block: evmBlock}

	w := worker.New(cc)
	m, err := New(zap.NewNop().Sugar(), repo, w, 1, 1, 1)
	if err != nil {
		t.Fatalf("New manager error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	// Send realtime header
	m.blockChan <- headerStub{n: new(big.Int).SetUint64(h)}

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for realtime processing")
	}
	cancel()
	repo.AssertExpectations(t)
	// no expectations on stub
}
