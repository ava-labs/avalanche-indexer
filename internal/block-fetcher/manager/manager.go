package manager

import (
	"context"
	"errors"
	"sync"

	"github.com/ava-labs/avalanche-indexer/internal/block-fetcher/worker"
	"github.com/ava-labs/avalanche-indexer/internal/types"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

type Manager struct {
	sugar         *zap.SugaredLogger
	swRepo        types.SlidingWindowRepository
	workerSem     *semaphore.Weighted
	backfillSem   *semaphore.Weighted
	mu            sync.Mutex
	worker        worker.Worker
	blockChan     chan types.Header
	backfillReady chan struct{}
	inflight      map[uint64]struct{}
}

var (
	ErrInvalidLogger                  = errors.New("invalid logger: must not be nil")
	ErrInvalidConcurrency             = errors.New("invalid concurrency: must be greater than 0")
	ErrInvalidBackfillPriority        = errors.New("invalid backfill priority: must be less than or equal to concurrency")
	ErrInvalidNewBlocksCapacity       = errors.New("invalid new blocks capacity: must be greater than 0")
	ErrInvalidWorker                  = errors.New("invalid worker: must not be nil")
	ErrInvalidSlidingWindowRepository = errors.New("invalid sliding window repository: must not be nil")
)

func New(sugar *zap.SugaredLogger, r types.SlidingWindowRepository, w worker.Worker, concurrency, backfillPriority uint64, blocksChCapacity int) (*Manager, error) {
	if sugar == nil {
		return nil, ErrInvalidLogger
	}

	if r == nil {
		return nil, ErrInvalidSlidingWindowRepository
	}

	if concurrency <= 0 {
		return nil, ErrInvalidConcurrency
	}
	if backfillPriority <= 0 || backfillPriority > concurrency {
		return nil, ErrInvalidBackfillPriority
	}
	if blocksChCapacity <= 0 {
		return nil, ErrInvalidNewBlocksCapacity
	}

	if w == nil {
		return nil, ErrInvalidWorker
	}

	return &Manager{
		sugar:         sugar,
		swRepo:        r,
		worker:        w,
		workerSem:     semaphore.NewWeighted(int64(concurrency)),
		backfillSem:   semaphore.NewWeighted(int64(backfillPriority)),
		blockChan:     make(chan types.Header, blocksChCapacity),
		backfillReady: make(chan struct{}, 1),
		inflight:      make(map[uint64]struct{}),
	}, nil
}

// BlockChan exposes a send-only view of the realtime queue for other services to populate the block channel.
func (m *Manager) BlockChan() chan<- types.Header { return m.blockChan }

func (m *Manager) Run(ctx context.Context) {
	for {
		// A. Aggressive backfill fill (non-blocking)
		for {
			next, ok := m.findNextUnclaimedBlock()
			if !ok {
				break
			}
			// Try to acquire both a global worker slot and a backfill slot
			ok = m.tryAcquireBackfill()
			if !ok {
				break
			}
			m.setInflight(next, true)
			go m.process(ctx, next, true)
		}

		// B. Blocking wait for event
		select {
		case <-ctx.Done():
			return
		case header := <-m.blockChan:
			if header == nil {
				continue
			}
			h := header.Number().Uint64()
			// Ensure HIB covers this height (best-effort) so backfill can pick it up if we drop.
			hib := m.swRepo.GetHIB()
			if h > hib {
				_ = m.swRepo.SetHIB(h)
			}

			// Skip if already handled
			if m.swRepo.IsProcessed(h) || m.isInflight(h) {
				continue
			}

			// Realtime event: try to acquire immediately for low-latency
			if ok := m.tryAcquireBackfill(); ok {
				// Re-check inflight after acquiring to avoid race
				if m.isInflight(h) || m.swRepo.IsProcessed(h) {
					m.workerSem.Release(1)
					continue
				}
				m.setInflight(h, true)
				go m.process(ctx, h, false)
			} else {
				// No slot available; drop event. Backfill will pick it up via LUB..HIB scan.
			}
		case <-m.backfillReady:
			// A worker finished or watermarks changed; loop restarts and fills backfill again.
		}
	}
}

func (m *Manager) process(ctx context.Context, h uint64, isBackfill bool) {
	defer func() {
		if isBackfill {
			m.backfillSem.Release(1)
		}
		m.workerSem.Release(1)
		m.setInflight(h, false)
		m.signalBackfillReady()
	}()

	if err := m.worker.Process(ctx, h); err != nil {
		// Transient failure; leave unprocessed to retry later.
		m.sugar.Warnw("failed processing block", "height", h, "error", err)
		return
	}

	// Mark processed and attempt to advance LUB
	if err := m.swRepo.MarkProcessed(h); err != nil {
		m.sugar.Warnw("failed to mark processed", "height", h, "error", err)
		return
	}
	// Attempt to slide LUB forward; idempotent if not contiguous
	if _, changed := m.swRepo.AdvanceLUB(); changed {
		m.signalBackfillReady()
	}
}

func (m *Manager) isInflight(h uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.inflight[h]
	return ok
}

func (m *Manager) setInflight(h uint64, v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v {
		m.inflight[h] = struct{}{}
	} else {
		delete(m.inflight, h)
	}
}

func (m *Manager) findNextUnclaimedBlock() (uint64, bool) {
	lub, hib := m.swRepo.Window()
	for h := lub; h <= hib; h++ {
		if m.swRepo.IsProcessed(h) {
			continue
		}
		if m.isInflight(h) {
			continue
		}
		return h, true
	}
	return 0, false
}

func (m *Manager) tryAcquireBackfill() bool {
	acquired := m.backfillSem.TryAcquire(1)
	if !acquired {
		return false
	}

	acquired = m.workerSem.TryAcquire(1)
	if !acquired {
		m.backfillSem.Release(1)
		return false
	}

	return true
}

func (m *Manager) signalBackfillReady() {
	select {
	case m.backfillReady <- struct{}{}:
	default:
	}
}
