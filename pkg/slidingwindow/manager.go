package slidingwindow

import (
	"context"
	"errors"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

type Manager struct {
	log    *zap.SugaredLogger
	state  *State
	worker worker.Worker

	// Limits total concurrent workers (both realtime and backfill).
	workerSem *semaphore.Weighted
	// Caps how many of the concurrent workers may be backfill tasks.
	backfillSem *semaphore.Weighted

	// Input for realtime headers (send-only by callers).
	blockChan chan Header
	// Wake-up signal to re-run scheduling; buffered (size 1) to coalesce signals.
	workReady chan struct{}

	// Failure threshold for a height; when reached, the manager sends a signal to the failureChan.
	maxFailures int
	failureChan chan uint64
}

var ErrMaxFailuresExceeded = errors.New("max failures exceeded for block")

// New creates a Manager and returns an error if arguments are invalid.
// Constraints: concurrency>0; 1<=backfillPriority<=concurrency; blocksChCapacity>0; maxFailures>0.
func NewManager(
	log *zap.SugaredLogger,
	s *State,
	w worker.Worker,
	concurrency, backfillPriority uint64,
	blocksChCapacity, maxFailures int,
) (*Manager, error) {
	if log == nil {
		return nil, errors.New("invalid logger: must not be nil")
	}

	if s == nil {
		return nil, errors.New("invalid state: must not be nil")
	}

	if w == nil {
		return nil, errors.New("invalid worker: must not be nil")
	}

	if concurrency <= 0 {
		return nil, errors.New("invalid concurrency: must be greater than 0")
	}
	if backfillPriority <= 0 || backfillPriority > concurrency {
		return nil, errors.New(
			"invalid backfill priority: must be less than or equal to concurrency",
		)
	}
	if blocksChCapacity <= 0 {
		return nil, errors.New("invalid new blocks capacity: must be greater than 0")
	}

	if maxFailures <= 0 {
		return nil, errors.New("invalid max failures: must be greater than 0")
	}

	return &Manager{
		log:         log,
		state:       s,
		worker:      w,
		workerSem:   semaphore.NewWeighted(int64(concurrency)),
		backfillSem: semaphore.NewWeighted(int64(backfillPriority)),
		blockChan:   make(chan Header, blocksChCapacity),
		workReady:   make(chan struct{}, 1),
		maxFailures: maxFailures,
		failureChan: make(chan uint64, 1),
	}, nil
}

// BlockChan returns the channel to send realtime headers to.
func (m *Manager) BlockChan() chan<- Header { return m.blockChan }

// Run executes the scheduling loop until shutdown. It performs backfill work if there is capacity,
// and there is working window (lowest <= highest). It also handles realtime headers. The work among the two
// processes is performed concurrently and distributed according to priority threshold (backfillPriority).
//
// It returns when ctx is done or when the failure threshold is exceeded for a block.
func (m *Manager) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		// Aggressive backfill fill (non-blocking)
		for {
			next, ok := m.state.FindNextUnclaimedBlock()
			if !ok {
				break
			}
			ok = m.tryAcquireBackfill()
			if !ok {
				break
			}
			m.state.SetInflight(next)
			go m.process(ctx, next, true)
		}

		// Blocking wait for event (backfill, realtime, failure)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case h := <-m.failureChan:
			return fmt.Errorf(
				"block %d failed after %d attempts: %w",
				h,
				m.state.GetFailureCount(h),
				ErrMaxFailuresExceeded,
			)
		case header := <-m.blockChan:
			m.handleRealtimeHeader(ctx, header)
		case <-m.workReady:
			// A worker finished or watermarks changed; loop restarts
		}
	}
}

// handleRealtimeHeader processes new headers as they arrive. If there is a capacity, the worker is
// dispatched. In case there is no capacity, the header is dropped (backfill will pick it up via lowest..highest scan).
func (m *Manager) handleRealtimeHeader(ctx context.Context, header Header) {
	if header == nil {
		return
	}
	h := header.Number().Uint64()

	// Ensure highest covers this height so backfill can pick it up if we drop.
	highest := m.state.GetHighest()
	if h > highest {
		_ = m.state.SetHighest(h)
	}

	// Skip if already handled
	if m.state.IsProcessed(h) || m.state.IsInflight(h) {
		return
	}

	// Realtime event: try to acquire a worker slot immediately for low-latency.
	// Do NOT consume backfill priority for realtime work.
	// if no slot available; drop event. Backfill will pick it up via lowest..highest scan.
	if ok := m.tryAcquireWorker(); ok {
		// Re-check inflight after acquiring to avoid race
		if m.state.IsInflight(h) || m.state.IsProcessed(h) {
			m.workerSem.Release(1)
			return
		}
		m.state.SetInflight(h)
		go m.process(ctx, h, false)
	}
}

// process is the main worker function for backfill and realtime headers.
// It acquires semaphores, processes the block, and releases them.
// It also signals the backfill ready channel when the window advances.
func (m *Manager) process(ctx context.Context, h uint64, isBackfill bool) {
	defer func() {
		if isBackfill {
			m.backfillSem.Release(1)
		}
		m.workerSem.Release(1)
		m.state.UnsetInflight(h)
		m.signalWorkReady()
	}()

	if err := m.worker.Process(ctx, h); err != nil {
		m.log.Warnw("failed processing block", "height", h, "error", err)
		m.handleFailure(h)
		return
	}

	// Mark processed and attempt to advance lowest
	if err := m.state.MarkProcessed(h); err != nil {
		m.log.Warnw("failed to mark processed", "height", h, "error", err)
		m.handleFailure(h)
		return
	}
	// Attempt to slide lowest forward; idempotent if not contiguous
	_, _ = m.state.AdvanceLowest()
	m.state.ResetFailureCount(h)
}

// handleFailure increments the failure count for a height and sends a signal if the threshold is exceeded.
func (m *Manager) handleFailure(h uint64) {
	failCount := m.state.IncrementFailureCount(h)
	if failCount >= m.maxFailures {
		select {
		case m.failureChan <- h:
		default:
		}
	}
}

// tryAcquireBackfill tries to acquire a backfill permit and a worker permit.
// It returns true if both permits are acquired, false otherwise.
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

// tryAcquireWorker tries to acquire only a worker permit (used by realtime path).
func (m *Manager) tryAcquireWorker() bool {
	return m.workerSem.TryAcquire(1)
}

// signalWorkReady sends a signal to the workReady channel.
// It is used to wake up the scheduling loop.
func (m *Manager) signalWorkReady() {
	select {
	case m.workReady <- struct{}{}:
	default:
	}
}
