package slidingwindow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow/worker"
)

type Manager struct {
	log     *zap.SugaredLogger
	state   *State
	worker  worker.Worker
	metrics *metrics.Metrics

	// Limits total concurrent workers (both realtime and backfill).
	workerSem *semaphore.Weighted
	// Caps how many of the concurrent workers may be backfill tasks.
	backfillSem *semaphore.Weighted

	// Input for new heights (send-only by callers).
	heightChan chan uint64
	// Wake-up signal to re-run scheduling; buffered (size 1) to coalesce signals.
	workReady chan struct{}

	// Failure threshold for a height; when reached, the manager sends a signal to the failureChan.
	maxFailures int
	failureChan chan uint64
}

// New creates a Manager and returns an error if arguments are invalid.
// Constraints: concurrency>0; 0<backfillPriority<concurrency; heightsChCapacity>0; maxFailures>0.
// The metrics parameter is optional
func NewManager(
	log *zap.SugaredLogger,
	s *State,
	w worker.Worker,
	concurrency, backfillPriority int64,
	heightChanCapacity, maxFailures int,
	m *metrics.Metrics,
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
	if backfillPriority <= 0 || backfillPriority >= concurrency {
		return nil, errors.New(
			"invalid backfill priority: must be greater than 0 and less than concurrency",
		)
	}
	if heightChanCapacity <= 0 {
		return nil, errors.New("invalid new heights channel capacity: must be greater than 0")
	}

	if maxFailures <= 0 {
		return nil, errors.New("invalid max failures: must be greater than 0")
	}

	return &Manager{
		log:         log,
		state:       s,
		worker:      w,
		metrics:     m,
		workerSem:   semaphore.NewWeighted(concurrency),
		backfillSem: semaphore.NewWeighted(backfillPriority),
		heightChan:  make(chan uint64, heightChanCapacity),
		workReady:   make(chan struct{}, 1), // buffered (size 1) to coalesce signals
		maxFailures: maxFailures,
		failureChan: make(chan uint64, 1), // buffered (size 1) to send failure signal
	}, nil
}

// SubmitHeight initially sets the highest block height if the new block height
// is greater than the current highest to make sure backfill can pick
// it up if the height channel is full. It returns true if the height was submitted,
// false if the channel is full.
func (m *Manager) SubmitHeight(h uint64) bool {
	if ok := m.state.SetHighest(h); !ok {
		m.log.Debugw("failed to set highest height", h)
		return false
	}

	// Update window metrics when HIB changes
	if m.metrics != nil {
		lowest, highest, processedCount := m.state.Snapshot()
		m.metrics.UpdateWindowMetrics(lowest, highest, processedCount)
	}

	select {
	case m.heightChan <- h:
		return true
	default:
		return false
	}
}

// Run executes the scheduling loop until shutdown. It performs backfill work if there is capacity,
// and there is working window (lowest <= highest). It also handles realtime heights. The work among the two
// processes is performed concurrently and distributed according to priority threshold (backfillPriority).
//
// It returns when ctx is done or when the failure threshold is exceeded for a block height.
func (m *Manager) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		// Aggressive backfill (non-blocking)
		for {
			// acquire both backfill and worker capacity
			ok := m.tryAcquireBackfill()
			if !ok {
				break // no more backfill capacity
			}
			next, ok := m.state.FindAndSetNextInflight()
			if !ok {
				// No available heights to claim; release capacity and stop scanning
				m.backfillSem.Release(1)
				m.workerSem.Release(1)
				break
			}
			m.log.Debugw("dispatching backfill worker", "height", next)
			go m.process(ctx, next, true)
		}

		// Check for failure threshold
		select {
		case h := <-m.failureChan:
			return fmt.Errorf(
				"max failures exceeded for block %d, failed after %d attempts",
				h,
				m.state.GetFailureCount(h),
			)
		default:
		}

		// Blocking wait for event (backfill, realtime)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case h := <-m.heightChan:
			m.handleNewHeight(ctx, h)
		case <-m.workReady:
			// A worker finished or watermarks changed; loop restarts
		}
	}
}

// handleNewHeight applies admission control that preserves fairness without sacrificing utilization.
// When there is backfill backlog and unused backfill quota, we deliberately yield this opportunity
// so the scheduler loop can assign a backfill task on the next available worker permit. This prevents
// realtime from monopolizing the shared pool under sustained new-block load while still allowing
// realtime to use all remaining capacity once backfill reaches its cap or has no backlog. If worker
// capacity is unavailable for realtime, the height is dropped; backfill will pick it up later.
func (m *Manager) handleNewHeight(ctx context.Context, h uint64) {
	// If there is backfill backlog and backfill capacity available, yield this chance
	// so the scheduler loop
	if _, ok := m.state.FindNextUnclaimedHeight(); ok {
		if m.backfillSem.TryAcquire(1) {
			m.backfillSem.Release(1)
			return
		}
	}

	// Try to acquire a worker slot immediately for low-latency.
	// Do NOT consume backfill priority for realtime work.
	// If no slot available; drop height. Backfill will pick it up via lowest..highest scan.
	if ok := m.tryAcquireWorker(); ok {
		if ok := m.state.TrySetInflight(h); !ok {
			m.workerSem.Release(1)
			m.log.Debugw("height already inflight, skipping realtime dispatch", "height", h)
			return
		}
		m.log.Debugw("dispatching realtime worker", "height", h)
		go m.process(ctx, h, false)
	} else {
		m.log.Debugw("no worker capacity available, dropping realtime height", "height", h)
	}
}

// process is the main worker function for backfill and realtime heights.
// It acquires semaphores, processes the block height, and releases them.
// It also signals the backfill ready channel when the window advances.
// NOTE: this function has a delay before processing the block height that
// is equal to the failure count of the block height in seconds.
// It is used as a timeout between queries for the same block height.
func (m *Manager) process(ctx context.Context, h uint64, isBackfill bool) {
	count := m.state.GetFailureCount(h)
	if count > 0 {
		m.log.Debugw("delaying processing due to previous failures", "height", h, "failures", count, "delay_seconds", count)
	}
	time.Sleep(time.Duration(count) * time.Second)
	start := time.Now()

	workerType := "realtime"
	if isBackfill {
		workerType = "backfill"
	}
	m.log.Debugw("processing block", "height", h, "type", workerType)

	defer func() {
		if isBackfill {
			m.backfillSem.Release(1)
		}
		m.workerSem.Release(1)
		m.state.UnsetInflight(h)
		m.signalWorkReady()
	}()

	err := m.worker.Process(ctx, h)
	if err != nil {
		// Treat context cancellation as a normal shutdown: do not warn or count as failure.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return
		}
		m.log.Debugw("failed processing block height", "height", h, "error", err)
		m.handleFailure(h)
		return
	}

	// Mark processed and attempt to advance lowest
	err = m.state.MarkProcessed(h)
	if err != nil {
		// If shutting down, do not warn or count as failure.
		if ctx.Err() != nil {
			return
		}
		m.log.Warnw("failed to mark processed", "height", h, "error", err)
		m.handleFailure(h)
		return
	}

	// Record block processing duration on success
	duration := time.Since(start)
	m.log.Debugw("block processing completed", "height", h, "type", workerType, "duration_ms", duration.Milliseconds())
	if m.metrics != nil {
		m.metrics.ObserveBlockProcessingDuration(duration.Seconds())
	}

	// Get current lowest before attempting to advance (needed for commit count)
	oldLowest, _ := m.state.Window()

	// Attempt to slide lowest forward; idempotent if not contiguous
	newLowest, advanced := m.state.AdvanceLowest()

	// Update metrics after state change
	if m.metrics != nil {
		_, highest, processedCount := m.state.Snapshot()
		if advanced {
			// Window advanced - record committed blocks
			blocksCommitted := newLowest - oldLowest
			m.log.Debugw("window advanced", "old_lowest", oldLowest, "new_lowest", newLowest, "blocks_committed", blocksCommitted, "highest", highest)
			m.metrics.CommitBlocks(blocksCommitted, newLowest, highest, processedCount)
		} else {
			m.metrics.UpdateWindowMetrics(newLowest, highest, processedCount)
		}
	}

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
