package slidingwindow

import (
	"errors"
	"sync"

	"github.com/ava-labs/avalanche-indexer/internal/metrics"
	"github.com/ava-labs/avalanche-indexer/internal/types"
)

var (
	ErrInvalidInitialWatermarks = errors.New("invalid initial watermarks: HIB < LUB")
	ErrInvalidWatermark         = errors.New("invalid watermark update: new watermark violates invariants")
	ErrOutOfWindow              = errors.New("block height outside current window")
)

var _ types.SlidingWindowRepository = (*Repository)(nil)

type Config struct {
	InitialLUB uint64
	InitialHIB uint64
	Metrics    *metrics.Metrics // optional
}

// Repository is a thread-safe in-memory implementation of types.SlidingWindowRepository.
type Repository struct {
	mu        sync.Mutex
	lub       uint64
	hib       uint64
	processed map[uint64]struct{}
	metrics   *metrics.Metrics
}

// New creates a new in-memory repository with the given config.
func New(cfg Config) (*Repository, error) {
	if cfg.InitialHIB < cfg.InitialLUB {
		return nil, ErrInvalidInitialWatermarks
	}

	r := &Repository{
		lub: cfg.InitialLUB,
		hib: cfg.InitialHIB,
		// Using a sparse set is memory-friendly for out-of-order processing in wide windows.
		processed: make(map[uint64]struct{}, 1024),
		metrics:   cfg.Metrics,
	}

	if r.metrics != nil {
		r.metrics.UpdateWindowMetrics(r.lub, r.hib, len(r.processed))
	}

	return r, nil
}

func (r *Repository) Window() (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lub, r.hib
}

func (r *Repository) GetLUB() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lub
}

func (r *Repository) GetHIB() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hib
}

// SetHIB sets the Highest Ingested Block (chain tip watermark).
// New HIB must be greater than or equal to the current LUB.
func (r *Repository) SetHIB(newHIB uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if newHIB < r.lub {
		if r.metrics != nil {
			r.metrics.IncError(metrics.ErrTypeInvalidWatermark)
		}
		return ErrInvalidWatermark
	}
	r.hib = newHIB

	if r.metrics != nil {
		r.metrics.UpdateWindowMetrics(r.lub, r.hib, len(r.processed))
	}
	return nil
}

// ResetLUB sets the Lowest Unprocessed Block explicitly (used for re-ingestion).
// This may move the LUB forward or backward. Additionally, it drops all processed
// marks strictly below the new LUB.
func (r *Repository) ResetLUB(newLUB uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Allow moving LUB backward or forward. When moving forward, ensure it does not exceed HIB.
	if newLUB > r.hib {
		if r.metrics != nil {
			r.metrics.IncError(metrics.ErrTypeInvalidWatermark)
		}
		return ErrInvalidWatermark
	}
	r.lub = newLUB

	// Drop processed marks strictly below LUB; they are committed and no longer needed.
	for h := range r.processed {
		if h < r.lub {
			delete(r.processed, h)
		}
	}

	if r.metrics != nil {
		r.metrics.UpdateWindowMetrics(r.lub, r.hib, len(r.processed))
	}
	return nil
}

func (r *Repository) HasWork() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lub <= r.hib
}

func (r *Repository) MarkProcessed(h uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Heights strictly below LUB are implicitly processed/committed already.
	if h < r.lub {
		return nil
	}
	if h > r.hib {
		if r.metrics != nil {
			r.metrics.IncError(metrics.ErrTypeOutOfWindow)
		}
		return ErrOutOfWindow
	}
	r.processed[h] = struct{}{}
	return nil
}

// IsProcessed returns true if a block is recorded as processed.
// Note: block heights below the current LUB are considered committed and implicitly processed.
func (r *Repository) IsProcessed(h uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h < r.lub {
		return true
	}
	_, ok := r.processed[h]
	return ok
}

// AdvanceLUB slides LUB forward while contiguous block heights starting from current LUB are processed.
// Returns the new LUB and whether it changed.
func (r *Repository) AdvanceLUB() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	original := r.lub
	blocksCommitted := 0

	for r.lub <= r.hib {
		if _, ok := r.processed[r.lub]; ok {
			delete(r.processed, r.lub)
			r.lub++
			blocksCommitted++
			continue
		}
		break
	}

	changed := r.lub != original
	if changed && r.metrics != nil {
		r.metrics.CommitBlocks(blocksCommitted, r.lub, r.hib, len(r.processed))
	}

	return r.lub, changed
}
