package inmemory

import (
	"errors"
	"sync"

	"github.com/ava-labs/avalanche-indexer/internal/types"
)

var (
	ErrInvalidWatermark = errors.New("invalid watermark update: new watermark violates invariants")
	ErrOutOfWindow      = errors.New("block height outside current window")
)

var _ types.SlidingWindowRepository = (*SlidingWindowRepository)(nil)

// SlidingWindowRepository is a thread-safe in-memory implementation of SlidingWindowRepository.
type SlidingWindowRepository struct {
	mu        sync.Mutex
	lub       uint64
	lib       uint64
	processed map[uint64]struct{}
}

// NewInMemorySlidingWindowRepository creates a new in-memory repository with the given initial watermarks.
func NewInMemorySlidingWindowRepository(initialLUB, initialLIB uint64) *SlidingWindowRepository {
	if initialLIB < initialLUB {
		initialLIB = initialLUB
	}
	return &SlidingWindowRepository{
		lub: initialLUB,
		lib: initialLIB,
		// Using a sparse set is memory-friendly for out-of-order processing in wide windows.
		processed: make(map[uint64]struct{}, 1024),
	}
}

func (r *SlidingWindowRepository) Window() (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lub, r.lib
}

func (r *SlidingWindowRepository) GetLUB() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lub
}

func (r *SlidingWindowRepository) GetLIB() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lib
}

// SetLIB sets the Largest Ingested Block (chain tip watermark).
// New LIB must be greater than or equal to the current LUB.
func (r *SlidingWindowRepository) SetLIB(newLIB uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if newLIB < r.lub {
		return ErrInvalidWatermark
	}
	r.lib = newLIB
	return nil
}

// ResetLUB sets the Lowest Unprocessed Block explicitly (used for re-ingestion).
// This may move the LUB forward or backward. Additionally, it drops all processed
// marks strictly below the new LUB.
func (r *SlidingWindowRepository) ResetLUB(newLUB uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Allow moving LUB backward or forward. When moving forward, ensure it does not exceed LIB.
	if newLUB > r.lib {
		return ErrInvalidWatermark
	}
	r.lub = newLUB

	// Drop processed marks strictly below LUB; they are committed and no longer needed.
	for h := range r.processed {
		if h < r.lub {
			delete(r.processed, h)
		}
	}
	return nil
}

func (r *SlidingWindowRepository) HasWork() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lub <= r.lib
}

func (r *SlidingWindowRepository) MarkProcessed(h uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Heights strictly below LUB are implicitly processed/committed already.
	if h < r.lub {
		return nil
	}
	if h > r.lib {
		return ErrOutOfWindow
	}
	r.processed[h] = struct{}{}
	return nil
}

// IsProcessed returns true if a block is recorded as processed.
// Note: values below the current LUB are considered committed and implicitly processed.
func (r *SlidingWindowRepository) IsProcessed(h uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h < r.lub {
		return true
	}
	_, ok := r.processed[h]
	return ok
}

// AdvanceLUB slides LUB forward while contiguous values starting from current LUB are processed.
// Returns the new LUB and whether it changed.
func (r *SlidingWindowRepository) AdvanceLUB() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	original := r.lub
	for r.lub <= r.lib {
		if _, ok := r.processed[r.lub]; ok {
			delete(r.processed, r.lub)
			r.lub++
			continue
		}
		break
	}
	return r.lub, r.lub != original
}
