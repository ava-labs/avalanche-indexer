package slidingwindow

import (
	"errors"
	"sync"
)

var (
	ErrInvalidInitialWatermarks = errors.New("invalid initial watermarks: HIB < LUB")
	ErrInvalidWatermark         = errors.New(
		"invalid watermark update: new watermark violates invariants",
	)
	ErrOutOfWindow = errors.New("block height outside current window")
)

// Cache is a thread-safe in-memory store for the sliding window state (watermarks and processed blocks).
// LUB is the Lowest Unprocessed Block (the lowest block height that has not been processed).
// HIB is the Highest Ingested Block (the highest block height that has been ingested).
// Processed is a set of block heights that have been processed.
type Cache struct {
	mu        sync.Mutex
	lub       uint64
	hib       uint64
	processed map[uint64]struct{}
}

// NewCache creates a new in-memory Cache with the given initial watermarks.
func NewCache(initialLUB, initialHIB uint64) (*Cache, error) {
	if initialHIB < initialLUB {
		return nil, ErrInvalidInitialWatermarks
	}
	return &Cache{
		lub: initialLUB,
		hib: initialHIB,
		// Using a sparse set is memory-friendly for out-of-order processing in wide windows.
		processed: make(map[uint64]struct{}, 1024),
	}, nil
}

// Window returns the current window boundaries (LUB and HIB).
func (r *Cache) Window() (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lub, r.hib
}

// GetLUB returns Lowest Unprocessed Block (LUB).
func (r *Cache) GetLUB() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lub
}

// GetHIB returns Highest Ingested Block (HIB).
func (r *Cache) GetHIB() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hib
}

// SetHIB sets Highest Ingested Block (chain tip watermark).
// New HIB must be greater than or equal to the current LUB.
func (r *Cache) SetHIB(newHIB uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if newHIB < r.lub {
		return ErrInvalidWatermark
	}
	r.hib = newHIB
	return nil
}

// ResetLUB sets Lowest Unprocessed Block explicitly (used for re-ingestion).
// This may move the LUB forward or backward. Additionally, it drops all processed
// marks strictly below the new LUB.
func (r *Cache) ResetLUB(newLUB uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Allow moving LUB backward or forward. When moving forward, ensure it does not exceed LIB.
	if newLUB > r.hib {
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

// HasWork returns true if there is work to be done (LUB <= HIB).
func (r *Cache) HasWork() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lub <= r.hib
}

// MarkProcessed marks a block as processed.
func (r *Cache) MarkProcessed(h uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Heights strictly below LUB are implicitly processed/committed already.
	if h < r.lub {
		return nil
	}
	if h > r.hib {
		return ErrOutOfWindow
	}
	r.processed[h] = struct{}{}
	return nil
}

// IsProcessed returns true if a block is recorded as processed.
// Note: block heights below the current LUB are considered committed and implicitly processed.
func (r *Cache) IsProcessed(h uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h < r.lub {
		return true
	}
	_, ok := r.processed[h]
	return ok
}

// AdvanceLUB slides LUB forward while contiguous block heights starting from current LUB are processed.
// Returns the new LUB and whether it changed. Idepempotent.
func (r *Cache) AdvanceLUB() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	original := r.lub
	for r.lub <= r.hib {
		if _, ok := r.processed[r.lub]; ok {
			delete(r.processed, r.lub)
			r.lub++
			continue
		}
		break
	}
	return r.lub, r.lub != original
}
