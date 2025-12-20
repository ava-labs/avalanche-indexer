package slidingwindow

import (
	"fmt"
	"sync"
)

// State is a thread-safe in-memory store for the sliding window state (watermarks and processed/inflight blocks).
type State struct {
	mu        sync.Mutex
	lowest    uint64              // lowest unprocessed block.
	highest   uint64              // highest unprocessed block.
	processed map[uint64]struct{} // set of processed block heights.
}

// NewState creates a new in-memory State with the given initial watermarks.
func NewState(initialLowest, initialHighest uint64) (*State, error) {
	if initialHighest < initialLowest {
		return nil, fmt.Errorf(
			"invalid initial watermarks: highest < lowest: %d < %d",
			initialHighest,
			initialLowest,
		)
	}
	return &State{
		lowest:    initialLowest,
		highest:   initialHighest,
		processed: make(map[uint64]struct{}),
	}, nil
}

// Window returns the current window boundaries (low and high).
func (r *State) Window() (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lowest, r.highest
}

// GetLowest returns lowest unprocessed block.
func (r *State) GetLowest() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lowest
}

// GetHighest returns highest unprocessed block.
func (r *State) GetHighest() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.highest
}

// SetHighest sets highest unprocessed block.
// New highest must be greater than or equal to the current lowest.
func (r *State) SetHighest(newHighest uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if newHighest < r.lowest {
		return fmt.Errorf(
			"invalid watermark update: new watermark violates invariants: new highest < lowest: %d < %d",
			newHighest,
			r.lowest,
		)
	}
	r.highest = newHighest
	return nil
}

// ResetLowest sets lowest unprocessed block explicitly (used for re-ingestion).
// This may move the lowest forward or backward. Additionally, it drops all processed
// marks strictly below the new lowest.
func (r *State) ResetLowest(newLowest uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Allow moving lowest backward or forward. When moving forward, ensure it does not exceed highest.
	if newLowest > r.highest {
		return fmt.Errorf(
			"invalid watermark update: new watermark violates invariants: new lowest > highest: %d > %d",
			newLowest,
			r.highest,
		)
	}
	r.lowest = newLowest

	// Drop processed marks strictly below lowest; they are committed and no longer needed.
	for h := range r.processed {
		if h < newLowest {
			delete(r.processed, h)
		}
	}
	return nil
}

// HasWork returns true if there is work to be done (lowest <= highest).
func (r *State) HasWork() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lowest <= r.highest
}

// MarkProcessed marks a block as processed.
func (r *State) MarkProcessed(h uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Heights strictly below lowest are implicitly processed/committed already.
	if h < r.lowest {
		return nil
	}
	if h > r.highest {
		return fmt.Errorf(
			"invalid block height: block height is greater than highest: %d > %d",
			h,
			r.highest,
		)
	}
	r.processed[h] = struct{}{}
	return nil
}

// IsProcessed returns true if a block is recorded as processed.
// Note: block heights below the current lowest are considered committed and implicitly processed.
func (r *State) IsProcessed(h uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h < r.lowest {
		return true
	}
	_, ok := r.processed[h]
	return ok
}

// AdvanceLowest slides lowest forward while contiguous block heights starting from current lowest are processed.
// Returns the new lowest and whether it changed. Idepempotent.
func (r *State) AdvanceLowest() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	original := r.lowest
	for r.lowest <= r.highest {
		if _, ok := r.processed[r.lowest]; ok {
			delete(r.processed, r.lowest)
			r.lowest++
			continue
		}
		break
	}
	return r.lowest, r.lowest != original
}
