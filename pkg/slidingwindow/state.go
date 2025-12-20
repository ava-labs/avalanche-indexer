package slidingwindow

import (
	"fmt"
	"sync"
)

// State is a thread-safe in-memory store for the sliding window state (watermarks and processed/inflight blocks).
type State struct {
	mu        sync.Mutex          // TODO check if I need this many mutexes
	lowest    uint64              // lowest unprocessed block watermark.
	highest   uint64              // highest unprocessed block watermark.
	processed map[uint64]struct{} // set of processed block heights.

	// Heights currently being processed to avoid duplicate work.
	inflight   map[uint64]struct{}
	inflightMu sync.Mutex

	// Per-height failure counters; trip threshold shuts down Run.
	failCounts map[uint64]int
	failMu     sync.Mutex
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
		lowest:     initialLowest,
		highest:    initialHighest,
		processed:  make(map[uint64]struct{}),
		inflight:   make(map[uint64]struct{}),
		failCounts: make(map[uint64]int),
	}, nil
}

// GetLowest returns lowest unprocessed block.
func (s *State) GetLowest() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lowest
}

// GetHighest returns highest unprocessed block.
func (s *State) GetHighest() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.highest
}

// SetHighest sets highest unprocessed block.
// New highest must be greater than or equal to the current lowest.
func (s *State) SetHighest(newHighest uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newHighest < s.lowest {
		return fmt.Errorf(
			"invalid watermark update: new watermark violates invariants: new highest < lowest: %d < %d",
			newHighest,
			s.lowest,
		)
	}
	s.highest = newHighest
	return nil
}

// ResetLowest sets lowest unprocessed block explicitly (used for re-ingestion).
// This may move the lowest forward or backward. Additionally, it drops all processed
// marks strictly below the new lowest.
func (s *State) ResetLowest(newLowest uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Allow moving lowest backward or forward. When moving forward, ensure it does not exceed highest.
	if newLowest > s.highest {
		return fmt.Errorf(
			"invalid watermark update: new watermark violates invariants: new lowest > highest: %d > %d",
			newLowest,
			s.highest,
		)
	}
	s.lowest = newLowest

	// Drop processed marks strictly below lowest; they are committed and no longer needed.
	for h := range s.processed {
		if h < newLowest {
			delete(s.processed, h)
		}
	}
	return nil
}

// MarkProcessed marks a block as processed.
func (s *State) MarkProcessed(h uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Heights strictly below lowest are implicitly processed/committed already.
	if h < s.lowest {
		return nil
	}
	if h > s.highest {
		return fmt.Errorf(
			"invalid block height: block height is greater than highest: %d > %d",
			h,
			s.highest,
		)
	}
	s.processed[h] = struct{}{}
	return nil
}

// IsProcessed returns true if a block is recorded as processed.
// Note: block heights below the current lowest are considered committed and implicitly processed.
func (s *State) IsProcessed(h uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h < s.lowest {
		return true
	}
	_, ok := s.processed[h]
	return ok
}

// AdvanceLowest slides lowest forward while contiguous block heights starting from current lowest are processed.
// Returns the new lowest and whether it changed. Idepempotent.
func (s *State) AdvanceLowest() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	original := s.lowest
	for s.lowest <= s.highest {
		if _, ok := s.processed[s.lowest]; ok {
			delete(s.processed, s.lowest)
			s.lowest++
			continue
		}
		break
	}
	return s.lowest, s.lowest != original
}

// GetFailureCount returns the current failure count for a height.
func (s *State) GetFailureCount(h uint64) int {
	s.failMu.Lock()
	defer s.failMu.Unlock()
	return s.failCounts[h]
}

// IncrementFailureCount increments the failure count for a height.
// Returns the new failure count.
func (s *State) IncrementFailureCount(h uint64) int {
	s.failMu.Lock()
	defer s.failMu.Unlock()
	s.failCounts[h]++
	return s.failCounts[h]
}

// ResetFailureCount resets the failure count for a height.
func (s *State) ResetFailureCount(h uint64) {
	s.failMu.Lock()
	defer s.failMu.Unlock()
	delete(s.failCounts, h)
}

// IsInflight returns true if a height is being processed.
func (s *State) IsInflight(h uint64) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	_, ok := s.inflight[h]
	return ok
}

// SetInflight marks a height as being processed.
func (s *State) SetInflight(h uint64) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	s.inflight[h] = struct{}{}
}

// UnsetInflight removes a height from the inflight set.
func (s *State) UnsetInflight(h uint64) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	delete(s.inflight, h)
}

// FindNextUnclaimedBlock finds the next height in the [lowset..highest] window that is not processed and not inflight.
func (s *State) FindNextUnclaimedBlock() (uint64, bool) {
	for h := s.lowest; h <= s.highest; h++ {
		if s.IsProcessed(h) {
			continue
		}
		if s.IsInflight(h) {
			continue
		}
		return h, true
	}
	return 0, false
}
