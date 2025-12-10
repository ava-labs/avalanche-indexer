package types

// Repository defines the operations for:
// - Managing watermarks: LUB (Lowest Unprocessed Block) and LIB (Largest Ingested Block)
// - Tracking processed blocks to enable sliding the window efficiently
//
// Concurrency-safety:
// All implementations must be safe for concurrent use by multiple goroutines.
type SlidingWindowRepository interface {
	// Window returns the current [LUB, LIB] watermarks.
	Window() (lub uint64, lib uint64)

	// GetLUB returns the Lowest Unprocessed Block.
	GetLUB() uint64

	// GetLIB returns the Largest Ingested Block.
	GetLIB() uint64

	// SetLIB sets the Largest Ingested Block (chain tip watermark).
	// Must not set LIB below LUB.
	SetLIB(newLIB uint64) error

	// ResetLUB sets the Lowest Unprocessed Block explicitly (used for re-ingestion).
	// This may move the LUB forward or backward.
	ResetLUB(newLUB uint64) error

	// HasWork returns true if LUB <= LIB.
	HasWork() bool

	// MarkProcessed marks a block as processed (data durably written to the final repository).
	// This does not mutate LUB by itself; call AdvanceLUB to slide the window.
	MarkProcessed(h uint64) error

	// IsProcessed returns true if a block is recorded as processed.
	// Note: uint64s below the current LUB are considered committed and implicitly processed.
	IsProcessed(h uint64) bool

	// AdvanceLUB slides LUB forward while contiguous uint64s starting from current LUB are processed.
	// Returns the new LUB and whether it changed.
	AdvanceLUB() (newLUB uint64, changed bool)
}
