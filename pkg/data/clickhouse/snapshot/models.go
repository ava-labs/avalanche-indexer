package snapshot

// Snapshot is a struct that represents a snapshot of the sliding window state.
// It is used for starting or recovering ingestion from a specific block height
// (lowest unprocessed block). The snapshot is chain specific. Timestamp is used
// to track the last time the snapshot was written.
type Snapshot struct {
	ChainID   uint64 `json:"chain_id"`
	Lowest    uint64 `json:"lowest_unprocessed_block"`
	Timestamp int64  `json:"timestamp"`
}
