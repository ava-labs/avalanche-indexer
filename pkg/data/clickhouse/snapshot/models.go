package snapshot

type Snapshot struct {
	ChainID   uint64 `json:"chain_id"`
	Lowest    uint64 `json:"lowest_unprocessed_block"`
	Timestamp int64  `json:"timestamp"`
}
