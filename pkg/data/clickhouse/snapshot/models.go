package snapshot

type Snapshot struct {
	Lowest    uint64 `json:"lowest"`
	Timestamp int64  `json:"timestamp"`
}
