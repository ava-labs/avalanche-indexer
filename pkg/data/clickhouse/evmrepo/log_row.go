package evmrepo

import (
	"math/big"
	"time"
)

// LogRow represents a log row in the database
type LogRow struct {
	BlockchainID *string
	EVMChainID   *big.Int // UInt256 in ClickHouse
	BlockNumber  uint64
	BlockHash    string // Hex string, stored as FixedString(32)
	BlockTime    time.Time
	TxHash       string // Hex string, stored as FixedString(32)
	TxIndex      uint32
	Address      string   // Hex string, stored as FixedString(20)
	Topics       []string // Array of hex strings, stored as Array(FixedString(32))
	Data         []byte   // Binary data, stored as String
	LogIndex     uint32
	Removed      uint8 // 1 for removed, 0 for not removed
}
