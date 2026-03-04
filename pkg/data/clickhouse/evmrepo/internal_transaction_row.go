package evmrepo

import (
	"math/big"

	"github.com/ava-labs/libevm/common"
)

// InternalTransactionRow represents a internal transaction row in the database
type InternalTransactionRow struct {
	BlockchainID    *string
	EVMChainID      *big.Int // UInt256 in ClickHouse
	BlockNumber     uint64
	BlockTimestamp  uint64 // DateTime64(3) in ClickHouse
	TransactionHash string
	Type            string
	From            common.Address
	To              common.Address
	Value           string
	Gas             string
	GasUsed         string
	Revert          bool
	Error           string
	RevertReason    string
	Input           string
	Output          string
	CallIndex       string
}
