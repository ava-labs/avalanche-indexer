package evmrepo

import (
	"errors"
	"math/big"
	"time"
)

// Sentinel errors for block parsing
var (
	ErrBlockChainIDRequired = errors.New("block blockchain ID is required but was not set")
)

// BlockRow represents a block row in the database
type BlockRow struct {
	BcID                  *big.Int // Blockchain ID
	EvmID                 *big.Int // EVM Chain ID (defaults to 0 for now)
	BlockNumber           uint64
	Hash                  string
	ParentHash            string
	BlockTime             time.Time
	Miner                 string
	Difficulty            uint64
	TotalDifficulty       uint64
	Size                  uint64
	GasLimit              uint64
	GasUsed               uint64
	BaseFeePerGas         uint64
	BlockGasCost          uint64
	StateRoot             string
	TransactionsRoot      string
	ReceiptsRoot          string
	ExtraData             string
	BlockExtraData        string
	ExtDataHash           string
	ExtDataGasUsed        uint32
	MixHash               string
	Nonce                 string
	Sha3Uncles            string
	Uncles                []string
	BlobGasUsed           uint64
	ExcessBlobGas         uint64
	ParentBeaconBlockRoot string
	MinDelayExcess        uint64
}
