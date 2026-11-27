package evmrepo

import (
	"errors"
	"math/big"
	"time"
)

// Sentinel errors for block parsing
var (
	ErrBlockChainIDRequired = errors.New("block blockchain ID is required but was not set")
	ErrEvmChainIDRequired   = errors.New("evmChainID is required but was not set")
)

// BlockRow represents a block row in the database
type BlockRow struct {
	BlockchainID          *string
	EVMChainID            *big.Int // UInt256 in ClickHouse
	BlockNumber           *big.Int // UInt64 in ClickHouse
	Hash                  string
	ParentHash            string
	BlockTime             time.Time
	TimestampMs           uint64
	Miner                 string
	Difficulty            *big.Int // UInt256 in ClickHouse
	TotalDifficulty       *big.Int // UInt256 in ClickHouse
	Size                  uint64
	GasLimit              uint64
	GasUsed               uint64
	BaseFeePerGas         *big.Int // UInt256 in ClickHouse
	BlockGasCost          *big.Int // UInt256 in ClickHouse
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
	NumTxns               uint32

	// Helicon (C-Chain only) header fields; zero on Subnet-EVM and pre-Helicon blocks.
	TargetExponent      uint64
	MinPriceExponent    uint64
	SettledHeight       uint64
	SettledGasUnix      uint64
	SettledGasNumerator uint64
	SettledExcess       uint64

	// ExecutedGasUsed is the gas used by this block's own transactions. Under
	// ACP-194 the header's GasUsed instead reports gas charged across newly
	// settled blocks, so the two diverge on post-Helicon C-Chain blocks.
	ExecutedGasUsed uint64
}
