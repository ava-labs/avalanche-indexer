package evmrepo

import (
	"errors"
	"math/big"
	"time"
)

// Sentinel errors for transaction parsing
var (
	ErrTransactionChainIDRequired = errors.New("transaction blockchain ID is required but was not set")
	ErrBlockChainIDRequiredForTx  = errors.New("block blockchain ID is required")
)

// TransactionRow represents a transaction row in the database
type TransactionRow struct {
	BcID             *big.Int // Blockchain ID
	EvmID            *big.Int // EVM Chain ID (defaults to 0 for now)
	BlockNumber      uint64
	BlockHash        string
	BlockTime        time.Time
	Hash             string
	From             string
	To               *string // Nullable
	Nonce            uint64
	Value            string // Stored as string to handle large values
	Gas              uint64
	GasPrice         string  // Stored as string to handle large values
	MaxFeePerGas     *string // Nullable, stored as string
	MaxPriorityFee   *string // Nullable, stored as string
	Input            string
	Type             uint8
	TransactionIndex uint64
}
