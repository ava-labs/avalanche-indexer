package models

import (
	"errors"
	"time"
)

// Sentinel errors for transaction parsing
var (
	ErrTransactionChainIDRequired = errors.New("transaction chainID is required but was not set")
	ErrBlockChainIDRequiredForTx  = errors.New("block chainID is required")
)

// TransactionRow represents a transaction row in the database
type TransactionRow struct {
	ChainID          uint32
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
