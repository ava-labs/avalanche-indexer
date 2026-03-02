package worker

import (
	"errors"
	"sync"
)

var (
	RegisterCustomTypesOnce sync.Once
	ErrReceiptCountMismatch = errors.New("receipt count mismatch")
	ErrReceiptFetchFailed   = errors.New("fetch block receipts failed")
	ErrBlockFetchFailed     = errors.New("fetch block failed")
	ErrTracesFetchFailed    = errors.New("fetch block traces failed")
)
