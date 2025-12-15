package slidingwindow

import (
	"math/big"

	"github.com/ava-labs/libevm/common"
)

type Header interface {
	Number() *big.Int
	Hash() common.Hash
	ParentHash() common.Hash
	Time() uint64
	ChainID() uint64
}
