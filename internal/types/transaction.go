package types

import (
	"math/big"

	"github.com/ava-labs/libevm/common"
)

type EVMTransaction struct {
	Hash     common.Hash     `json:"hash"`
	To       *common.Address `json:"to"`
	GasLimit uint64          `json:"gasLimit"`
	Value    *big.Int        `json:"value"`
	Type     uint8           `json:"type"`
	Nonce    uint64          `json:"nonce"`
	Data     []byte          `json:"data"`
}
