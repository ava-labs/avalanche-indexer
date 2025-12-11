package types

import (
	"math/big"

	"github.com/ava-labs/libevm/common"
)

type BlockType string

const (
	BlockTypeEVM BlockType = "evm"
)

type Block interface {
	GetType() BlockType
}

type EVMBlock struct {
	BaseFee               *big.Int          `json:"baseFee"`
	GasUsed               uint64            `json:"gasUsed"`
	GasLimit              uint64            `json:"gasLimit"`
	BlockGasCost          *big.Int          `json:"blockGasCost"`
	Hash                  common.Hash       `json:"hash"`
	Time                  uint64            `json:"time"`
	Number                *big.Int          `json:"number"`
	ParentHash            common.Hash       `json:"parentHash"`
	Transactions          []*EVMTransaction `json:"transactions"`
	TimestampMilliseconds uint64            `json:"timestampMilliseconds"`
	MinDelayExcess        uint64            `json:"minDelayExcess"`
}

func (*EVMBlock) GetType() BlockType {
	return BlockTypeEVM
}
