package types

import (
	"math/big"

	"github.com/ava-labs/libevm/common"
	libevmtypes "github.com/ava-labs/libevm/core/types"
)

type CorethBlock struct {
	Number        *big.Int                  `json:"number"`
	GasLimit      uint64                    `json:"gasLimit"`
	GasUsed       uint64                    `json:"gasUsed"`
	Difficulty    *big.Int                  `json:"difficulty"`
	Time          uint64                    `json:"time"`
	NumberU64     uint64                    `json:"numberU64"`
	MixDigest     common.Hash               `json:"mixDigest"`
	Nonce         uint64                    `json:"nonce"`
	Bloom         libevmtypes.Bloom         `json:"bloom"`
	Coinbase      common.Address            `json:"coinbase"`
	Root          common.Hash               `json:"root"`
	ParentHash    common.Hash               `json:"parentHash"`
	TxHash        common.Hash               `json:"txHash"`
	ReceiptHash   common.Hash               `json:"receiptHash"`
	UncleHash     common.Hash               `json:"uncleHash"`
	Extra         []byte                    `json:"extra"`
	BaseFee       *big.Int                  `json:"baseFee"`
	BeaconRoot    *common.Hash              `json:"beaconRoot"`
	ExcessBlobGas *uint64                   `json:"excessBlobGas"`
	BlobGasUsed   *uint64                   `json:"blobGasUsed"`
	Withdrawals   []*libevmtypes.Withdrawal `json:"withdrawals"`
	Transactions  []*CorethTransaction      `json:"transactions"`
}

type CorethTransaction struct {
	Hash                 common.Hash     `json:"hash"`
	To                   *common.Address `json:"to"`
	GasLimit             uint64          `json:"gasLimit"`
	Value                *big.Int        `json:"value"`
	Type                 uint8           `json:"type"`
	Nonce                uint64          `json:"nonce"`
	Data                 []byte          `json:"data"`
	MsgGasPrice          *big.Int        `json:"msgGasPrice"`
	MsgFrom              common.Address  `json:"msgFrom"`
	MaxPriorityFeePerGas *big.Int        `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *big.Int        `json:"maxFeePerGas"`
}
