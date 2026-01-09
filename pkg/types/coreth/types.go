// Package coreth provides types for Coreth (C-Chain) blocks and transactions,
// along with conversion functions from libevm types.
package coreth

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common/hexutil"
	libevmtypes "github.com/ava-labs/libevm/core/types"
)

type Block struct {
	Number     *big.Int `json:"number"`
	Hash       string   `json:"hash"`
	ParentHash string   `json:"parentHash"`

	StateRoot        string `json:"stateRoot"`
	TransactionsRoot string `json:"transactionsRoot"`
	ReceiptsRoot     string `json:"receiptsRoot"`
	UncleHash        string `json:"sha3Uncles"`

	Miner string `json:"miner"`

	GasLimit uint64   `json:"gasLimit"`
	GasUsed  uint64   `json:"gasUsed"`
	BaseFee  *big.Int `json:"baseFeePerGas,omitempty"`

	Timestamp uint64 `json:"timestamp"` // hex string to match RPC
	Size      uint64 `json:"size"`

	Difficulty *big.Int `json:"difficulty"`
	MixHash    string   `json:"mixHash"`
	Nonce      uint64   `json:"nonce"`

	LogsBloom string `json:"logsBloom"`

	ExtraData string `json:"extraData"`

	ExcessBlobGas *uint64 `json:"excessBlobGas,omitempty"`
	BlobGasUsed   *uint64 `json:"blobGasUsed,omitempty"`

	ParentBeaconBlockRoot string `json:"parentBeaconBlockRoot,omitempty"`

	Withdrawals []*Withdrawal `json:"withdrawals,omitempty"`

	Transactions []*Transaction `json:"transactions"`
}

type Transaction struct {
	Hash           string   `json:"hash"`
	From           string   `json:"from"`
	To             string   `json:"to"`
	Nonce          uint64   `json:"nonce"`
	Value          *big.Int `json:"value"`
	Gas            uint64   `json:"gas"`
	GasPrice       *big.Int `json:"gasPrice"`
	MaxFeePerGas   *big.Int `json:"maxFeePerGas"`
	MaxPriorityFee *big.Int `json:"maxPriorityFeePerGas"`
	Input          string   `json:"input"`
	Type           uint8    `json:"type"`
	ChainID        *big.Int `json:"chainId"`
}

type Withdrawal struct {
	Index          uint64 `json:"index"`
	ValidatorIndex uint64 `json:"validatorIndex"`
	Address        string `json:"address"`
	Amount         uint64 `json:"amount"`
}

// BlockFromLibevm converts a libevm Block to a Coreth Block.
func BlockFromLibevm(block *libevmtypes.Block) (*Block, error) {
	transactions, err := TransactionsFromLibevm(block.Transactions())
	if err != nil {
		return nil, fmt.Errorf("convert transactions: %w", err)
	}

	var beaconRoot string
	if block.BeaconRoot() != nil {
		beaconRoot = block.BeaconRoot().Hex()
	}

	return &Block{
		Size:                  block.Size(),
		Hash:                  block.Hash().Hex(),
		Number:                block.Number(),
		GasLimit:              block.GasLimit(),
		GasUsed:               block.GasUsed(),
		BaseFee:               block.BaseFee(),
		Difficulty:            block.Difficulty(),
		Timestamp:             block.Time(),
		MixHash:               block.MixDigest().Hex(),
		Nonce:                 block.Nonce(),
		LogsBloom:             hexutil.Encode(block.Bloom().Bytes()),
		Miner:                 block.Coinbase().Hex(),
		StateRoot:             block.Root().Hex(),
		ParentHash:            block.ParentHash().Hex(),
		TransactionsRoot:      block.TxHash().Hex(),
		ReceiptsRoot:          block.ReceiptHash().Hex(),
		UncleHash:             block.UncleHash().Hex(),
		ExtraData:             hexutil.Encode(block.Extra()),
		ParentBeaconBlockRoot: beaconRoot,
		ExcessBlobGas:         block.ExcessBlobGas(),
		BlobGasUsed:           block.BlobGasUsed(),
		Withdrawals:           WithdrawalsFromLibevm(block.Withdrawals()),
		Transactions:          transactions,
	}, nil
}

// TransactionsFromLibevm converts libevm Transactions to Coreth Transactions.
func TransactionsFromLibevm(transactions []*libevmtypes.Transaction) ([]*Transaction, error) {
	result := make([]*Transaction, len(transactions))

	for i, tx := range transactions {
		signer := libevmtypes.LatestSignerForChainID(tx.ChainId())
		from, err := libevmtypes.Sender(signer, tx)
		if err != nil {
			return nil, fmt.Errorf("recover sender for tx %s: %w", tx.Hash().Hex(), err)
		}

		var to string
		if tx.To() != nil {
			to = tx.To().Hex()
		}

		result[i] = &Transaction{
			Hash:           tx.Hash().Hex(),
			From:           from.Hex(),
			To:             to,
			Nonce:          tx.Nonce(),
			Value:          tx.Value(),
			Gas:            tx.Gas(),
			GasPrice:       tx.GasPrice(),
			MaxFeePerGas:   tx.GasFeeCap(),
			MaxPriorityFee: tx.GasTipCap(),
			Input:          hexutil.Encode(tx.Data()),
			Type:           tx.Type(),
			ChainID:        tx.ChainId(),
		}
	}
	return result, nil
}

// WithdrawalsFromLibevm converts libevm Withdrawals to Coreth Withdrawals.
func WithdrawalsFromLibevm(withdrawals []*libevmtypes.Withdrawal) []*Withdrawal {
	result := make([]*Withdrawal, len(withdrawals))

	for i, w := range withdrawals {
		result[i] = &Withdrawal{
			Index:          w.Index,
			ValidatorIndex: w.Validator,
			Address:        w.Address.Hex(),
			Amount:         w.Amount,
		}
	}
	return result
}

func (b *Block) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

func (b *Block) Unmarshal(data []byte) error {
	return json.Unmarshal(data, b)
}

func (t *Transaction) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

func (t *Transaction) Unmarshal(data []byte) error {
	return json.Unmarshal(data, t)
}

func (w *Withdrawal) Marshal() ([]byte, error) {
	return json.Marshal(w)
}

func (w *Withdrawal) Unmarshal(data []byte) error {
	return json.Unmarshal(data, w)
}
