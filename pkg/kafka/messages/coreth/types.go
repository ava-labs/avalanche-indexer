// Package messages provides types for Kafka messages,
// along with conversion functions from libevm types.
package messages

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common/hexutil"

	corethCustomtypes "github.com/ava-labs/coreth/plugin/evm/customtypes"
	libevmtypes "github.com/ava-labs/libevm/core/types"
)

type CorethBlock struct {
	Number     *big.Int `json:"number"`
	Hash       string   `json:"hash"`
	ParentHash string   `json:"parentHash"`

	ChainID *big.Int `json:"chainId,omitempty"`

	StateRoot        string `json:"stateRoot"`
	TransactionsRoot string `json:"transactionsRoot"`
	ReceiptsRoot     string `json:"receiptsRoot"`
	UncleHash        string `json:"sha3Uncles"`

	Miner string `json:"miner"`

	GasLimit uint64   `json:"gasLimit"`
	GasUsed  uint64   `json:"gasUsed"`
	BaseFee  *big.Int `json:"baseFeePerGas,omitempty"`

	Timestamp      uint64 `json:"timestamp"`
	TimestampMs    uint64 `json:"timestampMs,omitempty"`
	MinDelayExcess uint64 `json:"minDelayExcess,omitempty"`
	Size           uint64 `json:"size"`

	Difficulty *big.Int `json:"difficulty"`
	MixHash    string   `json:"mixHash"`
	Nonce      uint64   `json:"nonce"`

	LogsBloom string `json:"logsBloom"`

	ExtraData string `json:"extraData"`

	ExcessBlobGas *uint64 `json:"excessBlobGas,omitempty"`
	BlobGasUsed   *uint64 `json:"blobGasUsed,omitempty"`

	ParentBeaconBlockRoot string `json:"parentBeaconBlockRoot,omitempty"`

	Withdrawals  []*CorethWithdrawal  `json:"withdrawals"`
	Transactions []*CorethTransaction `json:"transactions"`
}

type CorethTransaction struct {
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

type CorethWithdrawal struct {
	Index          uint64 `json:"index"`
	ValidatorIndex uint64 `json:"validatorIndex"`
	Address        string `json:"address"`
	Amount         uint64 `json:"amount"`
}

// CorethBlockFromLibevm converts a libevm Block to a Coreth Block.
// chainID should be provided since blocks may not have transactions to extract it from.
func CorethBlockFromLibevm(block *libevmtypes.Block, chainID *big.Int) (*CorethBlock, error) {
	transactions, err := CorethTransactionsFromLibevm(block.Transactions())
	if err != nil {
		return nil, fmt.Errorf("convert transactions: %w", err)
	}

	var beaconRoot string
	if block.BeaconRoot() != nil {
		beaconRoot = block.BeaconRoot().Hex()
	}

	var timestampMilliseconds uint64
	var minDelayExcess uint64
	extra := corethCustomtypes.GetHeaderExtra(block.Header())
	if extra.TimeMilliseconds != nil {
		timestampMilliseconds = *extra.TimeMilliseconds
	}
	if extra.MinDelayExcess != nil {
		minDelayExcess = extra.MinDelayExcess.Delay()
	}

	return &CorethBlock{
		Size:                  block.Size(),
		Hash:                  block.Hash().Hex(),
		Number:                block.Number(),
		ChainID:               chainID,
		GasLimit:              block.GasLimit(),
		GasUsed:               block.GasUsed(),
		BaseFee:               block.BaseFee(),
		Difficulty:            block.Difficulty(),
		Timestamp:             block.Time(),
		TimestampMs:           timestampMilliseconds,
		MinDelayExcess:        minDelayExcess,
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
		Withdrawals:           CorethWithdrawalFromLibevm(block.Withdrawals()),
		Transactions:          transactions,
	}, nil
}

// CorethTransactionsFromLibevm converts libevm Transactions to Coreth Transactions.
func CorethTransactionsFromLibevm(transactions []*libevmtypes.Transaction) ([]*CorethTransaction, error) {
	result := make([]*CorethTransaction, len(transactions))

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

		result[i] = &CorethTransaction{
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

// CorethWithdrawalFromLibevm converts libevm Withdrawals to Coreth Withdrawals.
func CorethWithdrawalFromLibevm(withdrawals []*libevmtypes.Withdrawal) []*CorethWithdrawal {
	result := make([]*CorethWithdrawal, len(withdrawals))

	for i, w := range withdrawals {
		result[i] = &CorethWithdrawal{
			Index:          w.Index,
			ValidatorIndex: w.Validator,
			Address:        w.Address.Hex(),
			Amount:         w.Amount,
		}
	}
	return result
}

func (b *CorethBlock) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

func (b *CorethBlock) Unmarshal(data []byte) error {
	return json.Unmarshal(data, b)
}

func (t *CorethTransaction) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

func (t *CorethTransaction) Unmarshal(data []byte) error {
	return json.Unmarshal(data, t)
}

func (w *CorethWithdrawal) Marshal() ([]byte, error) {
	return json.Marshal(w)
}

func (w *CorethWithdrawal) Unmarshal(data []byte) error {
	return json.Unmarshal(data, w)
}
