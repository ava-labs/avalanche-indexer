// Package messages provides types for Kafka messages,
// along with conversion functions from libevm types.
package messages

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"

	corethCustomtypes "github.com/ava-labs/coreth/plugin/evm/customtypes"
	libevmtypes "github.com/ava-labs/libevm/core/types"
)

type CorethBlock struct {
	EVMChainID   *big.Int `json:"evmChainId,omitempty"`
	BlockchainID *string  `json:"blockchainId,omitempty"`
	Number       *big.Int `json:"number"`
	Hash         string   `json:"hash"`
	ParentHash   string   `json:"parentHash"`

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
	Hash           string           `json:"hash"`
	From           string           `json:"from"`
	To             string           `json:"to"`
	Nonce          uint64           `json:"nonce"`
	Value          *big.Int         `json:"value"`
	Gas            uint64           `json:"gas"`
	GasPrice       *big.Int         `json:"gasPrice"`
	MaxFeePerGas   *big.Int         `json:"maxFeePerGas"`
	MaxPriorityFee *big.Int         `json:"maxPriorityFeePerGas"`
	Input          string           `json:"input"`
	Type           uint8            `json:"type"`
	Receipt        *CorethTxReceipt `json:"receipt,omitempty"`
}

type CorethWithdrawal struct {
	Index          uint64 `json:"index"`
	ValidatorIndex uint64 `json:"validatorIndex"`
	Address        string `json:"address"`
	Amount         uint64 `json:"amount"`
}

type CorethTxReceipt struct {
	ContractAddress common.Address `json:"contractAddress"`
	Status          uint64         `json:"status"`
	GasUsed         uint64         `json:"gasUsed"`
	Logs            []*CorethLog   `json:"logs"`
}

type CorethLog struct {
	Address     common.Address `json:"address"`
	Topics      []common.Hash  `json:"topics"`
	Data        []byte         `json:"data"`
	BlockNumber uint64         `json:"blockNumber"`
	TxHash      common.Hash    `json:"txHash"`
	TxIndex     uint           `json:"txIndex"`
	BlockHash   common.Hash    `json:"blockHash"`
	Index       uint           `json:"index"`
	Removed     bool           `json:"removed"`
}

// BlockFromLibevm converts a libevm Block to a Coreth Block.
// chainID should be provided since blocks may not have transactions to extract it from.
func CorethBlockFromLibevm(block *libevmtypes.Block, evmChainID *big.Int, blockchainID *string) (*CorethBlock, error) {
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
		EVMChainID:            evmChainID,
		BlockchainID:          blockchainID,
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

func CorethTxReceiptFromLibevm(tx *libevmtypes.Receipt) *CorethTxReceipt {
	return &CorethTxReceipt{
		ContractAddress: tx.ContractAddress,
		Status:          tx.Status,
		GasUsed:         tx.GasUsed,
		Logs:            CorethLogsFromLibevm(tx.Logs),
	}
}

func CorethLogsFromLibevm(logs []*libevmtypes.Log) []*CorethLog {
	logWrappers := make([]*CorethLog, len(logs))

	for i, log := range logs {
		logWrappers[i] = &CorethLog{
			Address:     log.Address,
			Topics:      log.Topics,
			Data:        log.Data,
			BlockNumber: log.BlockNumber,
			TxHash:      log.TxHash,
			TxIndex:     log.TxIndex,
			BlockHash:   log.BlockHash,
			Index:       log.Index,
			Removed:     log.Removed,
		}
	}
	return logWrappers
}

func (b *CorethBlock) Marshal() ([]byte, error) {
	// Convert big.Int fields to strings for JSON
	type BlockAlias CorethBlock
	alias := (*BlockAlias)(b)

	// Create a map and manually convert big.Int to strings
	result := make(map[string]interface{})
	data, err := json.Marshal(alias)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	// Convert big.Int fields to strings
	if b.EVMChainID != nil {
		result["evmChainId"] = b.EVMChainID.String()
	}
	if b.Number != nil {
		result["number"] = b.Number.String()
	}
	if b.BaseFee != nil {
		result["baseFeePerGas"] = b.BaseFee.String()
	}
	if b.Difficulty != nil {
		result["difficulty"] = b.Difficulty.String()
	}

	return json.Marshal(result)
}

func (b *CorethBlock) Unmarshal(data []byte) error {
	// Use a map to handle big.Int fields as strings
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Remove big.Int fields from raw map temporarily and convert them
	var evmChainIDStr string
	if val, ok := raw["evmChainId"]; ok {
		if str, ok := val.(string); ok {
			evmChainIDStr = str
		}
	}
	numberStr, _ := raw["number"].(string)
	baseFeeStr, _ := raw["baseFeePerGas"].(string)
	difficultyStr, _ := raw["difficulty"].(string)

	// Remove these fields so they don't cause unmarshal errors
	delete(raw, "evmChainId")
	delete(raw, "number")
	delete(raw, "baseFeePerGas")
	delete(raw, "difficulty")

	// Unmarshal everything else
	type BlockAlias CorethBlock
	var alias BlockAlias
	aliasData, _ := json.Marshal(raw)
	if err := json.Unmarshal(aliasData, &alias); err != nil {
		return err
	}
	*b = CorethBlock(alias)

	// Handle big.Int fields manually
	if evmChainIDStr != "" {
		val, ok := new(big.Int).SetString(evmChainIDStr, 10)
		if ok {
			b.EVMChainID = val
		}
	}
	if numberStr != "" {
		val, ok := new(big.Int).SetString(numberStr, 10)
		if ok {
			b.Number = val
		}
	}
	if baseFeeStr != "" {
		val, ok := new(big.Int).SetString(baseFeeStr, 10)
		if ok {
			b.BaseFee = val
		}
	}
	if difficultyStr != "" {
		val, ok := new(big.Int).SetString(difficultyStr, 10)
		if ok {
			b.Difficulty = val
		}
	}

	return nil
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
