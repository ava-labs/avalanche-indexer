// Package messages provides types for Kafka messages,
// along with conversion functions from libevm types.
package messages

import (
	"fmt"
	"math/big"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"

	corethCustomtypes "github.com/ava-labs/coreth/plugin/evm/customtypes"
	libevmtypes "github.com/ava-labs/libevm/core/types"
	subnetevmCustomtypes "github.com/ava-labs/subnet-evm/plugin/evm/customtypes"
)

// json is a drop-in replacement for encoding/json using jsoniter for 2-3x performance improvement.
// It's 100% compatible with the standard library API.
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// bigIntToString converts a *big.Int to its decimal string representation.
// Returns empty string if the input is nil, ensuring safe JSON marshaling.
func bigIntToString(v *big.Int) string {
	if v == nil {
		return ""
	}
	return v.String()
}

// parseBigIntField parses a string into *big.Int with field name context for error messages.
// Returns nil for empty strings, supporting optional JSON fields.
func parseBigIntField(s, fieldName string) (*big.Int, error) {
	if s == "" {
		return nil, nil
	}
	val, err := parseBigInt(s)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", fieldName, err)
	}
	return val, nil
}

// parseBigInt parses a string into *big.Int, supporting both decimal and scientific notation.
// Handles legacy Kafka messages that may contain scientific notation (e.g., "1e+21").
func parseBigInt(s string) (*big.Int, error) {
	val, ok := new(big.Int).SetString(s, 10)
	if ok {
		return val, nil
	}

	if strings.Contains(s, "e") || strings.Contains(s, "E") {
		f, _, err := big.ParseFloat(s, 10, 256, big.ToNearestEven)
		if err != nil {
			return nil, fmt.Errorf("failed to parse as float: %w", err)
		}
		result, _ := f.Int(nil)
		return result, nil
	}

	return nil, fmt.Errorf("invalid number format: %s", s)
}

type EVMBlock struct {
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

	Withdrawals  []*EVMWithdrawal  `json:"withdrawals"`
	Transactions []*EVMTransaction `json:"transactions"`
}

type EVMTransaction struct {
	Hash           string        `json:"hash"`
	From           string        `json:"from"`
	To             string        `json:"to"`
	Nonce          uint64        `json:"nonce"`
	Value          *big.Int      `json:"value"`
	Gas            uint64        `json:"gas"`
	GasPrice       *big.Int      `json:"gasPrice"`
	MaxFeePerGas   *big.Int      `json:"maxFeePerGas"`
	MaxPriorityFee *big.Int      `json:"maxPriorityFeePerGas"`
	Input          string        `json:"input"`
	Type           uint8         `json:"type"`
	Receipt        *EVMTxReceipt `json:"receipt,omitempty"`
}

type EVMWithdrawal struct {
	Index          uint64 `json:"index"`
	ValidatorIndex uint64 `json:"validatorIndex"`
	Address        string `json:"address"`
	Amount         uint64 `json:"amount"`
}

type EVMTxReceipt struct {
	ContractAddress common.Address `json:"contractAddress"`
	Status          uint64         `json:"status"`
	GasUsed         uint64         `json:"gasUsed"`
	Logs            []*EVMLog      `json:"logs"`
}

type EVMLog struct {
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

// evmBlockJSON is the JSON wire format for EVMBlock.
// Uses string representations for *big.Int fields to ensure decimal encoding
// and prevent scientific notation in marshaled output.
type evmBlockJSON struct {
	EVMChainID   string  `json:"evmChainId,omitempty"`
	BlockchainID *string `json:"blockchainId,omitempty"`
	Number       string  `json:"number"`
	Hash         string  `json:"hash"`
	ParentHash   string  `json:"parentHash"`

	StateRoot        string `json:"stateRoot"`
	TransactionsRoot string `json:"transactionsRoot"`
	ReceiptsRoot     string `json:"receiptsRoot"`
	UncleHash        string `json:"sha3Uncles"`

	Miner string `json:"miner"`

	GasLimit uint64 `json:"gasLimit"`
	GasUsed  uint64 `json:"gasUsed"`
	BaseFee  string `json:"baseFeePerGas,omitempty"`

	Timestamp      uint64 `json:"timestamp"`
	TimestampMs    uint64 `json:"timestampMs,omitempty"`
	MinDelayExcess uint64 `json:"minDelayExcess,omitempty"`
	Size           uint64 `json:"size"`

	Difficulty string `json:"difficulty"`
	MixHash    string `json:"mixHash"`
	Nonce      uint64 `json:"nonce"`

	LogsBloom string `json:"logsBloom"`

	ExtraData string `json:"extraData"`

	ExcessBlobGas *uint64 `json:"excessBlobGas,omitempty"`
	BlobGasUsed   *uint64 `json:"blobGasUsed,omitempty"`

	ParentBeaconBlockRoot string `json:"parentBeaconBlockRoot,omitempty"`

	Withdrawals  []*EVMWithdrawal  `json:"withdrawals"`
	Transactions []*EVMTransaction `json:"transactions"`
}

// evmTransactionJSON is the JSON wire format for EVMTransaction.
// Uses string representations for *big.Int fields (Value, GasPrice, MaxFeePerGas, MaxPriorityFee)
// to ensure consistent decimal encoding and avoid scientific notation.
type evmTransactionJSON struct {
	Hash           string        `json:"hash"`
	From           string        `json:"from"`
	To             string        `json:"to"`
	Nonce          uint64        `json:"nonce"`
	Value          string        `json:"value,omitempty"`
	Gas            uint64        `json:"gas"`
	GasPrice       string        `json:"gasPrice,omitempty"`
	MaxFeePerGas   string        `json:"maxFeePerGas,omitempty"`
	MaxPriorityFee string        `json:"maxPriorityFeePerGas,omitempty"`
	Input          string        `json:"input"`
	Type           uint8         `json:"type"`
	Receipt        *EVMTxReceipt `json:"receipt,omitempty"`
}

// EVMBlockFromLibevmCoreth converts a libevm coreth Block to a EVM Block.
// chainID should be provided since blocks may not have transactions to extract it from.
func EVMBlockFromLibevmCoreth(block *libevmtypes.Block, evmChainID *big.Int, blockchainID *string) (*EVMBlock, error) {
	transactions, err := EVMTransactionFromLibevm(block.Transactions())
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

	return &EVMBlock{
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
		Withdrawals:           EVMWithdrawalFromLibevm(block.Withdrawals()),
		Transactions:          transactions,
	}, nil
}

// EVMBlockFromLibevmSubnetEVM converts a libevm subnet-evm Block to a EVM Block.
// chainID should be provided since blocks may not have transactions to extract it from.
func EVMBlockFromLibevmSubnetEVM(block *libevmtypes.Block, evmChainID *big.Int, blockchainID *string) (*EVMBlock, error) {
	transactions, err := EVMTransactionFromLibevm(block.Transactions())
	if err != nil {
		return nil, fmt.Errorf("convert transactions: %w", err)
	}

	var beaconRoot string
	if block.BeaconRoot() != nil {
		beaconRoot = block.BeaconRoot().Hex()
	}

	var timestampMilliseconds uint64
	var minDelayExcess uint64
	extra := subnetevmCustomtypes.GetHeaderExtra(block.Header())
	if extra.TimeMilliseconds != nil {
		timestampMilliseconds = *extra.TimeMilliseconds
	}
	if extra.MinDelayExcess != nil {
		minDelayExcess = extra.MinDelayExcess.Delay()
	}

	return &EVMBlock{
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
		Withdrawals:           EVMWithdrawalFromLibevm(block.Withdrawals()),
		Transactions:          transactions,
	}, nil
}

// EVMTransactionFromLibevm converts libevm Transactions to EVM Transactions.
func EVMTransactionFromLibevm(transactions []*libevmtypes.Transaction) ([]*EVMTransaction, error) {
	result := make([]*EVMTransaction, len(transactions))

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

		result[i] = &EVMTransaction{
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

// EVMWithdrawalFromLibevm converts libevm Withdrawals to EVM Withdrawals.
func EVMWithdrawalFromLibevm(withdrawals []*libevmtypes.Withdrawal) []*EVMWithdrawal {
	result := make([]*EVMWithdrawal, len(withdrawals))

	for i, w := range withdrawals {
		result[i] = &EVMWithdrawal{
			Index:          w.Index,
			ValidatorIndex: w.Validator,
			Address:        w.Address.Hex(),
			Amount:         w.Amount,
		}
	}
	return result
}

func EVMTxReceiptFromLibevm(tx *libevmtypes.Receipt) *EVMTxReceipt {
	return &EVMTxReceipt{
		ContractAddress: tx.ContractAddress,
		Status:          tx.Status,
		GasUsed:         tx.GasUsed,
		Logs:            EVMLogsFromLibevm(tx.Logs),
	}
}

func EVMLogsFromLibevm(logs []*libevmtypes.Log) []*EVMLog {
	logWrappers := make([]*EVMLog, len(logs))

	for i, log := range logs {
		logWrappers[i] = &EVMLog{
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

// MarshalJSON implements json.Marshaler.
// Converts *big.Int fields to decimal strings to prevent scientific notation in JSON output.
func (b *EVMBlock) MarshalJSON() ([]byte, error) {
	alias := evmBlockJSON{
		BlockchainID:          b.BlockchainID,
		EVMChainID:            bigIntToString(b.EVMChainID),
		Number:                bigIntToString(b.Number),
		Hash:                  b.Hash,
		ParentHash:            b.ParentHash,
		StateRoot:             b.StateRoot,
		TransactionsRoot:      b.TransactionsRoot,
		ReceiptsRoot:          b.ReceiptsRoot,
		UncleHash:             b.UncleHash,
		Miner:                 b.Miner,
		GasLimit:              b.GasLimit,
		GasUsed:               b.GasUsed,
		BaseFee:               bigIntToString(b.BaseFee),
		Timestamp:             b.Timestamp,
		TimestampMs:           b.TimestampMs,
		MinDelayExcess:        b.MinDelayExcess,
		Size:                  b.Size,
		Difficulty:            bigIntToString(b.Difficulty),
		MixHash:               b.MixHash,
		Nonce:                 b.Nonce,
		LogsBloom:             b.LogsBloom,
		ExtraData:             b.ExtraData,
		ExcessBlobGas:         b.ExcessBlobGas,
		BlobGasUsed:           b.BlobGasUsed,
		ParentBeaconBlockRoot: b.ParentBeaconBlockRoot,
		Withdrawals:           b.Withdrawals,
		Transactions:          b.Transactions,
	}

	return json.Marshal(alias)
}

// UnmarshalJSON implements json.Unmarshaler.
// Parses *big.Int fields from strings, supporting both decimal and scientific notation
// for backward compatibility with legacy Kafka messages.
func (b *EVMBlock) UnmarshalJSON(data []byte) error {
	var alias evmBlockJSON
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}

	b.BlockchainID = alias.BlockchainID
	b.Hash = alias.Hash
	b.ParentHash = alias.ParentHash
	b.StateRoot = alias.StateRoot
	b.TransactionsRoot = alias.TransactionsRoot
	b.ReceiptsRoot = alias.ReceiptsRoot
	b.UncleHash = alias.UncleHash
	b.Miner = alias.Miner
	b.GasLimit = alias.GasLimit
	b.GasUsed = alias.GasUsed
	b.Timestamp = alias.Timestamp
	b.TimestampMs = alias.TimestampMs
	b.MinDelayExcess = alias.MinDelayExcess
	b.Size = alias.Size
	b.MixHash = alias.MixHash
	b.Nonce = alias.Nonce
	b.LogsBloom = alias.LogsBloom
	b.ExtraData = alias.ExtraData
	b.ExcessBlobGas = alias.ExcessBlobGas
	b.BlobGasUsed = alias.BlobGasUsed
	b.ParentBeaconBlockRoot = alias.ParentBeaconBlockRoot
	b.Withdrawals = alias.Withdrawals
	b.Transactions = alias.Transactions

	var err error
	if b.EVMChainID, err = parseBigIntField(alias.EVMChainID, "evmChainId"); err != nil {
		return err
	}
	if b.Number, err = parseBigIntField(alias.Number, "number"); err != nil {
		return err
	}
	if b.BaseFee, err = parseBigIntField(alias.BaseFee, "baseFeePerGas"); err != nil {
		return err
	}
	if b.Difficulty, err = parseBigIntField(alias.Difficulty, "difficulty"); err != nil {
		return err
	}

	return nil
}

// MarshalJSON implements json.Marshaler.
// Converts *big.Int fields to decimal strings to prevent scientific notation in JSON output.
func (t *EVMTransaction) MarshalJSON() ([]byte, error) {
	alias := evmTransactionJSON{
		Hash:           t.Hash,
		From:           t.From,
		To:             t.To,
		Nonce:          t.Nonce,
		Value:          bigIntToString(t.Value),
		Gas:            t.Gas,
		GasPrice:       bigIntToString(t.GasPrice),
		MaxFeePerGas:   bigIntToString(t.MaxFeePerGas),
		MaxPriorityFee: bigIntToString(t.MaxPriorityFee),
		Input:          t.Input,
		Type:           t.Type,
		Receipt:        t.Receipt,
	}

	return json.Marshal(alias)
}

// UnmarshalJSON implements json.Unmarshaler.
// Parses *big.Int fields from strings, supporting both decimal and scientific notation
// for backward compatibility with legacy Kafka messages.
func (t *EVMTransaction) UnmarshalJSON(data []byte) error {
	var alias evmTransactionJSON
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}

	t.Hash = alias.Hash
	t.From = alias.From
	t.To = alias.To
	t.Nonce = alias.Nonce
	t.Gas = alias.Gas
	t.Input = alias.Input
	t.Type = alias.Type
	t.Receipt = alias.Receipt

	var err error
	if t.Value, err = parseBigIntField(alias.Value, "value"); err != nil {
		return err
	}
	if t.GasPrice, err = parseBigIntField(alias.GasPrice, "gasPrice"); err != nil {
		return err
	}
	if t.MaxFeePerGas, err = parseBigIntField(alias.MaxFeePerGas, "maxFeePerGas"); err != nil {
		return err
	}
	if t.MaxPriorityFee, err = parseBigIntField(alias.MaxPriorityFee, "maxPriorityFeePerGas"); err != nil {
		return err
	}

	return nil
}
