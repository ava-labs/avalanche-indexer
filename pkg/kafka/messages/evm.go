// Package messages provides types for Kafka messages,
// along with conversion functions from libevm types.
package messages

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"

	corethCustomtypes "github.com/ava-labs/coreth/plugin/evm/customtypes"
	libevmtypes "github.com/ava-labs/libevm/core/types"
	subnetevmCustomtypes "github.com/ava-labs/subnet-evm/plugin/evm/customtypes"
	jsonIterator "github.com/json-iterator/go"
)

// jsonIter is a drop-in replacement for encoding/json using jsoniter for 2-3x performance improvement.
// It's 100% compatible with the standard library API.
var jsonIter = jsonIterator.ConfigCompatibleWithStandardLibrary

// bigIntToRawJSON converts a *big.Int to encodingjson.RawMessage as a quoted decimal string.
// Returns empty quoted string for nil values.
func bigIntToRawJSON(v *big.Int) json.RawMessage {
	if v == nil {
		return json.RawMessage(`""`)
	}
	return json.RawMessage(`"` + v.String() + `"`)
}

// parseBigIntFromRaw parses encodingjson.RawMessage into *big.Int, accepting both JSON strings and numbers.
// Handles unquoted numbers (e.g., 1e+21, 1000000000000000000) and quoted strings (e.g., "1e+21", "1000000000000000000").
// Returns nil for empty/null RawMessage.
func parseBigIntFromRaw(raw json.RawMessage, fieldName string) (*big.Int, error) {
	if len(raw) == 0 || string(raw) == "null" || string(raw) == `""` {
		return nil, nil
	}

	// Try as string first (e.g., "1000000000000000000" or "1e+21")
	if raw[0] == '"' {
		var s string
		if err := jsonIter.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("failed to parse %s as string: %w", fieldName, err)
		}
		if s == "" {
			return nil, nil
		}
		return parseBigIntFromString(s, fieldName)
	}

	// Try as JSON number (e.g., 1000000000000000000 or 1e+21)
	var f float64
	if err := jsonIter.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("failed to parse %s as number: %w", fieldName, err)
	}

	// Convert float to big.Int using big.Float for precision
	bf := big.NewFloat(f)
	bf.SetPrec(256) // High precision
	result, _ := bf.Int(nil)
	return result, nil
}

// parseBigIntFromString parses a string into *big.Int, supporting both decimal and scientific notation.
func parseBigIntFromString(s, fieldName string) (*big.Int, error) {
	// Try direct parsing first (handles decimal strings)
	val, ok := new(big.Int).SetString(s, 10)
	if ok {
		return val, nil
	}

	// Handle scientific notation (e.g., "1e+21", "1.5e18")
	if strings.Contains(s, "e") || strings.Contains(s, "E") {
		f, _, err := big.ParseFloat(s, 10, 256, big.ToNearestEven)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s as float: %w", fieldName, err)
		}
		result, _ := f.Int(nil)
		return result, nil
	}

	return nil, fmt.Errorf("invalid number format for %s: %s", fieldName, s)
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
// Uses encodingjson.RawMessage for *big.Int fields to accept both JSON strings and numbers (including
// scientific notation like 1e+21) during unmarshaling, while always marshaling to decimal strings.
type evmBlockJSON struct {
	EVMChainID   json.RawMessage `json:"evmChainId,omitempty"`
	BlockchainID *string         `json:"blockchainId,omitempty"`
	Number       json.RawMessage `json:"number"`
	Hash         string          `json:"hash"`
	ParentHash   string          `json:"parentHash"`

	StateRoot        string `json:"stateRoot"`
	TransactionsRoot string `json:"transactionsRoot"`
	ReceiptsRoot     string `json:"receiptsRoot"`
	UncleHash        string `json:"sha3Uncles"`

	Miner string `json:"miner"`

	GasLimit uint64          `json:"gasLimit"`
	GasUsed  uint64          `json:"gasUsed"`
	BaseFee  json.RawMessage `json:"baseFeePerGas,omitempty"`

	Timestamp      uint64 `json:"timestamp"`
	TimestampMs    uint64 `json:"timestampMs,omitempty"`
	MinDelayExcess uint64 `json:"minDelayExcess,omitempty"`
	Size           uint64 `json:"size"`

	Difficulty json.RawMessage `json:"difficulty"`
	MixHash    string          `json:"mixHash"`
	Nonce      uint64          `json:"nonce"`

	LogsBloom string `json:"logsBloom"`

	ExtraData string `json:"extraData"`

	ExcessBlobGas *uint64 `json:"excessBlobGas,omitempty"`
	BlobGasUsed   *uint64 `json:"blobGasUsed,omitempty"`

	ParentBeaconBlockRoot string `json:"parentBeaconBlockRoot,omitempty"`

	Withdrawals  []*EVMWithdrawal  `json:"withdrawals"`
	Transactions []*EVMTransaction `json:"transactions"`
}

// evmTransactionJSON is the JSON wire format for EVMTransaction.
// Uses encodingjson.RawMessage for *big.Int fields (Value, GasPrice, MaxFeePerGas, MaxPriorityFee) to accept
// both JSON strings and numbers (including scientific notation) during unmarshaling, while always
// marshaling to decimal strings.
type evmTransactionJSON struct {
	Hash           string          `json:"hash"`
	From           string          `json:"from"`
	To             string          `json:"to"`
	Nonce          uint64          `json:"nonce"`
	Value          json.RawMessage `json:"value,omitempty"`
	Gas            uint64          `json:"gas"`
	GasPrice       json.RawMessage `json:"gasPrice,omitempty"`
	MaxFeePerGas   json.RawMessage `json:"maxFeePerGas,omitempty"`
	MaxPriorityFee json.RawMessage `json:"maxPriorityFeePerGas,omitempty"`
	Input          string          `json:"input"`
	Type           uint8           `json:"type"`
	Receipt        *EVMTxReceipt   `json:"receipt,omitempty"`
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
		EVMChainID:            bigIntToRawJSON(b.EVMChainID),
		Number:                bigIntToRawJSON(b.Number),
		Hash:                  b.Hash,
		ParentHash:            b.ParentHash,
		StateRoot:             b.StateRoot,
		TransactionsRoot:      b.TransactionsRoot,
		ReceiptsRoot:          b.ReceiptsRoot,
		UncleHash:             b.UncleHash,
		Miner:                 b.Miner,
		GasLimit:              b.GasLimit,
		GasUsed:               b.GasUsed,
		BaseFee:               bigIntToRawJSON(b.BaseFee),
		Timestamp:             b.Timestamp,
		TimestampMs:           b.TimestampMs,
		MinDelayExcess:        b.MinDelayExcess,
		Size:                  b.Size,
		Difficulty:            bigIntToRawJSON(b.Difficulty),
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

	return jsonIter.Marshal(alias)
}

// UnmarshalJSON implements json.Unmarshaler.
// Parses *big.Int fields from strings, supporting both decimal and scientific notation
// for backward compatibility with legacy Kafka messages.
func (b *EVMBlock) UnmarshalJSON(data []byte) error {
	var alias evmBlockJSON
	if err := jsonIter.Unmarshal(data, &alias); err != nil {
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
	if b.EVMChainID, err = parseBigIntFromRaw(alias.EVMChainID, "evmChainId"); err != nil {
		return err
	}
	if b.Number, err = parseBigIntFromRaw(alias.Number, "number"); err != nil {
		return err
	}
	if b.BaseFee, err = parseBigIntFromRaw(alias.BaseFee, "baseFeePerGas"); err != nil {
		return err
	}
	if b.Difficulty, err = parseBigIntFromRaw(alias.Difficulty, "difficulty"); err != nil {
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
		Value:          bigIntToRawJSON(t.Value),
		Gas:            t.Gas,
		GasPrice:       bigIntToRawJSON(t.GasPrice),
		MaxFeePerGas:   bigIntToRawJSON(t.MaxFeePerGas),
		MaxPriorityFee: bigIntToRawJSON(t.MaxPriorityFee),
		Input:          t.Input,
		Type:           t.Type,
		Receipt:        t.Receipt,
	}

	return jsonIter.Marshal(alias)
}

// UnmarshalJSON implements json.Unmarshaler.
// Parses *big.Int fields from strings, supporting both decimal and scientific notation
// for backward compatibility with legacy Kafka messages.
func (t *EVMTransaction) UnmarshalJSON(data []byte) error {
	var alias evmTransactionJSON
	if err := jsonIter.Unmarshal(data, &alias); err != nil {
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
	if t.Value, err = parseBigIntFromRaw(alias.Value, "value"); err != nil {
		return err
	}
	if t.GasPrice, err = parseBigIntFromRaw(alias.GasPrice, "gasPrice"); err != nil {
		return err
	}
	if t.MaxFeePerGas, err = parseBigIntFromRaw(alias.MaxFeePerGas, "maxFeePerGas"); err != nil {
		return err
	}
	if t.MaxPriorityFee, err = parseBigIntFromRaw(alias.MaxPriorityFee, "maxPriorityFeePerGas"); err != nil {
		return err
	}

	return nil
}
