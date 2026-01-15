package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
)

// ClickhouseTransaction represents a transaction row in the raw_transactions ClickHouse table
type ClickhouseTransaction struct {
	ChainID          uint32
	BlockNumber      uint64
	BlockHash        string
	BlockTime        time.Time
	Hash             string
	From             string
	To               *string // Nullable
	Nonce            uint64
	Value            string // Stored as string to handle large values
	Gas              uint64
	GasPrice         string  // Stored as string to handle large values
	MaxFeePerGas     *string // Nullable, stored as string
	MaxPriorityFee   *string // Nullable, stored as string
	Input            string
	Type             uint8
	TransactionIndex uint64
}

// ParseTransactionFromJSON parses a JSON transaction from Kafka and converts it to ClickhouseTransaction
func ParseTransactionFromJSON(data []byte, blockNumber uint64, blockHash string, blockTime time.Time, chainID uint32, txIndex uint64) (*ClickhouseTransaction, error) {
	// Unmarshal to coreth.Transaction
	var tx coreth.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transaction JSON: %w", err)
	}

	// Extract chainID from transaction if not provided
	if chainID == 0 {
		if tx.ChainID == nil {
			return nil, errors.New("transaction chainID is required but was not set")
		}
		chainID = uint32(tx.ChainID.Uint64())
	}

	return corethTransactionToClickhouseTransaction(&tx, blockNumber, blockHash, blockTime, chainID, txIndex)
}

// TransactionsFromBlock extracts all transactions from a block and converts them to ClickhouseTransaction
func TransactionsFromBlock(block *ClickhouseBlock, transactions []*coreth.Transaction) ([]*ClickhouseTransaction, error) {
	if block.ChainID == 0 {
		return nil, errors.New("block chainID is required")
	}

	result := make([]*ClickhouseTransaction, len(transactions))
	for i, tx := range transactions {
		clickhouseTx, err := corethTransactionToClickhouseTransaction(tx, block.BlockNumber, block.Hash, block.BlockTime, block.ChainID, uint64(i))
		if err != nil {
			return nil, fmt.Errorf("failed to convert transaction %d: %w", i, err)
		}
		result[i] = clickhouseTx
	}
	return result, nil
}

// corethTransactionToClickhouseTransaction converts a coreth.Transaction to ClickhouseTransaction
func corethTransactionToClickhouseTransaction(tx *coreth.Transaction, blockNumber uint64, blockHash string, blockTime time.Time, chainID uint32, txIndex uint64) (*ClickhouseTransaction, error) {
	clickhouseTx := &ClickhouseTransaction{
		ChainID:          chainID,
		BlockNumber:      blockNumber,
		BlockHash:        blockHash,
		BlockTime:        blockTime,
		Hash:             tx.Hash,
		From:             tx.From,
		Nonce:            tx.Nonce,
		Gas:              tx.Gas,
		Input:            tx.Input,
		Type:             tx.Type,
		TransactionIndex: txIndex,
	}

	// Handle nullable To field
	if tx.To != "" {
		clickhouseTx.To = &tx.To
	}

	// Convert big.Int values to string
	if tx.Value != nil {
		clickhouseTx.Value = tx.Value.String()
	} else {
		clickhouseTx.Value = "0"
	}

	if tx.GasPrice != nil {
		gasPriceStr := tx.GasPrice.String()
		clickhouseTx.GasPrice = gasPriceStr
	} else {
		clickhouseTx.GasPrice = "0"
	}

	// Handle nullable MaxFeePerGas
	if tx.MaxFeePerGas != nil {
		maxFeeStr := tx.MaxFeePerGas.String()
		clickhouseTx.MaxFeePerGas = &maxFeeStr
	}

	// Handle nullable MaxPriorityFee
	if tx.MaxPriorityFee != nil {
		maxPriorityStr := tx.MaxPriorityFee.String()
		clickhouseTx.MaxPriorityFee = &maxPriorityStr
	}

	return clickhouseTx, nil
}
