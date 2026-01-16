package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
)

// TransactionRow represents a transaction row in the database
type TransactionRow struct {
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

// ParseTransactionFromJSON parses a JSON transaction from Kafka and converts it to TransactionRow
func ParseTransactionFromJSON(data []byte, blockNumber uint64, blockHash string, blockTime time.Time, chainID uint32, txIndex uint64) (*TransactionRow, error) {
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

	return corethTransactionToTransactionRow(&tx, blockNumber, blockHash, blockTime, chainID, txIndex)
}

// TransactionsFromBlock extracts all transactions from a block and converts them to TransactionRow
func TransactionsFromBlock(block *BlockRow, transactions []*coreth.Transaction) ([]*TransactionRow, error) {
	if block.ChainID == 0 {
		return nil, errors.New("block chainID is required")
	}

	result := make([]*TransactionRow, len(transactions))
	for i, tx := range transactions {
		txRow, err := corethTransactionToTransactionRow(tx, block.BlockNumber, block.Hash, block.BlockTime, block.ChainID, uint64(i))
		if err != nil {
			return nil, fmt.Errorf("failed to convert transaction %d: %w", i, err)
		}
		result[i] = txRow
	}
	return result, nil
}

// corethTransactionToTransactionRow converts a coreth.Transaction to TransactionRow
func corethTransactionToTransactionRow(tx *coreth.Transaction, blockNumber uint64, blockHash string, blockTime time.Time, chainID uint32, txIndex uint64) (*TransactionRow, error) {
	txRow := &TransactionRow{
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
		txRow.To = &tx.To
	}

	// Convert big.Int values to string
	if tx.Value != nil {
		txRow.Value = tx.Value.String()
	} else {
		txRow.Value = "0"
	}

	if tx.GasPrice != nil {
		gasPriceStr := tx.GasPrice.String()
		txRow.GasPrice = gasPriceStr
	} else {
		txRow.GasPrice = "0"
	}

	// Handle nullable MaxFeePerGas
	if tx.MaxFeePerGas != nil {
		maxFeeStr := tx.MaxFeePerGas.String()
		txRow.MaxFeePerGas = &maxFeeStr
	}

	// Handle nullable MaxPriorityFee
	if tx.MaxPriorityFee != nil {
		maxPriorityStr := tx.MaxPriorityFee.String()
		txRow.MaxPriorityFee = &maxPriorityStr
	}

	return txRow, nil
}
