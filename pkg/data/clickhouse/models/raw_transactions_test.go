package models

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTransactionFromJSON_Success(t *testing.T) {
	t.Parallel()

	txJSON := createTestTransactionJSON()
	data, err := json.Marshal(txJSON)
	require.NoError(t, err)

	blockNumber := uint64(1647)
	blockHash := "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	blockTime := time.Unix(1604768510, 0).UTC()
	chainID := uint32(43113)
	txIndex := uint64(0)

	txRow, err := ParseTransactionFromJSON(data, blockNumber, blockHash, blockTime, chainID, txIndex)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	// Verify transaction fields
	assert.Equal(t, chainID, txRow.ChainID)
	assert.Equal(t, blockNumber, txRow.BlockNumber)
	assert.Equal(t, blockHash, txRow.BlockHash)
	assert.Equal(t, blockTime, txRow.BlockTime)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273", txRow.Hash)
	assert.Equal(t, "0x4142434445464748494a4b4c4d4e4f5051525354", txRow.From)
	require.NotNil(t, txRow.To)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f6061626364656667", *txRow.To)
	assert.Equal(t, uint64(1), txRow.Nonce)
	assert.Equal(t, "1000000000000000000", txRow.Value)
	assert.Equal(t, uint64(21000), txRow.Gas)
	assert.Equal(t, "470000000000", txRow.GasPrice)
	assert.Equal(t, txIndex, txRow.TransactionIndex)
}

func TestParseTransactionFromJSON_InvalidJSON(t *testing.T) {
	t.Parallel()

	invalidJSON := []byte(`{invalid json}`)
	blockNumber := uint64(1647)
	blockHash := "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	blockTime := time.Unix(1604768510, 0).UTC()
	chainID := uint32(43113)
	txIndex := uint64(0)

	tx, err := ParseTransactionFromJSON(invalidJSON, blockNumber, blockHash, blockTime, chainID, txIndex)

	assert.Nil(t, tx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal transaction JSON")
}

func TestParseTransactionFromJSON_MissingChainID(t *testing.T) {
	t.Parallel()

	txJSON := &coreth.Transaction{
		Hash:  "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
		From:  "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:    "0x55565758595a5b5c5d5e5f6061626364656667",
		Nonce: 1,
		// ChainID is nil
	}

	data, err := json.Marshal(txJSON)
	require.NoError(t, err)

	blockNumber := uint64(1647)
	blockHash := "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	blockTime := time.Unix(1604768510, 0).UTC()
	chainID := uint32(0) // Zero chainID means we need to extract from transaction
	txIndex := uint64(0)

	tx, err := ParseTransactionFromJSON(data, blockNumber, blockHash, blockTime, chainID, txIndex)

	assert.Nil(t, tx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction chainID is required but was not set")
}

func TestParseTransactionFromJSON_ExtractChainIDFromTransaction(t *testing.T) {
	t.Parallel()

	txJSON := &coreth.Transaction{
		Hash:    "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Nonce:   1,
		ChainID: big.NewInt(43113), // ChainID in transaction
	}

	data, err := json.Marshal(txJSON)
	require.NoError(t, err)

	blockNumber := uint64(1647)
	blockHash := "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	blockTime := time.Unix(1604768510, 0).UTC()
	chainID := uint32(0) // Zero means extract from transaction
	txIndex := uint64(0)

	txRow, err := ParseTransactionFromJSON(data, blockNumber, blockHash, blockTime, chainID, txIndex)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	// Should extract chainID from transaction
	assert.Equal(t, uint32(43113), txRow.ChainID)
}

func TestParseTransactionFromJSON_NilValue(t *testing.T) {
	t.Parallel()

	txJSON := &coreth.Transaction{
		Hash:    "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Nonce:   1,
		Value:   nil, // Nil value
		Gas:     21000,
		ChainID: big.NewInt(43113),
	}

	data, err := json.Marshal(txJSON)
	require.NoError(t, err)

	blockNumber := uint64(1647)
	blockHash := "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	blockTime := time.Unix(1604768510, 0).UTC()
	chainID := uint32(43113)
	txIndex := uint64(0)

	txRow, err := ParseTransactionFromJSON(data, blockNumber, blockHash, blockTime, chainID, txIndex)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	// Value should default to "0" when nil
	assert.Equal(t, "0", txRow.Value)
}

func TestParseTransactionFromJSON_NilGasPrice(t *testing.T) {
	t.Parallel()

	txJSON := &coreth.Transaction{
		Hash:     "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
		From:     "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:       "0x55565758595a5b5c5d5e5f6061626364656667",
		Nonce:    1,
		Value:    big.NewInt(1000000000000000000),
		Gas:      21000,
		GasPrice: nil, // Nil gas price
		ChainID:  big.NewInt(43113),
	}

	data, err := json.Marshal(txJSON)
	require.NoError(t, err)

	blockNumber := uint64(1647)
	blockHash := "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	blockTime := time.Unix(1604768510, 0).UTC()
	chainID := uint32(43113)
	txIndex := uint64(0)

	txRow, err := ParseTransactionFromJSON(data, blockNumber, blockHash, blockTime, chainID, txIndex)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	// GasPrice should default to "0" when nil
	assert.Equal(t, "0", txRow.GasPrice)
}
func TestTransactionsFromBlock_Success(t *testing.T) {
	t.Parallel()

	block := &BlockRow{
		ChainID:     43113,
		BlockNumber: 1647,
		Hash:        "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		BlockTime:   time.Unix(1604768510, 0).UTC(),
	}

	transactions := []*coreth.Transaction{
		{
			Hash:    "0x1111111111111111111111111111111111111111111111111111111111111111",
			From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
			To:      "0x55565758595a5b5c5d5e5f6061626364656667",
			Nonce:   1,
			Value:   big.NewInt(1000000000000000000),
			Gas:     21000,
			ChainID: big.NewInt(43113),
		},
		{
			Hash:    "0x2222222222222222222222222222222222222222222222222222222222222222",
			From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
			To:      "0x55565758595a5b5c5d5e5f6061626364656667",
			Nonce:   2,
			Value:   big.NewInt(2000000000000000000),
			Gas:     21000,
			ChainID: big.NewInt(43113),
		},
	}

	txRows, err := TransactionsFromBlock(block, transactions)
	require.NoError(t, err)
	require.Len(t, txRows, 2)

	// Verify first transaction
	assert.Equal(t, uint32(43113), txRows[0].ChainID)
	assert.Equal(t, uint64(1647), txRows[0].BlockNumber)
	assert.Equal(t, "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20", txRows[0].BlockHash)
	assert.Equal(t, "0x1111111111111111111111111111111111111111111111111111111111111111", txRows[0].Hash)
	assert.Equal(t, uint64(0), txRows[0].TransactionIndex)

	// Verify second transaction
	assert.Equal(t, "0x2222222222222222222222222222222222222222222222222222222222222222", txRows[1].Hash)
	assert.Equal(t, uint64(1), txRows[1].TransactionIndex)
}

func TestTransactionsFromBlock_EmptyTransactions(t *testing.T) {
	t.Parallel()

	block := &BlockRow{
		ChainID:     43113,
		BlockNumber: 1647,
		Hash:        "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		BlockTime:   time.Unix(1604768510, 0).UTC(),
	}

	txRows, err := TransactionsFromBlock(block, []*coreth.Transaction{})
	require.NoError(t, err)
	assert.Empty(t, txRows)
}

func TestTransactionsFromBlock_ZeroChainID(t *testing.T) {
	t.Parallel()

	block := &BlockRow{
		ChainID:     0, // Zero chainID
		BlockNumber: 1647,
		Hash:        "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		BlockTime:   time.Unix(1604768510, 0).UTC(),
	}

	transactions := []*coreth.Transaction{
		{
			Hash:    "0x1111111111111111111111111111111111111111111111111111111111111111",
			From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
			ChainID: big.NewInt(43113),
		},
	}

	txRows, err := TransactionsFromBlock(block, transactions)

	assert.Nil(t, txRows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "block chainID is required")
}

// Helper function to create a test transaction JSON
func createTestTransactionJSON() *coreth.Transaction {
	return &coreth.Transaction{
		Hash:     "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
		From:     "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:       "0x55565758595a5b5c5d5e5f6061626364656667",
		Nonce:    1,
		Value:    big.NewInt(1000000000000000000),
		Gas:      21000,
		GasPrice: big.NewInt(470000000000),
		ChainID:  big.NewInt(43113),
	}
}
