package main

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Helper to compare *big.Int values (handles nil cases)
func assertBigIntEqual(t *testing.T, expected, actual *big.Int) {
	t.Helper()
	if expected == nil && actual == nil {
		return
	}
	if expected == nil {
		require.Nil(t, actual, "Expected nil, got %v", actual)
		return
	}
	if actual == nil {
		require.NotNil(t, actual, "Expected %v, got nil", expected)
		return
	}
	assert.Equal(t, 0, expected.Cmp(actual), "Expected %s, got %s", expected.String(), actual.String())
}

const testBlockHash = "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

func TestCorethBlockToBlockRow_Success(t *testing.T) {
	t.Parallel()

	block := createTestBlock()

	blockRow, err := processor.CorethBlockToBlockRow(block)
	require.NoError(t, err)

	require.NotNil(t, blockRow)
	// BlockchainID is a string
	if block.BlockchainID != nil {
		require.NotNil(t, blockRow.BlockchainID)
		assert.Equal(t, *block.BlockchainID, *blockRow.BlockchainID)
	} else {
		assert.Nil(t, blockRow.BlockchainID)
	}
	// EVMChainID defaults to 0 if nil in the source block
	if block.EVMChainID == nil {
		assertBigIntEqual(t, big.NewInt(0), blockRow.EVMChainID)
	} else {
		assertBigIntEqual(t, block.EVMChainID, blockRow.EVMChainID)
	}
	assertBigIntEqual(t, big.NewInt(1647), blockRow.BlockNumber)
	assert.Equal(t, testBlockHash, blockRow.Hash)
	assert.Equal(t, "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40", blockRow.ParentHash)
	assert.Equal(t, time.Unix(1604768510, 0).UTC(), blockRow.BlockTime)
	assert.Equal(t, uint64(1331), blockRow.Size)
	assert.Equal(t, uint64(20006296), blockRow.GasLimit)
	assert.Equal(t, uint64(183061), blockRow.GasUsed)
	assertBigIntEqual(t, big.NewInt(470000000000), blockRow.BaseFeePerGas)
	assert.Equal(t, "2a", blockRow.Nonce) // 42 in hex
}

func TestCorethBlockToBlockRow_NilNumber(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.Number = nil

	blockRow, err := processor.CorethBlockToBlockRow(block)
	require.NoError(t, err)

	require.NotNil(t, blockRow)
	assertBigIntEqual(t, big.NewInt(0), blockRow.BlockNumber)
}

func TestCorethBlockToBlockRow_OptionalFields(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.BaseFee = big.NewInt(470000000000)
	block.BlobGasUsed = uintPtr(1000)
	block.ExcessBlobGas = uintPtr(2000)
	block.ParentBeaconBlockRoot = "0xbeaconroot1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	block.MinDelayExcess = 5000

	blockRow, err := processor.CorethBlockToBlockRow(block)
	require.NoError(t, err)

	require.NotNil(t, blockRow)
	assertBigIntEqual(t, big.NewInt(470000000000), blockRow.BaseFeePerGas)
	assert.Equal(t, uint64(1000), blockRow.BlobGasUsed)
	assert.Equal(t, uint64(2000), blockRow.ExcessBlobGas)
	assert.Equal(t, "0xbeaconroot1234567890abcdef1234567890abcdef1234567890abcdef1234567890", blockRow.ParentBeaconBlockRoot)
	assert.Equal(t, uint64(5000), blockRow.MinDelayExcess)
}

func TestCorethTransactionToTransactionRow_Success(t *testing.T) {
	t.Parallel()

	tx := createTestTransaction()
	block := createTestBlock()
	txIndex := uint64(0)

	txRow, err := processor.CorethTransactionToTransactionRow(tx, block, txIndex)

	require.NoError(t, err)
	require.NotNil(t, txRow)
	// BlockchainID is a string, comes from block
	if block.BlockchainID != nil {
		require.NotNil(t, txRow.BlockchainID)
		assert.Equal(t, *block.BlockchainID, *txRow.BlockchainID)
	} else {
		assert.Nil(t, txRow.BlockchainID)
	}
	// EVMChainID defaults to 0 if nil in the source block
	if block.EVMChainID == nil {
		assertBigIntEqual(t, big.NewInt(0), txRow.EVMChainID)
	} else {
		assertBigIntEqual(t, block.EVMChainID, txRow.EVMChainID)
	}
	assert.Equal(t, uint64(1647), txRow.BlockNumber)
	assert.Equal(t, testBlockHash, txRow.BlockHash)
	assert.Equal(t, time.Unix(1604768510, 0).UTC(), txRow.BlockTime)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273", txRow.Hash)
	assert.Equal(t, "0x4142434445464748494a4b4c4d4e4f5051525354", txRow.From)
	require.NotNil(t, txRow.To)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f6061626364656667", *txRow.To)
	assert.Equal(t, uint64(1), txRow.Nonce)
	assertBigIntEqual(t, big.NewInt(1000000000000000000), txRow.Value)
	assert.Equal(t, uint64(21000), txRow.Gas)
	assertBigIntEqual(t, big.NewInt(470000000000), txRow.GasPrice)
	assert.Equal(t, txIndex, txRow.TransactionIndex)
}

func TestCorethTransactionToTransactionRow_NilTo(t *testing.T) {
	t.Parallel()

	tx := createTestTransaction()
	tx.To = "" // Empty To (contract creation)
	block := createTestBlock()
	txIndex := uint64(0)

	txRow, err := processor.CorethTransactionToTransactionRow(tx, block, txIndex)

	require.NoError(t, err)
	require.NotNil(t, txRow)
	assert.Nil(t, txRow.To)
}

func TestCorethTransactionToTransactionRow_NilValue(t *testing.T) {
	t.Parallel()

	tx := createTestTransaction()
	tx.Value = nil
	block := createTestBlock()
	txIndex := uint64(0)

	txRow, err := processor.CorethTransactionToTransactionRow(tx, block, txIndex)

	require.NoError(t, err)
	require.NotNil(t, txRow)
	assertBigIntEqual(t, big.NewInt(0), txRow.Value)
}

func TestCorethTransactionToTransactionRow_NilGasPrice(t *testing.T) {
	t.Parallel()

	tx := createTestTransaction()
	tx.GasPrice = nil
	block := createTestBlock()
	txIndex := uint64(0)

	txRow, err := processor.CorethTransactionToTransactionRow(tx, block, txIndex)

	require.NoError(t, err)
	require.NotNil(t, txRow)
	assertBigIntEqual(t, big.NewInt(0), txRow.GasPrice)
}

func TestCorethTransactionToTransactionRow_MaxFeeFields(t *testing.T) {
	t.Parallel()

	tx := createTestTransaction()
	tx.MaxFeePerGas = big.NewInt(1000000000)
	tx.MaxPriorityFee = big.NewInt(2000000000)
	block := createTestBlock()
	txIndex := uint64(0)

	txRow, err := processor.CorethTransactionToTransactionRow(tx, block, txIndex)

	require.NoError(t, err)
	require.NotNil(t, txRow)
	require.NotNil(t, txRow.MaxFeePerGas)
	require.NotNil(t, txRow.MaxPriorityFee)
	assertBigIntEqual(t, big.NewInt(1000000000), txRow.MaxFeePerGas)
	assertBigIntEqual(t, big.NewInt(2000000000), txRow.MaxPriorityFee)
}

func TestCorethTransactionToTransactionRow_NilBlockchainID(t *testing.T) {
	t.Parallel()

	tx := createTestTransaction()
	block := createTestBlock()
	block.BlockchainID = nil // Nil blockchain ID
	txIndex := uint64(0)

	txRow, err := processor.CorethTransactionToTransactionRow(tx, block, txIndex)

	assert.Nil(t, txRow)
	require.ErrorIs(t, err, evmrepo.ErrBlockChainIDRequiredForTx)
}

func TestCorethTransactionToTransactionRow_NilNumber(t *testing.T) {
	t.Parallel()

	tx := createTestTransaction()
	block := createTestBlock()
	block.Number = nil // Nil number
	txIndex := uint64(0)

	txRow, err := processor.CorethTransactionToTransactionRow(tx, block, txIndex)

	require.NoError(t, err)
	require.NotNil(t, txRow)
	assert.Equal(t, uint64(0), txRow.BlockNumber)
}

func TestProcessBlockMessage_InvalidJSON(t *testing.T) {
	t.Parallel()

	invalidJSON := []byte(`{invalid json}`)
	sugar := zap.NewNop().Sugar()
	proc := processor.NewCorethProcessor(sugar, nil, nil, nil, nil)

	msg := &cKafka.Message{
		Value: invalidJSON,
	}

	err := proc.Process(t.Context(), msg)

	var jsonErr *json.SyntaxError
	require.ErrorAs(t, err, &jsonErr)
	assert.Contains(t, err.Error(), "failed to unmarshal coreth block")
}

func TestProcessBlockMessage_MissingChainID(t *testing.T) {
	t.Parallel()

	blockJSON := &kafkamsg.CorethBlock{
		Number: big.NewInt(1647),
		Hash:   testBlockHash,
		// BlockchainID is nil
		Transactions: []*kafkamsg.CorethTransaction{},
	}

	data, err := json.Marshal(blockJSON)
	require.NoError(t, err)

	sugar := zap.NewNop().Sugar()
	proc := processor.NewCorethProcessor(sugar, nil, nil, nil, nil)

	msg := &cKafka.Message{
		Value: data,
	}

	err = proc.Process(t.Context(), msg)

	require.ErrorIs(t, err, evmrepo.ErrBlockChainIDRequired)
}

// Helper function to create a test block
func createTestBlock() *kafkamsg.CorethBlock {
	blockchainID := "11111111111111111111111111111111LpoYY"
	return &kafkamsg.CorethBlock{
		BlockchainID:     &blockchainID,
		EVMChainID:       big.NewInt(43113),
		Number:           big.NewInt(1647),
		Hash:             testBlockHash,
		ParentHash:       "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		Timestamp:        1604768510,
		Size:             1331,
		GasLimit:         20006296,
		GasUsed:          183061,
		Difficulty:       big.NewInt(1),
		Nonce:            42,
		Miner:            "0x4142434445464748494a4b4c4d4e4f5051525354",
		StateRoot:        testBlockHash,
		TransactionsRoot: testBlockHash,
		ReceiptsRoot:     testBlockHash,
		UncleHash:        testBlockHash,
		MixHash:          testBlockHash,
		ExtraData:        "0xd883010916846765746888676f312e31332e38856c696e7578236a756571a22fb6b759507d25baa07790e2dcb952924471d436785469db4655",
		BaseFee:          big.NewInt(470000000000),
		Transactions: []*kafkamsg.CorethTransaction{
			{
				Hash:     "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
				From:     "0x4142434445464748494a4b4c4d4e4f5051525354",
				To:       "0x55565758595a5b5c5d5e5f6061626364656667",
				Nonce:    1,
				Value:    big.NewInt(1000000000000000000),
				Gas:      21000,
				GasPrice: big.NewInt(470000000000),
			},
		},
	}
}

// Helper function to create a test transaction
func createTestTransaction() *kafkamsg.CorethTransaction {
	return &kafkamsg.CorethTransaction{
		Hash:     "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
		From:     "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:       "0x55565758595a5b5c5d5e5f6061626364656667",
		Nonce:    1,
		Value:    big.NewInt(1000000000000000000),
		Gas:      21000,
		GasPrice: big.NewInt(470000000000),
	}
}

// Helper function to create a uint64 pointer
func uintPtr(u uint64) *uint64 {
	return &u
}
