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

const testBlockHash = "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

func TestParseBlockFromJSON_Success(t *testing.T) {
	t.Parallel()

	// Create a test block JSON
	blockJSON := createTestBlockJSON()
	data, err := json.Marshal(blockJSON)
	require.NoError(t, err)

	// Parse the block
	blockRow, transactions, err := ParseBlockFromJSON(data)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	// Verify block fields
	assert.Equal(t, uint32(43113), blockRow.ChainID)
	assert.Equal(t, uint64(1647), blockRow.BlockNumber)
	assert.Equal(t, testBlockHash, blockRow.Hash)
	assert.Equal(t, "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40", blockRow.ParentHash)
	assert.Equal(t, time.Unix(1604768510, 0).UTC(), blockRow.BlockTime)
	assert.Equal(t, uint64(1331), blockRow.Size)
	assert.Equal(t, uint64(20006296), blockRow.GasLimit)
	assert.Equal(t, uint64(183061), blockRow.GasUsed)
	assert.Equal(t, uint64(470000000000), blockRow.BaseFeePerGas)

	// Verify transactions are returned
	assert.Len(t, transactions, 1)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273", transactions[0].Hash)
}

func TestParseBlockFromJSON_InvalidJSON(t *testing.T) {
	t.Parallel()

	invalidJSON := []byte(`{invalid json}`)
	block, transactions, err := ParseBlockFromJSON(invalidJSON)

	assert.Nil(t, block)
	assert.Nil(t, transactions)
	var jsonErr *json.SyntaxError
	require.ErrorAs(t, err, &jsonErr)
	assert.Contains(t, err.Error(), "failed to unmarshal block JSON")
}

func TestParseBlockFromJSON_MissingChainID(t *testing.T) {
	t.Parallel()

	blockJSON := &coreth.Block{
		Number: big.NewInt(1647),
		Hash:   testBlockHash,
		// ChainID is nil
		Transactions: []*coreth.Transaction{},
	}

	data, err := json.Marshal(blockJSON)
	require.NoError(t, err)

	block, transactions, err := ParseBlockFromJSON(data)

	assert.Nil(t, block)
	assert.Nil(t, transactions)
	require.ErrorIs(t, err, ErrBlockChainIDRequired)
}

func TestParseBlockFromJSON_NoTransactions(t *testing.T) {
	t.Parallel()

	blockJSON := &coreth.Block{
		ChainID:          big.NewInt(43113),
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
		Transactions:     []*coreth.Transaction{}, // Empty transactions
	}

	data, err := json.Marshal(blockJSON)
	require.NoError(t, err)

	blockRow, transactions, err := ParseBlockFromJSON(data)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	assert.Equal(t, uint32(43113), blockRow.ChainID)
	assert.Equal(t, uint64(1647), blockRow.BlockNumber)
	assert.Empty(t, transactions)
}

func TestParseBlockFromJSON_WithMultipleTransactions(t *testing.T) {
	t.Parallel()

	blockJSON := createTestBlockJSON()
	// Add a second transaction
	blockJSON.Transactions = append(blockJSON.Transactions, &coreth.Transaction{
		Hash:    "0x9999999999999999999999999999999999999999999999999999999999999999",
		From:    "0x1111111111111111111111111111111111111111",
		To:      "0x2222222222222222222222222222222222222222",
		Nonce:   1,
		Value:   big.NewInt(2000000000000000000),
		Gas:     21000,
		ChainID: big.NewInt(43113),
	})

	data, err := json.Marshal(blockJSON)
	require.NoError(t, err)

	blockRow, transactions, err := ParseBlockFromJSON(data)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	assert.Len(t, transactions, 2)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273", transactions[0].Hash)
	assert.Equal(t, "0x9999999999999999999999999999999999999999999999999999999999999999", transactions[1].Hash)
}

func TestParseBlockFromJSON_OptionalFields(t *testing.T) {
	t.Parallel()

	blockJSON := &coreth.Block{
		ChainID:               big.NewInt(43113),
		Number:                big.NewInt(1647),
		Hash:                  testBlockHash,
		ParentHash:            "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		Timestamp:             1604768510,
		Size:                  1331,
		GasLimit:              20006296,
		GasUsed:               183061,
		Difficulty:            big.NewInt(1),
		Nonce:                 42,
		Miner:                 "0x4142434445464748494a4b4c4d4e4f5051525354",
		StateRoot:             testBlockHash,
		TransactionsRoot:      testBlockHash,
		ReceiptsRoot:          testBlockHash,
		UncleHash:             testBlockHash,
		MixHash:               testBlockHash,
		ExtraData:             "0xd883010916846765746888676f312e31332e38856c696e7578236a756571a22fb6b759507d25baa07790e2dcb952924471d436785469db4655",
		BaseFee:               big.NewInt(470000000000),
		BlobGasUsed:           uintPtr(1000),
		ExcessBlobGas:         uintPtr(2000),
		ParentBeaconBlockRoot: "0xbeaconroot1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
		MinDelayExcess:        5000,
		Transactions:          []*coreth.Transaction{},
	}

	data, err := json.Marshal(blockJSON)
	require.NoError(t, err)

	blockRow, _, err := ParseBlockFromJSON(data)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	// Verify optional fields
	assert.Equal(t, uint64(470000000000), blockRow.BaseFeePerGas)
	assert.Equal(t, uint64(1000), blockRow.BlobGasUsed)
	assert.Equal(t, uint64(2000), blockRow.ExcessBlobGas)
	assert.Equal(t, "0xbeaconroot1234567890abcdef1234567890abcdef1234567890abcdef1234567890", blockRow.ParentBeaconBlockRoot)
	assert.Equal(t, uint64(5000), blockRow.MinDelayExcess)
}

func TestParseBlockFromJSON_NilNumber(t *testing.T) {
	t.Parallel()

	blockJSON := &coreth.Block{
		ChainID:          big.NewInt(43113),
		Number:           nil, // nil number
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
		Transactions:     []*coreth.Transaction{},
	}

	data, err := json.Marshal(blockJSON)
	require.NoError(t, err)

	blockRow, _, err := ParseBlockFromJSON(data)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	// BlockNumber should be 0 when Number is nil
	assert.Equal(t, uint64(0), blockRow.BlockNumber)
}

// Helper function to create a test block JSON
func createTestBlockJSON() *coreth.Block {
	return &coreth.Block{
		ChainID:          big.NewInt(43113),
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
		Transactions: []*coreth.Transaction{
			{
				Hash:     "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
				From:     "0x4142434445464748494a4b4c4d4e4f5051525354",
				To:       "0x55565758595a5b5c5d5e5f6061626364656667",
				Nonce:    1,
				Value:    big.NewInt(1000000000000000000),
				Gas:      21000,
				GasPrice: big.NewInt(470000000000),
				ChainID:  big.NewInt(43113),
			},
		},
	}
}

// Helper function to create a uint64 pointer
func uintPtr(u uint64) *uint64 {
	return &u
}
