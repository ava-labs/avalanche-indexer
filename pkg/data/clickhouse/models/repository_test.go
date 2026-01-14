package models

import (
	"errors"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRepository_WriteBlock_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Create a test block
	block := createTestBlock()

	// Expect Exec with query and all block fields
	// Match specific values where we can, use mock.Anything for complex types
	hashStr := string(block.Hash[:])
	parentHashStr := string(block.ParentHash[:])
	minerStr := string(block.Miner[:])
	stateRootStr := string(block.StateRoot[:])
	transactionsRootStr := string(block.StateRoot[:]) // Same as StateRoot in test
	receiptsRootStr := string(block.StateRoot[:])     // Same as StateRoot in test
	mixHashStr := string(block.MixHash[:])
	sha3UnclesStr := string(block.Sha3Uncles[:])
	nonceStr := string(block.Nonce[:])

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			// Verify the query contains INSERT INTO and the table name
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "default.raw_blocks")
		}),
			block.ChainID,         // uint32: 43113
			block.BlockNumber,     // uint32: 1647
			hashStr,               // string
			parentHashStr,         // string
			block.BlockTime,       // time.Time
			minerStr,              // string
			block.Difficulty,      // uint8: 1
			block.TotalDifficulty, // uint64: 1000
			block.Size,            // uint32: 1331
			block.GasLimit,        // uint32: 20006296
			block.GasUsed,         // uint32: 183061
			block.BaseFeePerGas,   // uint64: 470000000000
			block.BlockGasCost,    // uint64: 0
			stateRootStr,          // string
			transactionsRootStr,   // string
			receiptsRootStr,       // string
			block.ExtraData,       // string
			mixHashStr,            // string
			nonceStr,              // string (nonce converted to string)
			sha3UnclesStr,         // string
		).
		Return(nil)

	repo := NewRepository(testutils.NewTestClient(mockConn), "default.raw_blocks")
	err := repo.WriteBlock(ctx, block)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_WriteBlock_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	block := createTestBlock()
	execErr := errors.New("exec failed")

	// Match specific values like the success test
	hashStr := string(block.Hash[:])
	parentHashStr := string(block.ParentHash[:])
	minerStr := string(block.Miner[:])
	stateRootStr := string(block.StateRoot[:])
	transactionsRootStr := string(block.StateRoot[:])
	receiptsRootStr := string(block.StateRoot[:])
	mixHashStr := string(block.MixHash[:])
	sha3UnclesStr := string(block.Sha3Uncles[:])
	nonceStr := string(block.Nonce[:])

	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			block.ChainID,
			block.BlockNumber,
			hashStr,
			parentHashStr,
			block.BlockTime,
			minerStr,
			block.Difficulty,
			block.TotalDifficulty,
			block.Size,
			block.GasLimit,
			block.GasUsed,
			block.BaseFeePerGas,
			block.BlockGasCost,
			stateRootStr,
			transactionsRootStr,
			receiptsRootStr,
			block.ExtraData,
			mixHashStr,
			nonceStr,
			sha3UnclesStr,
		).
		Return(execErr)

	repo := NewRepository(testutils.NewTestClient(mockConn), "default.raw_blocks")
	err := repo.WriteBlock(ctx, block)
	require.ErrorIs(t, err, execErr)
	assert.Contains(t, err.Error(), "failed to write block")
	assert.Contains(t, err.Error(), "exec failed")
	mockConn.AssertExpectations(t)
}

// Helper function to create a test block with all fields populated
func createTestBlock() *RawBlock {
	// Use helper functions to convert hex strings to byte arrays for readability
	hash, _ := utils.HexToBytes32("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	parentHash, _ := utils.HexToBytes32("0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40")
	miner, _ := utils.HexToBytes20("0x4142434445464748494a4b4c4d4e4f5051525354")
	nonce, _ := utils.HexToBytes8("0x55565758595a5b5c")

	return &RawBlock{
		ChainID:               43113,
		BlockNumber:           1647,
		Hash:                  hash,
		ParentHash:            parentHash,
		BlockTime:             time.Unix(1604768510, 0).UTC(),
		Miner:                 miner,
		Difficulty:            1,
		TotalDifficulty:       1000,
		Size:                  1331,
		GasLimit:              20006296,
		GasUsed:               183061,
		BaseFeePerGas:         470000000000,
		BlockGasCost:          0,
		StateRoot:             hash,
		TransactionsRoot:      hash,
		ReceiptsRoot:          hash,
		ExtraData:             "0xd883010916846765746888676f312e31332e38856c696e7578236a756571a22fb6b759507d25baa07790e2dcb952924471d436785469db4655",
		BlockExtraData:        "",
		ExtDataHash:           [32]byte{},
		ExtDataGasUsed:        0,
		MixHash:               [32]byte{},
		Nonce:                 nonce,
		Sha3Uncles:            [32]byte{},
		Uncles:                nil,
		BlobGasUsed:           0,
		ExcessBlobGas:         0,
		ParentBeaconBlockRoot: [32]byte{},
		MinDelayExcess:        0,
	}
}

// Helper function to check if a string contains a substring (case-insensitive)
func containsSubstring(s, substr string) bool {
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
