package evmrepo

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
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
	// For nullable nonce - convert empty string to nil
	var nonceStr interface{}
	if block.Nonce == "" {
		nonceStr = nil
	} else {
		nonceStr = block.Nonce
	}

	// For nullable parent_beacon_block_root - convert empty string to nil
	var parentBeaconBlockRootStr interface{}
	if block.ParentBeaconBlockRoot == "" {
		parentBeaconBlockRootStr = nil
	} else {
		parentBeaconBlockRootStr = block.ParentBeaconBlockRoot
	}

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_blocks")
		})).
		Return(nil).
		Once()

	// Expect WriteBlock call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			// Verify the query contains INSERT INTO and the table name
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "default.raw_blocks")
		}),
			block.BcID,               // uint32: 43113
			block.EvmID,              // uint32: 0
			block.BlockNumber,        // uint64: 1647
			block.Hash,               // string
			block.ParentHash,         // string
			block.BlockTime,          // time.Time
			block.Miner,              // string
			block.Difficulty,         // uint64: 1
			block.TotalDifficulty,    // uint64: 1000
			block.Size,               // uint64: 1331
			block.GasLimit,           // uint64: 20006296
			block.GasUsed,            // uint64: 183061
			block.BaseFeePerGas,      // uint64: 470000000000
			block.BlockGasCost,       // uint64: 0
			block.StateRoot,          // string
			block.TransactionsRoot,   // string
			block.ReceiptsRoot,       // string
			block.ExtraData,          // string
			block.BlockExtraData,     // string
			block.ExtDataHash,        // string
			block.ExtDataGasUsed,     // uint32
			block.MixHash,            // string
			nonceStr,                 // string or nil
			block.Sha3Uncles,         // string
			block.Uncles,             // []string
			block.BlobGasUsed,        // uint64
			block.ExcessBlobGas,      // uint64
			parentBeaconBlockRootStr, // string or nil
			block.MinDelayExcess,     // uint64
		).
		Return(nil).
		Once()

	repo, err := NewBlocks(ctx, testutils.NewTestClient(mockConn), "default.raw_blocks")
	require.NoError(t, err)
	err = repo.WriteBlock(ctx, block)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_WriteBlock_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	block := createTestBlock()
	execErr := errors.New("exec failed")

	// For nullable nonce - convert empty string to nil
	var nonceStr interface{}
	if block.Nonce == "" {
		nonceStr = nil
	} else {
		nonceStr = block.Nonce
	}

	// For nullable parent_beacon_block_root - convert empty string to nil
	var parentBeaconBlockRootStr interface{}
	if block.ParentBeaconBlockRoot == "" {
		parentBeaconBlockRootStr = nil
	} else {
		parentBeaconBlockRootStr = block.ParentBeaconBlockRoot
	}

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_blocks")
		})).
		Return(nil).
		Once()

	// Expect WriteBlock call that fails
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			block.BcID,
			block.EvmID,
			block.BlockNumber,
			block.Hash,
			block.ParentHash,
			block.BlockTime,
			block.Miner,
			block.Difficulty,
			block.TotalDifficulty,
			block.Size,
			block.GasLimit,
			block.GasUsed,
			block.BaseFeePerGas,
			block.BlockGasCost,
			block.StateRoot,
			block.TransactionsRoot,
			block.ReceiptsRoot,
			block.ExtraData,
			block.BlockExtraData,
			block.ExtDataHash,
			block.ExtDataGasUsed,
			block.MixHash,
			nonceStr,
			block.Sha3Uncles,
			block.Uncles,
			block.BlobGasUsed,
			block.ExcessBlobGas,
			parentBeaconBlockRootStr,
			block.MinDelayExcess,
		).
		Return(execErr).
		Once()

	repo, err := NewBlocks(ctx, testutils.NewTestClient(mockConn), "default.raw_blocks")
	require.NoError(t, err)
	err = repo.WriteBlock(ctx, block)
	require.ErrorIs(t, err, execErr)
	assert.Contains(t, err.Error(), "failed to write block")
	assert.Contains(t, err.Error(), "exec failed")
	mockConn.AssertExpectations(t)
}

// Helper function to create a test block with all fields populated
func createTestBlock() *BlockRow {
	hash := testBlockHash
	parentHash := "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40"
	miner := "0x4142434445464748494a4b4c4d4e4f5051525354"
	nonce := "0x55565758595a5b5c"

	return &BlockRow{
		BcID:                  big.NewInt(43113),
		EvmID:                 big.NewInt(0),
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
		ExtDataHash:           "",
		ExtDataGasUsed:        0,
		MixHash:               "",
		Nonce:                 nonce,
		Sha3Uncles:            "",
		Uncles:                nil,
		BlobGasUsed:           0,
		ExcessBlobGas:         0,
		ParentBeaconBlockRoot: "",
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
