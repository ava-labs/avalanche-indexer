package models

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRepository_WriteBlock_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := context.Background()

	// Create a test block
	block := createTestBlock()

	// Expect Exec with query and all block fields
	// We have 28 variadic arguments, so we need to provide mock.Anything for each
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			// Verify the query contains INSERT INTO and the table name
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "default.raw_blocks")
		}), mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	repo := NewRepository(testutils.NewTestClient(mockConn, zap.NewNop().Sugar()), "default.raw_blocks")
	err := repo.WriteBlock(ctx, block)
	assert.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_WriteBlock_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := context.Background()

	block := createTestBlock()
	execErr := errors.New("exec failed")

	// Need to provide mock.Anything for context, query, and all 28 variadic arguments
	mockConn.
		On("Exec", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(execErr)

	repo := NewRepository(testutils.NewTestClient(mockConn, zap.NewNop().Sugar()), "default.raw_blocks")
	err := repo.WriteBlock(ctx, block)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write block")
	assert.Contains(t, err.Error(), "exec failed")
	mockConn.AssertExpectations(t)
}

func TestRepository_WriteBlock_WithUncles(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := context.Background()

	block := createTestBlock()
	// Add some uncles
	uncle1, err := utils.HexToBytes32("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	require.NoError(t, err)
	uncle2, err := utils.HexToBytes32("0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40")
	require.NoError(t, err)
	block.Uncles = [][32]byte{uncle1, uncle2}

	// Need to provide mock.Anything for context, query, and all 28 variadic arguments
	mockConn.
		On("Exec", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	repo := NewRepository(testutils.NewTestClient(mockConn, zap.NewNop().Sugar()), "default.raw_blocks")
	err = repo.WriteBlock(ctx, block)
	assert.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_WriteBlock_TableNameInQuery(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := context.Background()

	tableName := "custom_db.custom_table"
	block := createTestBlock()

	var capturedQuery string
	// Need to provide mock.Anything for all 28 variadic arguments
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			capturedQuery = q
			return true
		}), mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	repo := NewRepository(testutils.NewTestClient(mockConn, zap.NewNop().Sugar()), tableName)
	err := repo.WriteBlock(ctx, block)
	assert.NoError(t, err)
	assert.Contains(t, capturedQuery, tableName)
	mockConn.AssertExpectations(t)
}

func TestNewRepository(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	tableName := "test_table"

	repo := NewRepository(testutils.NewTestClient(mockConn, zap.NewNop().Sugar()), tableName)
	assert.NotNil(t, repo)

	// Verify it implements the Repository interface
	var _ Repository = repo
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
