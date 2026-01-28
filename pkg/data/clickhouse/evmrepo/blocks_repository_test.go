package evmrepo

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testBlockHash    = "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	testBlockchainID = "11111111111111111111111111111111LpoYY"
	testTxHash       = "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40"
	testFromAddress  = "0x4142434445464748494a4b4c4d4e4f5051525354"
	testToAddress    = "0x55565758595a5b5c5d5e5f6061626364656667"
)

func TestRepository_WriteBlock_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Create a test block
	block := createTestBlock()

	// Convert hex strings to binary strings for FixedString fields (matching what WriteBlock does)
	hashBytes, err := utils.HexToBytes32(block.Hash)
	require.NoError(t, err, "hash conversion should succeed")
	parentHashBytes, err := utils.HexToBytes32(block.ParentHash)
	require.NoError(t, err, "parentHash conversion should succeed")
	minerBytes, err := utils.HexToBytes20(block.Miner)
	require.NoError(t, err, "miner conversion should succeed")
	stateRootBytes, err := utils.HexToBytes32(block.StateRoot)
	require.NoError(t, err, "stateRoot conversion should succeed")
	transactionsRootBytes, err := utils.HexToBytes32(block.TransactionsRoot)
	require.NoError(t, err, "transactionsRoot conversion should succeed")
	receiptsRootBytes, err := utils.HexToBytes32(block.ReceiptsRoot)
	require.NoError(t, err, "receiptsRoot conversion should succeed")
	extDataHashBytes, err := utils.HexToBytes32(block.ExtDataHash)
	require.NoError(t, err, "extDataHash conversion should succeed")
	mixHashBytes, err := utils.HexToBytes32(block.MixHash)
	require.NoError(t, err, "mixHash conversion should succeed")
	sha3UnclesBytes, err := utils.HexToBytes32(block.Sha3Uncles)
	require.NoError(t, err, "sha3Uncles conversion should succeed")

	// Convert uncles array
	unclesStrings := make([]string, len(block.Uncles))
	for i, uncle := range block.Uncles {
		uncleBytes, err := utils.HexToBytes32(uncle)
		require.NoError(t, err, "uncle %d conversion should succeed", i)
		unclesStrings[i] = string(uncleBytes[:])
	}

	// For nullable nonce - convert empty string to nil, otherwise convert to binary string
	var nonceStr interface{}
	if block.Nonce == "" {
		nonceStr = nil
	} else {
		nonceBytes, err := utils.HexToBytes8(block.Nonce)
		require.NoError(t, err, "nonce conversion should succeed")
		nonceStr = string(nonceBytes[:])
	}

	// For nullable parent_beacon_block_root - convert empty string to nil, otherwise convert to binary string
	var parentBeaconBlockRootStr interface{}
	if block.ParentBeaconBlockRoot == "" {
		parentBeaconBlockRootStr = nil
	} else {
		beaconRootBytes, err := utils.HexToBytes32(block.ParentBeaconBlockRoot)
		require.NoError(t, err, "parentBeaconBlockRoot conversion should succeed")
		parentBeaconBlockRootStr = string(beaconRootBytes[:])
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
			*block.BlockchainID,              // string: "11111111111111111111111111111111LpoYY"
			block.EVMChainID.String(),        // string: "43113" (UInt256)
			block.BlockNumber.Uint64(),       // uint64: 1647
			string(hashBytes[:]),             // string: 32-byte binary string
			string(parentHashBytes[:]),       // string: 32-byte binary string
			block.BlockTime,                  // time.Time
			string(minerBytes[:]),            // string: 20-byte binary string
			block.Difficulty.String(),        // string: "1" (UInt256)
			block.TotalDifficulty.String(),   // string: "1000" (UInt256)
			block.Size,                       // uint64: 1331
			block.GasLimit,                   // uint64: 20006296
			block.GasUsed,                    // uint64: 183061
			block.BaseFeePerGas.String(),     // string: "470000000000" (UInt256)
			block.BlockGasCost.String(),      // string: "0" (UInt256)
			string(stateRootBytes[:]),        // string: 32-byte binary string
			string(transactionsRootBytes[:]), // string: 32-byte binary string
			string(receiptsRootBytes[:]),     // string: 32-byte binary string
			block.ExtraData,                  // string
			block.BlockExtraData,             // string
			string(extDataHashBytes[:]),      // string: 32-byte binary string
			block.ExtDataGasUsed,             // uint32
			string(mixHashBytes[:]),          // string: 32-byte binary string
			nonceStr,                         // string or nil
			string(sha3UnclesBytes[:]),       // string: 32-byte binary string
			unclesStrings,                    // []string: array of 32-byte binary strings
			block.BlobGasUsed,                // uint64
			block.ExcessBlobGas,              // uint64
			parentBeaconBlockRootStr,         // string or nil
			block.MinDelayExcess,             // uint64
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

	// Convert hex strings to binary strings for FixedString fields (matching what WriteBlock does)
	hashBytes, err := utils.HexToBytes32(block.Hash)
	require.NoError(t, err, "hash conversion should succeed")
	parentHashBytes, err := utils.HexToBytes32(block.ParentHash)
	require.NoError(t, err, "parentHash conversion should succeed")
	minerBytes, err := utils.HexToBytes20(block.Miner)
	require.NoError(t, err, "miner conversion should succeed")
	stateRootBytes, err := utils.HexToBytes32(block.StateRoot)
	require.NoError(t, err, "stateRoot conversion should succeed")
	transactionsRootBytes, err := utils.HexToBytes32(block.TransactionsRoot)
	require.NoError(t, err, "transactionsRoot conversion should succeed")
	receiptsRootBytes, err := utils.HexToBytes32(block.ReceiptsRoot)
	require.NoError(t, err, "receiptsRoot conversion should succeed")
	extDataHashBytes, err := utils.HexToBytes32(block.ExtDataHash)
	require.NoError(t, err, "extDataHash conversion should succeed")
	mixHashBytes, err := utils.HexToBytes32(block.MixHash)
	require.NoError(t, err, "mixHash conversion should succeed")
	sha3UnclesBytes, err := utils.HexToBytes32(block.Sha3Uncles)
	require.NoError(t, err, "sha3Uncles conversion should succeed")

	// Convert uncles array
	unclesStrings := make([]string, len(block.Uncles))
	for i, uncle := range block.Uncles {
		uncleBytes, err := utils.HexToBytes32(uncle)
		require.NoError(t, err, "uncle %d conversion should succeed", i)
		unclesStrings[i] = string(uncleBytes[:])
	}

	// For nullable nonce - convert empty string to nil, otherwise convert to binary string
	var nonceStr interface{}
	if block.Nonce == "" {
		nonceStr = nil
	} else {
		nonceBytes, err := utils.HexToBytes8(block.Nonce)
		require.NoError(t, err, "nonce conversion should succeed")
		nonceStr = string(nonceBytes[:])
	}

	// For nullable parent_beacon_block_root - convert empty string to nil, otherwise convert to binary string
	var parentBeaconBlockRootStr interface{}
	if block.ParentBeaconBlockRoot == "" {
		parentBeaconBlockRootStr = nil
	} else {
		beaconRootBytes, err := utils.HexToBytes32(block.ParentBeaconBlockRoot)
		require.NoError(t, err, "parentBeaconBlockRoot conversion should succeed")
		parentBeaconBlockRootStr = string(beaconRootBytes[:])
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
			*block.BlockchainID,        // string: blockchain ID
			block.EVMChainID.String(),  // string: evmChainID (UInt256)
			block.BlockNumber.Uint64(), // uint64: block number
			string(hashBytes[:]),
			string(parentHashBytes[:]),
			block.BlockTime,
			string(minerBytes[:]),
			block.Difficulty.String(),
			block.TotalDifficulty.String(),
			block.Size,
			block.GasLimit,
			block.GasUsed,
			block.BaseFeePerGas.String(),
			block.BlockGasCost.String(),
			string(stateRootBytes[:]),
			string(transactionsRootBytes[:]),
			string(receiptsRootBytes[:]),
			block.ExtraData,
			block.BlockExtraData,
			string(extDataHashBytes[:]),
			block.ExtDataGasUsed,
			string(mixHashBytes[:]),
			nonceStr,
			string(sha3UnclesBytes[:]),
			unclesStrings,
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
	blockchainID := testBlockchainID

	return &BlockRow{
		EVMChainID:            big.NewInt(43113),
		BlockchainID:          &blockchainID,
		BlockNumber:           big.NewInt(1647),
		Hash:                  hash,
		ParentHash:            parentHash,
		BlockTime:             time.Unix(1604768510, 0).UTC(),
		Miner:                 miner,
		Difficulty:            big.NewInt(1),
		TotalDifficulty:       big.NewInt(1000),
		Size:                  1331,
		GasLimit:              20006296,
		GasUsed:               183061,
		BaseFeePerGas:         big.NewInt(470000000000),
		BlockGasCost:          big.NewInt(0),
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
