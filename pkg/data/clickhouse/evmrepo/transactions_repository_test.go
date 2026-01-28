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

func TestTransactionsRepository_WriteTransaction_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Create a test transaction
	tx := createTestTransaction()

	// Convert hex strings to binary strings for FixedString fields (matching what WriteTransaction does)
	blockHashBytes, err := utils.HexToBytes32(tx.BlockHash)
	require.NoError(t, err, "blockHash conversion should succeed")
	hashBytes, err := utils.HexToBytes32(tx.Hash)
	require.NoError(t, err, "hash conversion should succeed")
	fromBytes, err := utils.HexToBytes20(tx.From)
	require.NoError(t, err, "fromAddress conversion should succeed")
	var toBytes interface{}
	if tx.To == nil || *tx.To == "" {
		toBytes = nil
	} else {
		toBytesVal, err := utils.HexToBytes20(*tx.To)
		require.NoError(t, err, "toAddress conversion should succeed")
		toBytes = string(toBytesVal[:])
	}

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_transactions")
		})).
		Return(nil).
		Once()

	// Calculate partition_month from BlockTime (November 2020 = 202011)
	partitionMonth := tx.BlockTime.Year()*100 + int(tx.BlockTime.Month())

	// Expect WriteTransaction call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			// Verify the query contains INSERT INTO and the table name
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "default.raw_transactions")
		}),
			*tx.BlockchainID,       // string: blockchain ID
			tx.EVMChainID.String(), // string: UInt256
			tx.BlockNumber,
			string(blockHashBytes[:]), // string: 32-byte binary string
			tx.BlockTime,
			string(hashBytes[:]), // string: 32-byte binary string
			string(fromBytes[:]), // string: 20-byte binary string
			toBytes,              // string or nil: 20-byte binary string
			tx.Nonce,
			tx.Value.String(), // string: UInt256
			tx.Gas,
			tx.GasPrice.String(),       // string: UInt256
			tx.MaxFeePerGas.String(),   // string: UInt256 (nullable)
			tx.MaxPriorityFee.String(), // string: UInt256 (nullable)
			tx.Input,
			tx.Type,
			tx.TransactionIndex,
			tx.Success,     // UInt8: success status
			partitionMonth, // int: partition_month
		).
		Return(nil).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default.raw_transactions")
	require.NoError(t, err)
	err = repo.WriteTransaction(ctx, tx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestTransactionsRepository_WriteTransaction_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	tx := createTestTransaction()
	execErr := errors.New("exec failed")

	// Convert hex strings to binary strings for FixedString fields (matching what WriteTransaction does)
	blockHashBytes, err := utils.HexToBytes32(tx.BlockHash)
	require.NoError(t, err, "blockHash conversion should succeed")
	hashBytes, err := utils.HexToBytes32(tx.Hash)
	require.NoError(t, err, "hash conversion should succeed")
	fromBytes, err := utils.HexToBytes20(tx.From)
	require.NoError(t, err, "fromAddress conversion should succeed")
	var toBytes interface{}
	if tx.To == nil || *tx.To == "" {
		toBytes = nil
	} else {
		toBytesVal, err := utils.HexToBytes20(*tx.To)
		require.NoError(t, err, "toAddress conversion should succeed")
		toBytes = string(toBytesVal[:])
	}

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_transactions")
		})).
		Return(nil).
		Once()

	// Calculate partition_month from BlockTime (November 2020 = 202011)
	partitionMonth := tx.BlockTime.Year()*100 + int(tx.BlockTime.Month())

	// Expect WriteTransaction call that fails
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			*tx.BlockchainID,       // string: blockchain ID
			tx.EVMChainID.String(), // string: UInt256
			tx.BlockNumber,
			string(blockHashBytes[:]), // string: 32-byte binary string
			tx.BlockTime,
			string(hashBytes[:]), // string: 32-byte binary string
			string(fromBytes[:]), // string: 20-byte binary string
			toBytes,              // string or nil: 20-byte binary string
			tx.Nonce,
			tx.Value.String(), // string: UInt256
			tx.Gas,
			tx.GasPrice.String(),       // string: UInt256
			tx.MaxFeePerGas.String(),   // string: UInt256 (nullable)
			tx.MaxPriorityFee.String(), // string: UInt256 (nullable)
			tx.Input,
			tx.Type,
			tx.TransactionIndex,
			tx.Success,     // UInt8: success status
			partitionMonth, // int: partition_month
		).
		Return(execErr).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default.raw_transactions")
	require.NoError(t, err)
	err = repo.WriteTransaction(ctx, tx)
	require.ErrorIs(t, err, execErr)
	assert.Contains(t, err.Error(), "failed to write transaction")
	assert.Contains(t, err.Error(), "exec failed")
	mockConn.AssertExpectations(t)
}

func TestTransactionsRepository_WriteTransaction_WithNullTo(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Create a test transaction with null To (contract creation)
	tx := createTestTransaction()
	tx.To = nil

	// Convert hex strings to binary strings for FixedString fields (matching what WriteTransaction does)
	blockHashBytes, err := utils.HexToBytes32(tx.BlockHash)
	require.NoError(t, err, "blockHash conversion should succeed")
	hashBytes, err := utils.HexToBytes32(tx.Hash)
	require.NoError(t, err, "hash conversion should succeed")
	fromBytes, err := utils.HexToBytes20(tx.From)
	require.NoError(t, err, "fromAddress conversion should succeed")
	// To is nil, so toBytes should be nil
	var toBytes interface{} = nil

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_transactions")
		})).
		Return(nil).
		Once()

	// Calculate partition_month from BlockTime (November 2020 = 202011)
	partitionMonth := tx.BlockTime.Year()*100 + int(tx.BlockTime.Month())

	// Expect WriteTransaction call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "default.raw_transactions")
		}),
			*tx.BlockchainID,       // string: blockchain ID
			tx.EVMChainID.String(), // string: UInt256
			tx.BlockNumber,
			string(blockHashBytes[:]), // string: 32-byte binary string
			tx.BlockTime,
			string(hashBytes[:]), // string: 32-byte binary string
			string(fromBytes[:]), // string: 20-byte binary string
			toBytes,              // nil for contract creation
			tx.Nonce,
			tx.Value.String(), // string: UInt256
			tx.Gas,
			tx.GasPrice.String(),       // string: UInt256
			tx.MaxFeePerGas.String(),   // string: UInt256 (nullable)
			tx.MaxPriorityFee.String(), // string: UInt256 (nullable)
			tx.Input,
			tx.Type,
			tx.TransactionIndex,
			tx.Success,     // UInt8: success status
			partitionMonth, // int: partition_month
		).
		Return(nil).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default.raw_transactions")
	require.NoError(t, err)
	err = repo.WriteTransaction(ctx, tx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

// Helper function to create a test transaction with all fields populated
func createTestTransaction() *TransactionRow {
	blockHash := testBlockHash
	txHash := testTxHash
	from := testFromAddress
	to := testToAddress

	blockchainID := testBlockchainID
	return &TransactionRow{
		BlockchainID:     &blockchainID,
		EVMChainID:       big.NewInt(0),
		BlockNumber:      1647,
		BlockHash:        blockHash,
		BlockTime:        time.Unix(1604768510, 0).UTC(),
		Hash:             txHash,
		From:             from,
		To:               &to,
		Nonce:            42,
		Value:            big.NewInt(1000000000000000000), // 1 ETH in wei
		Gas:              21000,
		GasPrice:         big.NewInt(470000000000), // 470 gwei
		MaxFeePerGas:     big.NewInt(1000000000),
		MaxPriorityFee:   big.NewInt(2000000000),
		Input:            "0x",
		Type:             2, // EIP-1559 transaction
		TransactionIndex: 0,
		Success:          1, // Default to success for tests
	}
}
