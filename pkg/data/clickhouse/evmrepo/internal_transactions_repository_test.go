package evmrepo

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

func TestInternalTransactionsRepository_WriteInternalTransaction_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	tx := createTestInternalTransaction()

	// Convert hex strings to binary strings for FixedString fields
	txHashBytes, err := utils.HexToBytes32(tx.TransactionHash)
	require.NoError(t, err, "txHash conversion should succeed")

	// Expect CreateTableIfNotExists and migration calls during initialization
	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	// Expect WriteInternalTransaction call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`internal_transactions`")
		}),
			*tx.BlockchainID,       // string: blockchain ID
			tx.EVMChainID.String(), // string: UInt256
			tx.BlockNumber,
			tx.BlockTimestamp,      // uint64: DateTime64(3)
			tx.TimestampMs,         // uint64
			string(txHashBytes[:]), // string: 32-byte binary string
			tx.Type,
			string(tx.From[:]), // string: 20-byte binary string
			string(tx.To[:]),   // string: 20-byte binary string
			tx.Value,           // string
			tx.Gas,             // string
			tx.GasUsed,         // string
			tx.Revert,          // bool
			tx.Error,           // string
			tx.RevertReason,    // string
			tx.Input,           // string
			tx.Output,          // string
			tx.CallIndex,       // string
		).
		Return(nil).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.WriteInternalTransaction(ctx, tx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_WriteInternalTransaction_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	tx := createTestInternalTransaction()
	execErr := errors.New("exec failed")

	// Convert hex strings to binary strings
	txHashBytes, err := utils.HexToBytes32(tx.TransactionHash)
	require.NoError(t, err, "txHash conversion should succeed")

	// Expect CreateTableIfNotExists and migration calls during initialization
	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	// Expect WriteInternalTransaction call that fails
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			*tx.BlockchainID,
			tx.EVMChainID.String(),
			tx.BlockNumber,
			tx.BlockTimestamp,
			tx.TimestampMs,
			string(txHashBytes[:]),
			tx.Type,
			string(tx.From[:]),
			string(tx.To[:]),
			tx.Value,
			tx.Gas,
			tx.GasUsed,
			tx.Revert,
			tx.Error,
			tx.RevertReason,
			tx.Input,
			tx.Output,
			tx.CallIndex,
		).
		Return(execErr).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.WriteInternalTransaction(ctx, tx)
	require.ErrorIs(t, err, execErr)
	assert.Contains(t, err.Error(), "failed to write internal transaction")
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_WriteInternalTransaction_NilBlockchainID(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	tx := createTestInternalTransaction()
	tx.BlockchainID = nil

	txHashBytes, err := utils.HexToBytes32(tx.TransactionHash)
	require.NoError(t, err)

	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	// Empty string should be used for nil BlockchainID
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			"", // Empty string for nil BlockchainID
			tx.EVMChainID.String(),
			tx.BlockNumber,
			tx.BlockTimestamp,
			tx.TimestampMs,
			string(txHashBytes[:]),
			tx.Type,
			string(tx.From[:]),
			string(tx.To[:]),
			tx.Value,
			tx.Gas,
			tx.GasUsed,
			tx.Revert,
			tx.Error,
			tx.RevertReason,
			tx.Input,
			tx.Output,
			tx.CallIndex,
		).
		Return(nil).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.WriteInternalTransaction(ctx, tx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_WriteInternalTransaction_NilEVMChainID(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	tx := createTestInternalTransaction()
	tx.EVMChainID = nil

	txHashBytes, err := utils.HexToBytes32(tx.TransactionHash)
	require.NoError(t, err)

	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	// "0" should be used for nil EVMChainID
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			*tx.BlockchainID,
			"0", // Default "0" for nil EVMChainID
			tx.BlockNumber,
			tx.BlockTimestamp,
			tx.TimestampMs,
			string(txHashBytes[:]),
			tx.Type,
			string(tx.From[:]),
			string(tx.To[:]),
			tx.Value,
			tx.Gas,
			tx.GasUsed,
			tx.Revert,
			tx.Error,
			tx.RevertReason,
			tx.Input,
			tx.Output,
			tx.CallIndex,
		).
		Return(nil).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.WriteInternalTransaction(ctx, tx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_WriteInternalTransaction_InvalidTxHash(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	tx := createTestInternalTransaction()
	tx.TransactionHash = "invalid_hash"

	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.WriteInternalTransaction(ctx, tx)
	assert.Contains(t, err.Error(), "failed to convert transaction_hash to bytes")
	mockConn.AssertExpectations(t)
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_WriteInternalTransaction_WithRevert(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	tx := createTestInternalTransaction()
	tx.Revert = true
	tx.Error = "execution reverted"
	tx.RevertReason = "insufficient funds"

	txHashBytes, err := utils.HexToBytes32(tx.TransactionHash)
	require.NoError(t, err)

	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			*tx.BlockchainID,
			tx.EVMChainID.String(),
			tx.BlockNumber,
			tx.BlockTimestamp,
			tx.TimestampMs,
			string(txHashBytes[:]),
			tx.Type,
			string(tx.From[:]),
			string(tx.To[:]),
			tx.Value,
			tx.Gas,
			tx.GasUsed,
			true, // Revert
			"execution reverted",
			"insufficient funds",
			tx.Input,
			tx.Output,
			tx.CallIndex,
		).
		Return(nil).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.WriteInternalTransaction(ctx, tx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_DeleteInternalTransactions_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	chainID := uint64(43114)

	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	// Expect DeleteInternalTransactions call
	mockConn.
		On("Exec", mock.Anything, "DELETE FROM `default`.`internal_transactions_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n", chainID).
		Return(nil).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.DeleteInternalTransactions(ctx, chainID)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_DeleteInternalTransactions_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	chainID := uint64(43114)
	deleteErr := errors.New("delete failed")

	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	// Expect DeleteInternalTransactions call that fails
	mockConn.
		On("Exec", mock.Anything, "DELETE FROM `default`.`internal_transactions_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n", chainID).
		Return(deleteErr).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.DeleteInternalTransactions(ctx, chainID)
	require.ErrorIs(t, err, deleteErr)
	assert.Contains(t, err.Error(), "failed to delete internal transactions")
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_CreateTableIfNotExists_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	require.NotNil(t, repo)
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_CreateTableIfNotExists_LocalTableError(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	createErr := errors.New("create table failed")

	// Expect CreateTableIfNotExists call for local table that fails
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "`internal_transactions_local`")
		})).
		Return(createErr).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	assert.Nil(t, repo)
	require.ErrorIs(t, err, createErr)
	assert.Contains(t, err.Error(), "failed to initialize internal_transactions table")
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_CreateTableIfNotExists_DistributedTableError(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	createErr := errors.New("create distributed table failed")

	// Expect CreateTableIfNotExists call for local table (succeeds)
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "`internal_transactions_local`")
		})).
		Return(nil).
		Once()

	// Expect CreateTableIfNotExists call for distributed table (fails)
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "`default`.`internal_transactions`")
		})).
		Return(createErr).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	assert.Nil(t, repo)
	require.ErrorIs(t, err, createErr)
	assert.Contains(t, err.Error(), "failed to initialize internal_transactions table")
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_WriteInternalTransaction_ZeroAddresses(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	tx := createTestInternalTransaction()
	tx.From = common.HexToAddress("0x0000000000000000000000000000000000000000")
	tx.To = common.HexToAddress("0x0000000000000000000000000000000000000000")

	txHashBytes, err := utils.HexToBytes32(tx.TransactionHash)
	require.NoError(t, err)

	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			*tx.BlockchainID,
			tx.EVMChainID.String(),
			tx.BlockNumber,
			tx.BlockTimestamp,
			tx.TimestampMs,
			string(txHashBytes[:]),
			tx.Type,
			string(tx.From[:]),
			string(tx.To[:]),
			tx.Value,
			tx.Gas,
			tx.GasUsed,
			tx.Revert,
			tx.Error,
			tx.RevertReason,
			tx.Input,
			tx.Output,
			tx.CallIndex,
		).
		Return(nil).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.WriteInternalTransaction(ctx, tx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestInternalTransactionsRepository_WriteInternalTransaction_EmptyStrings(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	tx := createTestInternalTransaction()
	tx.Error = ""
	tx.RevertReason = ""
	tx.Input = ""
	tx.Output = ""

	txHashBytes, err := utils.HexToBytes32(tx.TransactionHash)
	require.NoError(t, err)

	expectTableInit(mockConn, "internal_transactions_local", "internal_transactions")

	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			*tx.BlockchainID,
			tx.EVMChainID.String(),
			tx.BlockNumber,
			tx.BlockTimestamp,
			tx.TimestampMs,
			string(txHashBytes[:]),
			tx.Type,
			string(tx.From[:]),
			string(tx.To[:]),
			tx.Value,
			tx.Gas,
			tx.GasUsed,
			tx.Revert,
			"", // Empty error
			"", // Empty revert reason
			"", // Empty input
			"", // Empty output
			tx.CallIndex,
		).
		Return(nil).
		Once()

	repo, err := NewInternalTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "internal_transactions")
	require.NoError(t, err)
	err = repo.WriteInternalTransaction(ctx, tx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

// Helper function to create a test internal transaction
func createTestInternalTransaction() *InternalTransactionRow {
	blockchainID := testBlockchainID
	return &InternalTransactionRow{
		BlockchainID:    &blockchainID,
		EVMChainID:      big.NewInt(43113),
		BlockNumber:     1647,
		BlockTimestamp:  1640000000,
		TimestampMs:     1640000000000,
		TransactionHash: testTxHash,
		Type:            "CALL",
		From:            common.HexToAddress(testFromAddress),
		To:              common.HexToAddress(testToAddress),
		Value:           "1000000000000000000",
		Gas:             "21000",
		GasUsed:         "21000",
		Revert:          false,
		Error:           "",
		RevertReason:    "",
		Input:           "0x",
		Output:          "0x",
		CallIndex:       "call_0",
	}
}
