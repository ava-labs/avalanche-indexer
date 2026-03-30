package evmrepo

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

const testInvalidHash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

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

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	// Expect WriteTransaction call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`raw_transactions`")
		}),
			*tx.BlockchainID,       // string: blockchain ID
			tx.EVMChainID.String(), // string: UInt256
			tx.BlockNumber,
			string(blockHashBytes[:]), // string: 32-byte binary string
			tx.BlockTime,
			tx.TimestampMs,
			string(hashBytes[:]), // string: 32-byte binary string
			string(fromBytes[:]), // string: 20-byte binary string
			toBytes,              // string or nil: 20-byte binary string
			tx.Nonce,
			tx.Value.String(), // string: UInt256
			tx.Gas,
			tx.GasUsed,
			tx.EffectiveGasPrice.String(), // string: UInt256
			tx.GasPrice.String(),          // string: UInt256
			tx.MaxFeePerGas.String(),      // string: UInt256 (nullable)
			tx.MaxPriorityFee.String(),    // string: UInt256 (nullable)
			tx.Input,
			tx.Type,
			tx.TransactionIndex,
			tx.Success, // UInt8: success status
			tx.NumLogs, // uint32: number of logs
		).
		Return(nil).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
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

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	// Expect WriteTransaction call that fails
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			*tx.BlockchainID,       // string: blockchain ID
			tx.EVMChainID.String(), // string: UInt256
			tx.BlockNumber,
			string(blockHashBytes[:]), // string: 32-byte binary string
			tx.BlockTime,
			tx.TimestampMs,
			string(hashBytes[:]), // string: 32-byte binary string
			string(fromBytes[:]), // string: 20-byte binary string
			toBytes,              // string or nil: 20-byte binary string
			tx.Nonce,
			tx.Value.String(), // string: UInt256
			tx.Gas,
			tx.GasUsed,
			tx.EffectiveGasPrice.String(), // string: UInt256
			tx.GasPrice.String(),          // string: UInt256
			tx.MaxFeePerGas.String(),      // string: UInt256 (nullable)
			tx.MaxPriorityFee.String(),    // string: UInt256 (nullable)
			tx.Input,
			tx.Type,
			tx.TransactionIndex,
			tx.Success, // UInt8: success status
			tx.NumLogs, // uint32: number of logs
		).
		Return(execErr).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
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

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	// Expect WriteTransaction call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`raw_transactions`")
		}),
			*tx.BlockchainID,       // string: blockchain ID
			tx.EVMChainID.String(), // string: UInt256
			tx.BlockNumber,
			string(blockHashBytes[:]), // string: 32-byte binary string
			tx.BlockTime,
			tx.TimestampMs,
			string(hashBytes[:]), // string: 32-byte binary string
			string(fromBytes[:]), // string: 20-byte binary string
			toBytes,              // nil for contract creation
			tx.Nonce,
			tx.Value.String(), // string: UInt256
			tx.Gas,
			tx.GasUsed,
			tx.EffectiveGasPrice.String(), // string: UInt256
			tx.GasPrice.String(),          // string: UInt256
			tx.MaxFeePerGas.String(),      // string: UInt256 (nullable)
			tx.MaxPriorityFee.String(),    // string: UInt256 (nullable)
			tx.Input,
			tx.Type,
			tx.TransactionIndex,
			tx.Success, // UInt8: success status
			tx.NumLogs, // uint32: number of logs
		).
		Return(nil).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)
	err = repo.WriteTransaction(ctx, tx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestTransactionsRepository_DeleteTransactions_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	chainID := uint64(43114)

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	// Expect DeleteTransactions call
	mockConn.
		On("Exec", mock.Anything, "DELETE FROM `default`.`raw_transactions_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n", chainID).
		Return(nil).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)
	err = repo.DeleteTransactions(ctx, chainID)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestTransactionsRepository_DeleteTransactions_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	chainID := uint64(43114)
	deleteErr := errors.New("delete failed")

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	// Expect DeleteTransactions call that fails
	mockConn.
		On("Exec", mock.Anything, "DELETE FROM `default`.`raw_transactions_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n", chainID).
		Return(deleteErr).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)
	err = repo.DeleteTransactions(ctx, chainID)
	require.ErrorIs(t, err, deleteErr)
	assert.Contains(t, err.Error(), "failed to delete transactions")
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
		BlockchainID:      &blockchainID,
		EVMChainID:        big.NewInt(0),
		BlockNumber:       1647,
		BlockHash:         blockHash,
		BlockTime:         time.Unix(1604768510, 0).UTC(),
		TimestampMs:       1604768510000,
		Hash:              txHash,
		From:              from,
		To:                &to,
		Nonce:             42,
		Value:             big.NewInt(1000000000000000000), // 1 ETH in wei
		Gas:               21000,
		GasUsed:           11000,
		EffectiveGasPrice: big.NewInt(470000000000), // 470 gwei
		GasPrice:          big.NewInt(470000000000), // 470 gwei
		MaxFeePerGas:      big.NewInt(1000000000),
		MaxPriorityFee:    big.NewInt(2000000000),
		Input:             "0x",
		Type:              2, // EIP-1559 transaction
		TransactionIndex:  0,
		Success:           1, // Default to success for tests
	}
}

func TestTransactionsRepository_BatchInsertTransactions_Success(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	tx1 := createTestTransaction()
	tx2 := createTestTransaction()
	tx2.BlockNumber = 1648
	tx2.Hash = testInvalidHash

	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 &&
				containsSubstring(q, "INSERT INTO") &&
				containsSubstring(q, "`default`.`raw_transactions`")
		})).
		Return(mockBatch, nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			chRow, ok := row.(*chTransactionRow)
			if !ok {
				return false
			}
			return chRow.BlockNumber == tx1.BlockNumber
		})).
		Return(nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			chRow, ok := row.(*chTransactionRow)
			if !ok {
				return false
			}
			return chRow.BlockNumber == tx2.BlockNumber
		})).
		Return(nil).
		Once()

	mockBatch.
		On("Send").
		Return(nil).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)

	err = repo.BatchInsertTransactions(ctx, []*TransactionRow{tx1, tx2})
	require.NoError(t, err)

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}

func TestTransactionsRepository_BatchInsertTransactions_Empty(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)

	err = repo.BatchInsertTransactions(ctx, nil)
	require.NoError(t, err)

	err = repo.BatchInsertTransactions(ctx, []*TransactionRow{})
	require.NoError(t, err)

	mockConn.AssertExpectations(t)
}

func TestTransactionsRepository_BatchInsertTransactions_SkipsNilTransactions(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	tx := createTestTransaction()

	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 &&
				containsSubstring(q, "INSERT INTO") &&
				containsSubstring(q, "`default`.`raw_transactions`")
		})).
		Return(mockBatch, nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			chRow, ok := row.(*chTransactionRow)
			if !ok {
				return false
			}
			return chRow.BlockNumber == tx.BlockNumber
		})).
		Return(nil).
		Once()

	mockBatch.
		On("Send").
		Return(nil).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)

	err = repo.BatchInsertTransactions(ctx, []*TransactionRow{nil, tx, nil})
	require.NoError(t, err)

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}

func TestTransactionsRepository_BatchInsertTransactions_PrepareBatchError(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	prepareErr := errors.New("prepare batch failed")
	tx := createTestTransaction()

	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.Anything).
		Return(nil, prepareErr).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)

	err = repo.BatchInsertTransactions(ctx, []*TransactionRow{tx})
	require.ErrorIs(t, err, prepareErr)
	assert.Contains(t, err.Error(), "failed to prepare batch")

	mockConn.AssertExpectations(t)
}

func TestTransactionsRepository_BatchInsertTransactions_ConvertError(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	tx := createTestTransaction()
	tx.Hash = "invalid-hash"

	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.Anything).
		Return(mockBatch, nil).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)

	err = repo.BatchInsertTransactions(ctx, []*TransactionRow{tx})
	assert.Contains(t, err.Error(), "failed to convert transaction row")
	assert.Contains(t, err.Error(), tx.Hash)

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}

func TestTransactionsRepository_BatchInsertTransactions_AppendStructError(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	appendErr := errors.New("append failed")
	tx := createTestTransaction()

	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.Anything).
		Return(mockBatch, nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			_, ok := row.(*chTransactionRow)
			return ok
		})).
		Return(appendErr).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)

	err = repo.BatchInsertTransactions(ctx, []*TransactionRow{tx})
	require.ErrorIs(t, err, appendErr)
	assert.Contains(t, err.Error(), "failed to append transaction")
	assert.Contains(t, err.Error(), tx.Hash)

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}

func TestTransactionsRepository_BatchInsertTransactions_SendError(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	sendErr := errors.New("send failed")
	tx := createTestTransaction()

	expectTableInit(mockConn, "raw_transactions_local", "raw_transactions")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.Anything).
		Return(mockBatch, nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			_, ok := row.(*chTransactionRow)
			return ok
		})).
		Return(nil).
		Once()

	mockBatch.
		On("Send").
		Return(sendErr).
		Once()

	repo, err := NewTransactions(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_transactions")
	require.NoError(t, err)

	err = repo.BatchInsertTransactions(ctx, []*TransactionRow{tx})
	require.ErrorIs(t, err, sendErr)
	assert.Contains(t, err.Error(), "failed to send batch")

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}
