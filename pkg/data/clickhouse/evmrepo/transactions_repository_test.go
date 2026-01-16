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

const testBlockHash = "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

func TestTransactionsRepository_WriteTransaction_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Create a test transaction
	tx := createTestTransaction()

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_transactions")
		})).
		Return(nil).
		Once()

	// Expect WriteTransaction call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			// Verify the query contains INSERT INTO and the table name
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "default.raw_transactions")
		}),
			uint32(tx.BcID.Uint64()),  // uint32: converted from *big.Int
			uint32(tx.EvmID.Uint64()), // uint32: converted from *big.Int
			tx.BlockNumber,
			tx.BlockHash,
			tx.BlockTime,
			tx.Hash,
			tx.From,
			tx.To,
			tx.Nonce,
			tx.Value,
			tx.Gas,
			tx.GasPrice,
			tx.MaxFeePerGas,
			tx.MaxPriorityFee,
			tx.Input,
			tx.Type,
			tx.TransactionIndex,
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

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_transactions")
		})).
		Return(nil).
		Once()

	// Expect WriteTransaction call that fails
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			uint32(tx.BcID.Uint64()),  // uint32: converted from *big.Int
			uint32(tx.EvmID.Uint64()), // uint32: converted from *big.Int
			tx.BlockNumber,
			tx.BlockHash,
			tx.BlockTime,
			tx.Hash,
			tx.From,
			tx.To,
			tx.Nonce,
			tx.Value,
			tx.Gas,
			tx.GasPrice,
			tx.MaxFeePerGas,
			tx.MaxPriorityFee,
			tx.Input,
			tx.Type,
			tx.TransactionIndex,
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

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_transactions")
		})).
		Return(nil).
		Once()

	// Expect WriteTransaction call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "default.raw_transactions")
		}),
			uint32(tx.BcID.Uint64()),  // uint32: converted from *big.Int
			uint32(tx.EvmID.Uint64()), // uint32: converted from *big.Int
			tx.BlockNumber,
			tx.BlockHash,
			tx.BlockTime,
			tx.Hash,
			tx.From,
			mock.Anything, // To can be nil or *string
			tx.Nonce,
			tx.Value,
			tx.Gas,
			tx.GasPrice,
			tx.MaxFeePerGas,
			tx.MaxPriorityFee,
			tx.Input,
			tx.Type,
			tx.TransactionIndex,
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
	txHash := "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40"
	from := "0x4142434445464748494a4b4c4d4e4f5051525354"
	to := "0x55565758595a5b5c5d5e5f6061626364656667"
	maxFee := "1000000000"
	maxPriority := "2000000000"

	return &TransactionRow{
		BcID:             big.NewInt(43113),
		EvmID:            big.NewInt(0),
		BlockNumber:      1647,
		BlockHash:        blockHash,
		BlockTime:        time.Unix(1604768510, 0).UTC(),
		Hash:             txHash,
		From:             from,
		To:               &to,
		Nonce:            42,
		Value:            "1000000000000000000", // 1 ETH in wei
		Gas:              21000,
		GasPrice:         "470000000000", // 470 gwei
		MaxFeePerGas:     &maxFee,
		MaxPriorityFee:   &maxPriority,
		Input:            "0x",
		Type:             2, // EIP-1559 transaction
		TransactionIndex: 0,
	}
}
