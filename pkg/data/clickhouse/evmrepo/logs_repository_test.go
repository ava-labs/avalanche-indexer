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

func TestLogsRepository_WriteLog_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Create a test log
	log := createTestLog()

	// Convert hex strings to binary strings for FixedString fields (matching what WriteLog does)
	blockHashBytes, err := utils.HexToBytes32(log.BlockHash)
	require.NoError(t, err, "blockHash conversion should succeed")
	txHashBytes, err := utils.HexToBytes32(log.TxHash)
	require.NoError(t, err, "txHash conversion should succeed")
	addressBytes, err := utils.HexToBytes20(log.Address)
	require.NoError(t, err, "address conversion should succeed")

	// Convert topics
	topic0Bytes, err := utils.HexToBytes32(*log.Topic0)
	require.NoError(t, err, "topic0 conversion should succeed")
	topic1Bytes, err := utils.HexToBytes32(*log.Topic1)
	require.NoError(t, err, "topic1 conversion should succeed")
	topic2Bytes, err := utils.HexToBytes32(*log.Topic2)
	require.NoError(t, err, "topic2 conversion should succeed")

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_logs")
		})).
		Return(nil).
		Once()

	// Expect WriteLog call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			// Verify the query contains INSERT INTO and the table name
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "default.raw_logs")
		}),
			*log.BlockchainID,         // string: blockchain ID
			log.EVMChainID.String(),   // string: UInt256
			log.BlockNumber,           // uint64
			string(blockHashBytes[:]), // string: 32-byte binary string
			log.BlockTime,             // time.Time
			string(txHashBytes[:]),    // string: 32-byte binary string
			log.TxIndex,               // uint32
			string(addressBytes[:]),   // string: 20-byte binary string
			string(topic0Bytes[:]),    // string: 32-byte binary string (topic0)
			string(topic1Bytes[:]),    // string: 32-byte binary string (topic1)
			string(topic2Bytes[:]),    // string: 32-byte binary string (topic2)
			nil,                       // nil: topic3 is nil
			string(log.Data),          // string: binary data
			log.LogIndex,              // uint32
			log.Removed,               // uint8
		).
		Return(nil).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default.raw_logs")
	require.NoError(t, err)
	err = repo.WriteLog(ctx, log)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestLogsRepository_WriteLog_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	log := createTestLog()
	execErr := errors.New("exec failed")

	// Convert hex strings to binary strings for FixedString fields
	blockHashBytes, err := utils.HexToBytes32(log.BlockHash)
	require.NoError(t, err, "blockHash conversion should succeed")
	txHashBytes, err := utils.HexToBytes32(log.TxHash)
	require.NoError(t, err, "txHash conversion should succeed")
	addressBytes, err := utils.HexToBytes20(log.Address)
	require.NoError(t, err, "address conversion should succeed")

	// Convert topics
	topic0Bytes, err := utils.HexToBytes32(*log.Topic0)
	require.NoError(t, err, "topic0 conversion should succeed")
	topic1Bytes, err := utils.HexToBytes32(*log.Topic1)
	require.NoError(t, err, "topic1 conversion should succeed")
	topic2Bytes, err := utils.HexToBytes32(*log.Topic2)
	require.NoError(t, err, "topic2 conversion should succeed")

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_logs")
		})).
		Return(nil).
		Once()

	// Expect WriteLog call that fails
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			*log.BlockchainID,         // string: blockchain ID
			log.EVMChainID.String(),   // string: UInt256
			log.BlockNumber,           // uint64
			string(blockHashBytes[:]), // string: 32-byte binary string
			log.BlockTime,             // time.Time
			string(txHashBytes[:]),    // string: 32-byte binary string
			log.TxIndex,               // uint32
			string(addressBytes[:]),   // string: 20-byte binary string
			string(topic0Bytes[:]),    // string: 32-byte binary string (topic0)
			string(topic1Bytes[:]),    // string: 32-byte binary string (topic1)
			string(topic2Bytes[:]),    // string: 32-byte binary string (topic2)
			nil,                       // nil: topic3 is nil
			string(log.Data),          // string: binary data
			log.LogIndex,              // uint32
			log.Removed,               // uint8
		).
		Return(execErr).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default.raw_logs")
	require.NoError(t, err)
	err = repo.WriteLog(ctx, log)
	require.ErrorIs(t, err, execErr)
	assert.Contains(t, err.Error(), "failed to write log")
	assert.Contains(t, err.Error(), "exec failed")
	mockConn.AssertExpectations(t)
}

func TestLogsRepository_WriteLog_NilTopics(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Create a test log with nil topics
	log := createTestLog()
	log.Topic0 = nil
	log.Topic1 = nil
	log.Topic2 = nil
	log.Topic3 = nil

	// Convert hex strings to binary strings for FixedString fields
	blockHashBytes, err := utils.HexToBytes32(log.BlockHash)
	require.NoError(t, err, "blockHash conversion should succeed")
	txHashBytes, err := utils.HexToBytes32(log.TxHash)
	require.NoError(t, err, "txHash conversion should succeed")
	addressBytes, err := utils.HexToBytes20(log.Address)
	require.NoError(t, err, "address conversion should succeed")

	// Expect CreateTableIfNotExists call during initialization
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "default.raw_logs")
		})).
		Return(nil).
		Once()

	// Expect WriteLog call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "default.raw_logs")
		}),
			*log.BlockchainID,         // string: blockchain ID
			log.EVMChainID.String(),   // string: UInt256
			log.BlockNumber,           // uint64
			string(blockHashBytes[:]), // string: 32-byte binary string
			log.BlockTime,             // time.Time
			string(txHashBytes[:]),    // string: 32-byte binary string
			log.TxIndex,               // uint32
			string(addressBytes[:]),   // string: 20-byte binary string
			nil,                       // nil: topic0 is nil
			nil,                       // nil: topic1 is nil
			nil,                       // nil: topic2 is nil
			nil,                       // nil: topic3 is nil
			string(log.Data),          // string: binary data
			log.LogIndex,              // uint32
			log.Removed,               // uint8
		).
		Return(nil).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default.raw_logs")
	require.NoError(t, err)
	err = repo.WriteLog(ctx, log)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

// Helper function to create a test log with all fields populated
func createTestLog() *LogRow {
	blockHash := testBlockHash
	txHash := "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40"
	address := "0x4142434445464748494a4b4c4d4e4f5051525354"

	topic0 := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" // Transfer event signature
	topic1 := "0x0000000000000000000000004142434445464748494a4b4c4d4e4f5051525354" // from address
	topic2 := "0x00000000000000000000000055565758595a5b5c5d5e5f6061626364656667"   // to address

	blockchainID := "11111111111111111111111111111111LpoYY"
	return &LogRow{
		BlockchainID: &blockchainID,
		EVMChainID:   big.NewInt(43113),
		BlockNumber:  1647,
		BlockHash:    blockHash,
		BlockTime:    time.Unix(1604768510, 0).UTC(),
		TxHash:       txHash,
		TxIndex:      0,
		Address:      address,
		Topic0:       &topic0,
		Topic1:       &topic1,
		Topic2:       &topic2,
		Topic3:       nil,
		Data:         []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		LogIndex:     0,
		Removed:      0,
	}
}
