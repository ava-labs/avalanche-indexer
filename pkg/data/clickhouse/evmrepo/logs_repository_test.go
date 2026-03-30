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
	topic0Bytes, err := utils.HexToBytes32(log.Topic0)
	require.NoError(t, err, "topic0 conversion should succeed")
	topic1Bytes, err := utils.HexToBytes32(*log.Topic1)
	require.NoError(t, err, "topic1 conversion should succeed")
	topic2Bytes, err := utils.HexToBytes32(*log.Topic2)
	require.NoError(t, err, "topic2 conversion should succeed")

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	// Convert topic bytes to string pointers (matching new return type)
	topic0Str := string(topic0Bytes[:])
	topic1Str := string(topic1Bytes[:])
	topic2Str := string(topic2Bytes[:])

	// Expect WriteLog call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			// Verify the query contains INSERT INTO and the table name
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`raw_logs`")
		}),
			*log.BlockchainID,         // string: blockchain ID
			log.EVMChainID.String(),   // string: UInt256
			log.BlockNumber,           // uint64
			string(blockHashBytes[:]), // string: 32-byte binary string
			log.BlockTime,             // time.Time
			log.TimestampMs,           // uint64
			string(txHashBytes[:]),    // string: 32-byte binary string
			log.TxIndex,               // uint32
			string(addressBytes[:]),   // string: 20-byte binary string
			&topic0Str,                // *string: 32-byte binary string (topic0)
			&topic1Str,                // *string: 32-byte binary string (topic1)
			&topic2Str,                // *string: 32-byte binary string (topic2)
			(*string)(nil),            // *string nil: topic3 is nil
			string(log.Data),          // string: binary data
			log.LogIndex,              // uint32
			log.Removed,               // bool
		).
		Return(nil).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
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
	topic0Bytes, err := utils.HexToBytes32(log.Topic0)
	require.NoError(t, err, "topic0 conversion should succeed")
	topic1Bytes, err := utils.HexToBytes32(*log.Topic1)
	require.NoError(t, err, "topic1 conversion should succeed")
	topic2Bytes, err := utils.HexToBytes32(*log.Topic2)
	require.NoError(t, err, "topic2 conversion should succeed")

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	// Convert topic bytes to string pointers (matching new return type)
	topic0Str := string(topic0Bytes[:])
	topic1Str := string(topic1Bytes[:])
	topic2Str := string(topic2Bytes[:])

	// Expect WriteLog call that fails
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			*log.BlockchainID,         // string: blockchain ID
			log.EVMChainID.String(),   // string: UInt256
			log.BlockNumber,           // uint64
			string(blockHashBytes[:]), // string: 32-byte binary string
			log.BlockTime,             // time.Time
			log.TimestampMs,           // uint64
			string(txHashBytes[:]),    // string: 32-byte binary string
			log.TxIndex,               // uint32
			string(addressBytes[:]),   // string: 20-byte binary string
			&topic0Str,                // *string: 32-byte binary string (topic0)
			&topic1Str,                // *string: 32-byte binary string (topic1)
			&topic2Str,                // *string: 32-byte binary string (topic2)
			(*string)(nil),            // *string nil: topic3 is nil
			string(log.Data),          // string: binary data
			log.LogIndex,              // uint32
			log.Removed,               // bool
		).
		Return(execErr).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
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

	// Create a test log with nil/empty topics
	log := createTestLog()
	log.Topic0 = ""
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

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	// Expect WriteLog call
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`raw_logs`")
		}),
			*log.BlockchainID,         // string: blockchain ID
			log.EVMChainID.String(),   // string: UInt256
			log.BlockNumber,           // uint64
			string(blockHashBytes[:]), // string: 32-byte binary string
			log.BlockTime,             // time.Time
			log.TimestampMs,           // uint64
			string(txHashBytes[:]),    // string: 32-byte binary string
			log.TxIndex,               // uint32
			string(addressBytes[:]),   // string: 20-byte binary string
			(*string)(nil),            // *string nil: topic0 is nil
			(*string)(nil),            // *string nil: topic1 is nil
			(*string)(nil),            // *string nil: topic2 is nil
			(*string)(nil),            // *string nil: topic3 is nil
			string(log.Data),          // string: binary data
			log.LogIndex,              // uint32
			log.Removed,               // bool
		).
		Return(nil).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)
	err = repo.WriteLog(ctx, log)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestLogsRepository_DeleteLogs_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	chainID := uint64(43114)

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	// Expect DeleteLogs call
	mockConn.
		On("Exec", mock.Anything, "DELETE FROM `default`.`raw_logs_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n", chainID).
		Return(nil).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)
	err = repo.DeleteLogs(ctx, chainID)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestLogsRepository_DeleteLogs_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	chainID := uint64(43114)
	deleteErr := errors.New("delete failed")

	// Expect CreateTableIfNotExists + migrations during initialization
	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	// Expect DeleteLogs call that fails
	mockConn.
		On("Exec", mock.Anything, "DELETE FROM `default`.`raw_logs_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n", chainID).
		Return(deleteErr).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)
	err = repo.DeleteLogs(ctx, chainID)
	require.ErrorIs(t, err, deleteErr)
	assert.Contains(t, err.Error(), "failed to delete logs")
	mockConn.AssertExpectations(t)
}

// Helper function to create a test log with all fields populated
func createTestLog() *LogRow {
	blockHash := testBlockHash
	txHash := testTxHash
	address := testFromAddress

	topic0 := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" // Transfer event signature
	topic1 := "0x0000000000000000000000004142434445464748494a4b4c4d4e4f5051525354" // from address
	topic2 := "0x00000000000000000000000055565758595a5b5c5d5e5f6061626364656667"   // to address

	blockchainID := testBlockchainID
	return &LogRow{
		BlockchainID: &blockchainID,
		EVMChainID:   big.NewInt(43113),
		BlockNumber:  1647,
		BlockHash:    blockHash,
		BlockTime:    time.Unix(1604768510, 0).UTC(),
		TimestampMs:  1604768510000,
		TxHash:       txHash,
		TxIndex:      0,
		Address:      address,
		Topic0:       topic0,
		Topic1:       &topic1,
		Topic2:       &topic2,
		Topic3:       nil,
		Data:         []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		LogIndex:     0,
		Removed:      false,
	}
}

func TestLogsRepository_BatchInsertLogs_Success(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	log1 := createTestLog()
	log2 := createTestLog()
	log2.BlockNumber = 1648
	log2.LogIndex = 1
	log2.TxHash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 &&
				containsSubstring(q, "INSERT INTO") &&
				containsSubstring(q, "`default`.`raw_logs`")
		})).
		Return(mockBatch, nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			chRow, ok := row.(*chLogRow)
			if !ok {
				return false
			}
			return chRow.BlockNumber == log1.BlockNumber &&
				chRow.LogIndex == log1.LogIndex
		})).
		Return(nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			chRow, ok := row.(*chLogRow)
			if !ok {
				return false
			}
			return chRow.BlockNumber == log2.BlockNumber &&
				chRow.LogIndex == log2.LogIndex
		})).
		Return(nil).
		Once()

	mockBatch.
		On("Send").
		Return(nil).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)

	err = repo.BatchInsertLogs(ctx, []*LogRow{log1, log2})
	require.NoError(t, err)

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}

func TestLogsRepository_BatchInsertLogs_Empty(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)

	err = repo.BatchInsertLogs(ctx, nil)
	require.NoError(t, err)

	err = repo.BatchInsertLogs(ctx, []*LogRow{})
	require.NoError(t, err)

	mockConn.AssertExpectations(t)
}

func TestLogsRepository_BatchInsertLogs_SkipsNilLogs(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	log := createTestLog()

	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 &&
				containsSubstring(q, "INSERT INTO") &&
				containsSubstring(q, "`default`.`raw_logs`")
		})).
		Return(mockBatch, nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			chRow, ok := row.(*chLogRow)
			if !ok {
				return false
			}
			return chRow.BlockNumber == log.BlockNumber &&
				chRow.LogIndex == log.LogIndex
		})).
		Return(nil).
		Once()

	mockBatch.
		On("Send").
		Return(nil).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)

	err = repo.BatchInsertLogs(ctx, []*LogRow{nil, log, nil})
	require.NoError(t, err)

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}

func TestLogsRepository_BatchInsertLogs_PrepareBatchError(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	prepareErr := errors.New("prepare batch failed")
	log := createTestLog()

	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.Anything).
		Return(nil, prepareErr).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)

	err = repo.BatchInsertLogs(ctx, []*LogRow{log})
	require.ErrorIs(t, err, prepareErr)
	assert.Contains(t, err.Error(), "failed to prepare batch")

	mockConn.AssertExpectations(t)
}

func TestLogsRepository_BatchInsertLogs_ConvertError(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	log := createTestLog()
	log.TxHash = "invalid-hash"

	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.Anything).
		Return(mockBatch, nil).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)

	err = repo.BatchInsertLogs(ctx, []*LogRow{log})
	assert.Contains(t, err.Error(), "failed to convert log row")
	assert.Contains(t, err.Error(), log.TxHash)

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}

func TestLogsRepository_BatchInsertLogs_AppendStructError(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	appendErr := errors.New("append failed")
	log := createTestLog()

	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.Anything).
		Return(mockBatch, nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			_, ok := row.(*chLogRow)
			return ok
		})).
		Return(appendErr).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)

	err = repo.BatchInsertLogs(ctx, []*LogRow{log})
	require.ErrorIs(t, err, appendErr)
	assert.Contains(t, err.Error(), "failed to append log")
	assert.Contains(t, err.Error(), log.TxHash)

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}

func TestLogsRepository_BatchInsertLogs_SendError(t *testing.T) {
	t.Parallel()

	mockConn := &testutils.MockConn{}
	mockBatch := &testutils.MockBatch{}
	ctx := t.Context()

	sendErr := errors.New("send failed")
	log := createTestLog()

	expectTableInit(mockConn, "raw_logs_local", "raw_logs")

	mockConn.
		On("PrepareBatch", mock.Anything, mock.Anything).
		Return(mockBatch, nil).
		Once()

	mockBatch.
		On("AppendStruct", mock.MatchedBy(func(row interface{}) bool {
			_, ok := row.(*chLogRow)
			return ok
		})).
		Return(nil).
		Once()

	mockBatch.
		On("Send").
		Return(sendErr).
		Once()

	repo, err := NewLogs(ctx, testutils.NewTestClient(mockConn), "default", "default", "raw_logs")
	require.NoError(t, err)

	err = repo.BatchInsertLogs(ctx, []*LogRow{log})
	require.ErrorIs(t, err, sendErr)
	assert.Contains(t, err.Error(), "failed to send batch")

	mockConn.AssertExpectations(t)
	mockBatch.AssertExpectations(t)
}
