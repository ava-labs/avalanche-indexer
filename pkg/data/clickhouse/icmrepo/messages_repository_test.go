package icmrepo

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// Shared test constants used across all icmrepo test files.
const (
	testCluster         = "default"
	testDatabase        = "default"
	testBlockchainID    = "11111111111111111111111111111111LpoYY"
	testDstBlockchainID = "2oYMBNV4eNHyqk2fjjV5nVQLDbtmNJzq5s3qs3Lo6ftnC6FByM"

	testTxHashHex       = "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	testMessageIDHex    = "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40"
	testContractAddrHex = "0x4142434445464748494a4b4c4d4e4f5051525354"
	testAddr1Hex        = "0x55565758595a5b5c5d5e5f606162636465666768"
	testAddr2Hex        = "0x696a6b6c6d6e6f707172737475767778797a7b7c"
)

var testBlockTime = time.Date(2023, 11, 14, 0, 0, 0, 0, time.UTC)

// expectICMTableInit registers mock expectations for ICM table initialization:
// one Exec for the local CREATE TABLE and one for the distributed CREATE TABLE.
func expectICMTableInit(mockConn *testutils.MockConn, localTable, distributedTable string) {
	localPrefix := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`", testDatabase, localTable)
	distributedPrefix := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`", testDatabase, distributedTable)

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return strings.HasPrefix(q, localPrefix)
		})).
		Return(nil).
		Once()
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return strings.HasPrefix(q, distributedPrefix)
		})).
		Return(nil).
		Once()
}

func containsSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}

// binaryFixedString helpers used in tests to compute expected binary values.
func mustFixed32(t *testing.T, hex string) string {
	t.Helper()
	b, err := utils.HexToBytes32(hex)
	require.NoError(t, err)
	return string(b[:])
}

func mustFixed20(t *testing.T, hex string) string {
	t.Helper()
	b, err := utils.HexToBytes20(hex)
	require.NoError(t, err)
	return string(b[:])
}

func strPtr(s string) *string { return &s }

// -- nil-row guards --

func TestMessages_WritePartialSend_NilRow(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	require.ErrorIs(t, repo.WritePartialSend(ctx, nil), errNilRow)
	mockConn.AssertExpectations(t)
}

func TestMessages_WritePartialReceive_NilRow(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	require.ErrorIs(t, repo.WritePartialReceive(ctx, nil), errNilRow)
	mockConn.AssertExpectations(t)
}

func TestMessages_WritePartialExecuted_NilRow(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	require.ErrorIs(t, repo.WritePartialExecuted(ctx, nil), errNilRow)
	mockConn.AssertExpectations(t)
}

func TestMessages_WritePartialExecutionFailed_NilRow(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	require.ErrorIs(t, repo.WritePartialExecutionFailed(ctx, nil), errNilRow)
	mockConn.AssertExpectations(t)
}

func TestMessages_WritePartialReceipt_NilRow(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	require.ErrorIs(t, repo.WritePartialReceipt(ctx, nil), errNilRow)
	mockConn.AssertExpectations(t)
}

// -- WritePartialSend --

func TestMessages_WritePartialSend_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	msgID := mustFixed32(t, testMessageIDHex)
	txHash := mustFixed32(t, testTxHashHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`icm_messages`")
		}),
			testBlockchainID, testDstBlockchainID,
			msgID, testBlockTime, txHash,
			"43114", contract,
			"1", addr1, addr2,
			"100000", []string{}, addr1,
			"500000", "test-data", (*string)(nil), "",
		).
		Return(nil).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialSend(ctx, &MessagePartialSendRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		SourceBlockTime:         testBlockTime,
		SourceTxHash:            testTxHashHex,
		EVMChainID:              big.NewInt(43114),
		ContractAddress:         testContractAddrHex,
		MessageNonce:            big.NewInt(1),
		SenderAddress:           testAddr1Hex,
		DestinationAddress:      testAddr2Hex,
		RequiredGasLimit:        big.NewInt(100000),
		AllowedRelayerAddresses: nil,
		FeeTokenAddress:         testAddr1Hex,
		FeeAmount:               big.NewInt(500000),
		MessageData:             "test-data",
		SourceGasSpent:          nil,
		MessageReceipts:         "",
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestMessages_WritePartialSend_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	msgID := mustFixed32(t, testMessageIDHex)
	txHash := mustFixed32(t, testTxHashHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			testBlockchainID, testDstBlockchainID,
			msgID, testBlockTime, txHash,
			"43114", contract,
			"1", addr1, addr2,
			"100000", []string{}, addr1,
			"500000", "test-data", (*string)(nil), "",
		).
		Return(execErr).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialSend(ctx, &MessagePartialSendRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		SourceBlockTime:         testBlockTime,
		SourceTxHash:            testTxHashHex,
		EVMChainID:              big.NewInt(43114),
		ContractAddress:         testContractAddrHex,
		MessageNonce:            big.NewInt(1),
		SenderAddress:           testAddr1Hex,
		DestinationAddress:      testAddr2Hex,
		RequiredGasLimit:        big.NewInt(100000),
		AllowedRelayerAddresses: nil,
		FeeTokenAddress:         testAddr1Hex,
		FeeAmount:               big.NewInt(500000),
		MessageData:             "test-data",
		SourceGasSpent:          nil,
		MessageReceipts:         "",
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

// -- WritePartialReceive --

func TestMessages_WritePartialReceive_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	msgID := mustFixed32(t, testMessageIDHex)
	txHash := mustFixed32(t, testTxHashHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`icm_messages`")
		}),
			testBlockchainID, testDstBlockchainID,
			msgID, testBlockTime, txHash,
			addr1, addr2,
			strPtr("43114"), strPtr("200000"),
		).
		Return(nil).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialReceive(ctx, &MessagePartialReceiveRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		ReceiveBlockTime:        testBlockTime,
		ReceiveTxHash:           testTxHashHex,
		DelivererAddress:        testAddr1Hex,
		RewardRedeemerAddress:   testAddr2Hex,
		DestinationEVMChainID:   big.NewInt(43114),
		DestinationGasSpent:     big.NewInt(200000),
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestMessages_WritePartialReceive_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	msgID := mustFixed32(t, testMessageIDHex)
	txHash := mustFixed32(t, testTxHashHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			testBlockchainID, testDstBlockchainID,
			msgID, testBlockTime, txHash,
			addr1, addr2,
			strPtr("43114"), strPtr("200000"),
		).
		Return(execErr).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialReceive(ctx, &MessagePartialReceiveRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		ReceiveBlockTime:        testBlockTime,
		ReceiveTxHash:           testTxHashHex,
		DelivererAddress:        testAddr1Hex,
		RewardRedeemerAddress:   testAddr2Hex,
		DestinationEVMChainID:   big.NewInt(43114),
		DestinationGasSpent:     big.NewInt(200000),
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

// -- WritePartialExecuted --

func TestMessages_WritePartialExecuted_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	msgID := mustFixed32(t, testMessageIDHex)
	txHash := mustFixed32(t, testTxHashHex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`icm_messages`")
		}),
			testBlockchainID, testDstBlockchainID,
			msgID, testBlockTime, txHash,
		).
		Return(nil).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialExecuted(ctx, &MessagePartialExecutedRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		ExecutedBlockTime:       testBlockTime,
		ExecutedTxHash:          testTxHashHex,
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestMessages_WritePartialExecuted_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	msgID := mustFixed32(t, testMessageIDHex)
	txHash := mustFixed32(t, testTxHashHex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			testBlockchainID, testDstBlockchainID,
			msgID, testBlockTime, txHash,
		).
		Return(execErr).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialExecuted(ctx, &MessagePartialExecutedRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		ExecutedBlockTime:       testBlockTime,
		ExecutedTxHash:          testTxHashHex,
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

// -- WritePartialExecutionFailed --

func TestMessages_WritePartialExecutionFailed_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	msgID := mustFixed32(t, testMessageIDHex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`icm_messages`")
		}),
			testBlockchainID, testDstBlockchainID,
			msgID, testBlockTime,
		).
		Return(nil).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialExecutionFailed(ctx, &MessagePartialExecutionFailedRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		LastExecutionFailedTime: testBlockTime,
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestMessages_WritePartialExecutionFailed_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	msgID := mustFixed32(t, testMessageIDHex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			testBlockchainID, testDstBlockchainID,
			msgID, testBlockTime,
		).
		Return(execErr).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialExecutionFailed(ctx, &MessagePartialExecutionFailedRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		LastExecutionFailedTime: testBlockTime,
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

// -- WritePartialReceipt --

func TestMessages_WritePartialReceipt_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	msgID := mustFixed32(t, testMessageIDHex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`icm_messages`")
		}),
			testBlockchainID, testDstBlockchainID,
			msgID, uint8(1),
		).
		Return(nil).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialReceipt(ctx, &MessagePartialReceiptRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		ReceiptDelivered:        1,
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestMessages_WritePartialReceipt_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	msgID := mustFixed32(t, testMessageIDHex)

	expectICMTableInit(mockConn, "icm_messages_local", "icm_messages")
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			testBlockchainID, testDstBlockchainID,
			msgID, uint8(1),
		).
		Return(execErr).
		Once()

	repo, err := NewMessages(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_messages")
	require.NoError(t, err)
	err = repo.WritePartialReceipt(ctx, &MessagePartialReceiptRow{
		SourceBlockchainID:      testBlockchainID,
		DestinationBlockchainID: testDstBlockchainID,
		MessageID:               testMessageIDHex,
		ReceiptDelivered:        1,
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}
