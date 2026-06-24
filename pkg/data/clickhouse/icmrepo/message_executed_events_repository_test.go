package icmrepo

import (
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
)

func TestMessageExecutedEvents_WriteMessageExecutedEvent_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)

	expectICMTableInit(mockConn, "icm_message_executed_events_local", "icm_message_executed_events")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`icm`.`message_executed_events`")
		}),
			testBlockchainID,
			"43114",
			uint64(100),
			testBlockTime,
			txHash,
			uint32(0),
			uint32(1),
			contract,
			msgID,
			testDstBlockchainID,
		).
		Return(nil).
		Once()

	repo, err := NewMessageExecutedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_message_executed_events")
	require.NoError(t, err)
	err = repo.WriteMessageExecutedEvent(ctx, &MessageExecutedEventRow{
		BlockchainID:       testBlockchainID,
		EVMChainID:         big.NewInt(43114),
		BlockNumber:        100,
		BlockTime:          testBlockTime,
		TxHash:             testTxHashHex,
		TxIndex:            0,
		LogIndex:           1,
		ContractAddress:    testContractAddrHex,
		MessageID:          testMessageIDHex,
		SourceBlockchainID: testDstBlockchainID,
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestMessageExecutedEvents_WriteMessageExecutedEvent_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)

	expectICMTableInit(mockConn, "icm_message_executed_events_local", "icm_message_executed_events")
	mockConn.
		On("Exec", mock.Anything, mock.Anything,
			testBlockchainID,
			"43114",
			uint64(100),
			testBlockTime,
			txHash,
			uint32(0),
			uint32(1),
			contract,
			msgID,
			testDstBlockchainID,
		).
		Return(execErr).
		Once()

	repo, err := NewMessageExecutedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_message_executed_events")
	require.NoError(t, err)
	err = repo.WriteMessageExecutedEvent(ctx, &MessageExecutedEventRow{
		BlockchainID:       testBlockchainID,
		EVMChainID:         big.NewInt(43114),
		BlockNumber:        100,
		BlockTime:          testBlockTime,
		TxHash:             testTxHashHex,
		TxIndex:            0,
		LogIndex:           1,
		ContractAddress:    testContractAddrHex,
		MessageID:          testMessageIDHex,
		SourceBlockchainID: testDstBlockchainID,
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

func TestMessageExecutedEvents_DeleteMessageExecutedEvents_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)

	expectICMTableInit(mockConn, "icm_message_executed_events_local", "icm_message_executed_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `icm`.`message_executed_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(nil).
		Once()

	repo, err := NewMessageExecutedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_message_executed_events")
	require.NoError(t, err)
	err = repo.DeleteMessageExecutedEvents(ctx, chainID)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestMessageExecutedEvents_DeleteMessageExecutedEvents_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)
	deleteErr := errors.New("delete failed")

	expectICMTableInit(mockConn, "icm_message_executed_events_local", "icm_message_executed_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `icm`.`message_executed_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(deleteErr).
		Once()

	repo, err := NewMessageExecutedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_message_executed_events")
	require.NoError(t, err)
	err = repo.DeleteMessageExecutedEvents(ctx, chainID)
	require.ErrorIs(t, err, deleteErr)
	mockConn.AssertExpectations(t)
}
