package icmrepo

import (
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
)

func TestMessageExecutionFailedEvents_WriteMessageExecutionFailedEvent_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "icm_message_execution_failed_events_local", "icm_message_execution_failed_events")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`icm_message_execution_failed_events`")
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
			"1",
			addr1,
			testDstBlockchainID,
			addr2,
			"100000",
			[]string{},
			"test-data",
			[]*big.Int{},
			[]string{},
		).
		Return(nil).
		Once()

	repo, err := NewMessageExecutionFailedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_message_execution_failed_events")
	require.NoError(t, err)
	err = repo.WriteMessageExecutionFailedEvent(ctx, &MessageExecutionFailedEventRow{
		BlockchainID:             testBlockchainID,
		EVMChainID:               big.NewInt(43114),
		BlockNumber:              100,
		BlockTime:                testBlockTime,
		TxHash:                   testTxHashHex,
		TxIndex:                  0,
		LogIndex:                 1,
		ContractAddress:          testContractAddrHex,
		MessageID:                testMessageIDHex,
		SourceBlockchainID:       testDstBlockchainID,
		MessageNonce:             big.NewInt(1),
		OriginSenderAddress:      testAddr1Hex,
		DestinationBlockchainID:  testDstBlockchainID,
		DestinationAddress:       testAddr2Hex,
		RequiredGasLimit:         big.NewInt(100000),
		AllowedRelayerAddresses:  nil,
		MessageData:              []byte("test-data"),
		ReceiptsMessageNonces:    nil,
		ReceiptsRelayerAddresses: nil,
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestMessageExecutionFailedEvents_WriteMessageExecutionFailedEvent_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "icm_message_execution_failed_events_local", "icm_message_execution_failed_events")
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
			"1",
			addr1,
			testDstBlockchainID,
			addr2,
			"100000",
			[]string{},
			"test-data",
			[]*big.Int{},
			[]string{},
		).
		Return(execErr).
		Once()

	repo, err := NewMessageExecutionFailedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_message_execution_failed_events")
	require.NoError(t, err)
	err = repo.WriteMessageExecutionFailedEvent(ctx, &MessageExecutionFailedEventRow{
		BlockchainID:             testBlockchainID,
		EVMChainID:               big.NewInt(43114),
		BlockNumber:              100,
		BlockTime:                testBlockTime,
		TxHash:                   testTxHashHex,
		TxIndex:                  0,
		LogIndex:                 1,
		ContractAddress:          testContractAddrHex,
		MessageID:                testMessageIDHex,
		SourceBlockchainID:       testDstBlockchainID,
		MessageNonce:             big.NewInt(1),
		OriginSenderAddress:      testAddr1Hex,
		DestinationBlockchainID:  testDstBlockchainID,
		DestinationAddress:       testAddr2Hex,
		RequiredGasLimit:         big.NewInt(100000),
		AllowedRelayerAddresses:  nil,
		MessageData:              []byte("test-data"),
		ReceiptsMessageNonces:    nil,
		ReceiptsRelayerAddresses: nil,
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

func TestMessageExecutionFailedEvents_DeleteMessageExecutionFailedEvents_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)

	expectICMTableInit(mockConn, "icm_message_execution_failed_events_local", "icm_message_execution_failed_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `default`.`icm_message_execution_failed_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(nil).
		Once()

	repo, err := NewMessageExecutionFailedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_message_execution_failed_events")
	require.NoError(t, err)
	err = repo.DeleteMessageExecutionFailedEvents(ctx, chainID)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestMessageExecutionFailedEvents_DeleteMessageExecutionFailedEvents_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)
	deleteErr := errors.New("delete failed")

	expectICMTableInit(mockConn, "icm_message_execution_failed_events_local", "icm_message_execution_failed_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `default`.`icm_message_execution_failed_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(deleteErr).
		Once()

	repo, err := NewMessageExecutionFailedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_message_execution_failed_events")
	require.NoError(t, err)
	err = repo.DeleteMessageExecutionFailedEvents(ctx, chainID)
	require.ErrorIs(t, err, deleteErr)
	mockConn.AssertExpectations(t)
}
