package icmrepo

import (
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
)

func TestReceiveEvents_WriteReceiveEvent_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "receive_events_local", "receive_events")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`receive_events`")
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
			addr1,
			addr2,
			"1",
			addr1,
			testDstBlockchainID,
			addr2,
			"100000",
			[]string{},
			"test-data",
			[]string{},
			[]string{},
		).
		Return(nil).
		Once()

	repo, err := NewReceiveEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "receive_events")
	require.NoError(t, err)
	err = repo.WriteReceiveEvent(ctx, &ReceiveEventRow{
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
		DelivererAddress:         testAddr1Hex,
		RewardRedeemerAddress:    testAddr2Hex,
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

func TestReceiveEvents_WriteReceiveEvent_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "receive_events_local", "receive_events")
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
			addr1,
			addr2,
			"1",
			addr1,
			testDstBlockchainID,
			addr2,
			"100000",
			[]string{},
			"test-data",
			[]string{},
			[]string{},
		).
		Return(execErr).
		Once()

	repo, err := NewReceiveEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "receive_events")
	require.NoError(t, err)
	err = repo.WriteReceiveEvent(ctx, &ReceiveEventRow{
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
		DelivererAddress:         testAddr1Hex,
		RewardRedeemerAddress:    testAddr2Hex,
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

func TestReceiveEvents_DeleteReceiveEvents_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)

	expectICMTableInit(mockConn, "receive_events_local", "receive_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `default`.`receive_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(nil).
		Once()

	repo, err := NewReceiveEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "receive_events")
	require.NoError(t, err)
	err = repo.DeleteReceiveEvents(ctx, chainID)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestReceiveEvents_DeleteReceiveEvents_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)
	deleteErr := errors.New("delete failed")

	expectICMTableInit(mockConn, "receive_events_local", "receive_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `default`.`receive_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(deleteErr).
		Once()

	repo, err := NewReceiveEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "receive_events")
	require.NoError(t, err)
	err = repo.DeleteReceiveEvents(ctx, chainID)
	require.ErrorIs(t, err, deleteErr)
	mockConn.AssertExpectations(t)
}
