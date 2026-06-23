package icmrepo

import (
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
)

func TestSendEvents_WriteSendEvent_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "send_events_local", "send_events")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`icm`.`send_events`")
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
			"100000",
			[]string{},
			addr1,
			"500000",
			"1",
			"test-data",
			[]string{},
			[]string{},
		).
		Return(nil).
		Once()

	repo, err := NewSendEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "send_events")
	require.NoError(t, err)
	err = repo.WriteSendEvent(ctx, &SendEventRow{
		BlockchainID:             testBlockchainID,
		EVMChainID:               big.NewInt(43114),
		BlockNumber:              100,
		BlockTime:                testBlockTime,
		TxHash:                   testTxHashHex,
		TxIndex:                  0,
		LogIndex:                 1,
		ContractAddress:          testContractAddrHex,
		MessageID:                testMessageIDHex,
		DestinationBlockchainID:  testDstBlockchainID,
		SenderAddress:            testAddr1Hex,
		DestinationAddress:       testAddr2Hex,
		RequiredGasLimit:         big.NewInt(100000),
		AllowedRelayerAddresses:  nil,
		FeeTokenAddress:          testAddr1Hex,
		FeeAmount:                big.NewInt(500000),
		MessageNonce:             big.NewInt(1),
		MessageData:              []byte("test-data"),
		ReceiptsMessageNonces:    nil,
		ReceiptsRelayerAddresses: nil,
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestSendEvents_WriteSendEvent_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)
	addr2 := mustFixed20(t, testAddr2Hex)

	expectICMTableInit(mockConn, "send_events_local", "send_events")
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
			"100000",
			[]string{},
			addr1,
			"500000",
			"1",
			"test-data",
			[]string{},
			[]string{},
		).
		Return(execErr).
		Once()

	repo, err := NewSendEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "send_events")
	require.NoError(t, err)
	err = repo.WriteSendEvent(ctx, &SendEventRow{
		BlockchainID:             testBlockchainID,
		EVMChainID:               big.NewInt(43114),
		BlockNumber:              100,
		BlockTime:                testBlockTime,
		TxHash:                   testTxHashHex,
		TxIndex:                  0,
		LogIndex:                 1,
		ContractAddress:          testContractAddrHex,
		MessageID:                testMessageIDHex,
		DestinationBlockchainID:  testDstBlockchainID,
		SenderAddress:            testAddr1Hex,
		DestinationAddress:       testAddr2Hex,
		RequiredGasLimit:         big.NewInt(100000),
		AllowedRelayerAddresses:  nil,
		FeeTokenAddress:          testAddr1Hex,
		FeeAmount:                big.NewInt(500000),
		MessageNonce:             big.NewInt(1),
		MessageData:              []byte("test-data"),
		ReceiptsMessageNonces:    nil,
		ReceiptsRelayerAddresses: nil,
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

func TestSendEvents_DeleteSendEvents_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)

	expectICMTableInit(mockConn, "send_events_local", "send_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `icm`.`send_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(nil).
		Once()

	repo, err := NewSendEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "send_events")
	require.NoError(t, err)
	err = repo.DeleteSendEvents(ctx, chainID)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestSendEvents_DeleteSendEvents_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)
	deleteErr := errors.New("delete failed")

	expectICMTableInit(mockConn, "send_events_local", "send_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `icm`.`send_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(deleteErr).
		Once()

	repo, err := NewSendEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "send_events")
	require.NoError(t, err)
	err = repo.DeleteSendEvents(ctx, chainID)
	require.ErrorIs(t, err, deleteErr)
	mockConn.AssertExpectations(t)
}
