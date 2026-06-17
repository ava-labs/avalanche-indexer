package icmrepo

import (
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
)

func TestFeeInfoEvents_WriteFeeInfoEvent_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)

	expectICMTableInit(mockConn, "icm_fee_info_events_local", "icm_fee_info_events")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`default`.`icm_fee_info_events`")
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
			"500000",
		).
		Return(nil).
		Once()

	repo, err := NewFeeInfoEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_fee_info_events")
	require.NoError(t, err)
	err = repo.WriteFeeInfoEvent(ctx, &FeeInfoEventRow{
		BlockchainID:            testBlockchainID,
		EVMChainID:              big.NewInt(43114),
		BlockNumber:             100,
		BlockTime:               testBlockTime,
		TxHash:                  testTxHashHex,
		TxIndex:                 0,
		LogIndex:                1,
		ContractAddress:         testContractAddrHex,
		MessageID:               testMessageIDHex,
		DestinationBlockchainID: testDstBlockchainID,
		FeeTokenAddress:         testAddr1Hex,
		AdditionalFeeAmount:     big.NewInt(500000),
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestFeeInfoEvents_WriteFeeInfoEvent_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	txHash := mustFixed32(t, testTxHashHex)
	msgID := mustFixed32(t, testMessageIDHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)

	expectICMTableInit(mockConn, "icm_fee_info_events_local", "icm_fee_info_events")
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
			"500000",
		).
		Return(execErr).
		Once()

	repo, err := NewFeeInfoEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_fee_info_events")
	require.NoError(t, err)
	err = repo.WriteFeeInfoEvent(ctx, &FeeInfoEventRow{
		BlockchainID:            testBlockchainID,
		EVMChainID:              big.NewInt(43114),
		BlockNumber:             100,
		BlockTime:               testBlockTime,
		TxHash:                  testTxHashHex,
		TxIndex:                 0,
		LogIndex:                1,
		ContractAddress:         testContractAddrHex,
		MessageID:               testMessageIDHex,
		DestinationBlockchainID: testDstBlockchainID,
		FeeTokenAddress:         testAddr1Hex,
		AdditionalFeeAmount:     big.NewInt(500000),
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

func TestFeeInfoEvents_DeleteFeeInfoEvents_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)

	expectICMTableInit(mockConn, "icm_fee_info_events_local", "icm_fee_info_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `default`.`icm_fee_info_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(nil).
		Once()

	repo, err := NewFeeInfoEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_fee_info_events")
	require.NoError(t, err)
	err = repo.DeleteFeeInfoEvents(ctx, chainID)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestFeeInfoEvents_DeleteFeeInfoEvents_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)
	deleteErr := errors.New("delete failed")

	expectICMTableInit(mockConn, "icm_fee_info_events_local", "icm_fee_info_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `default`.`icm_fee_info_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(deleteErr).
		Once()

	repo, err := NewFeeInfoEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_fee_info_events")
	require.NoError(t, err)
	err = repo.DeleteFeeInfoEvents(ctx, chainID)
	require.ErrorIs(t, err, deleteErr)
	mockConn.AssertExpectations(t)
}
