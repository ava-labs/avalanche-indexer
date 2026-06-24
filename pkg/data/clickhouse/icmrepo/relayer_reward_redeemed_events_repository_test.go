package icmrepo

import (
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
)

func TestRelayerRewardRedeemedEvents_WriteRelayerRewardRedeemedEvent_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	txHash := mustFixed32(t, testTxHashHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)

	expectICMTableInit(mockConn, "icm_relayer_reward_redeemed_events_local", "icm_relayer_reward_redeemed_events")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return containsSubstring(q, "INSERT INTO") && containsSubstring(q, "`icm`.`icm_relayer_reward_redeemed_events`")
		}),
			testBlockchainID,
			"43114",
			uint64(100),
			testBlockTime,
			txHash,
			uint32(0),
			uint32(1),
			contract,
			addr1,
			addr1,
			"500000",
		).
		Return(nil).
		Once()

	repo, err := NewRelayerRewardRedeemedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_relayer_reward_redeemed_events")
	require.NoError(t, err)
	err = repo.WriteRelayerRewardRedeemedEvent(ctx, &RelayerRewardRedeemedEventRow{
		BlockchainID:    testBlockchainID,
		EVMChainID:      big.NewInt(43114),
		BlockNumber:     100,
		BlockTime:       testBlockTime,
		TxHash:          testTxHashHex,
		TxIndex:         0,
		LogIndex:        1,
		ContractAddress: testContractAddrHex,
		RedeemerAddress: testAddr1Hex,
		FeeTokenAddress: testAddr1Hex,
		Amount:          big.NewInt(500000),
	})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRelayerRewardRedeemedEvents_WriteRelayerRewardRedeemedEvent_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	execErr := errors.New("exec failed")

	txHash := mustFixed32(t, testTxHashHex)
	contract := mustFixed20(t, testContractAddrHex)
	addr1 := mustFixed20(t, testAddr1Hex)

	expectICMTableInit(mockConn, "icm_relayer_reward_redeemed_events_local", "icm_relayer_reward_redeemed_events")
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
			addr1,
			addr1,
			"500000",
		).
		Return(execErr).
		Once()

	repo, err := NewRelayerRewardRedeemedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_relayer_reward_redeemed_events")
	require.NoError(t, err)
	err = repo.WriteRelayerRewardRedeemedEvent(ctx, &RelayerRewardRedeemedEventRow{
		BlockchainID:    testBlockchainID,
		EVMChainID:      big.NewInt(43114),
		BlockNumber:     100,
		BlockTime:       testBlockTime,
		TxHash:          testTxHashHex,
		TxIndex:         0,
		LogIndex:        1,
		ContractAddress: testContractAddrHex,
		RedeemerAddress: testAddr1Hex,
		FeeTokenAddress: testAddr1Hex,
		Amount:          big.NewInt(500000),
	})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

func TestRelayerRewardRedeemedEvents_DeleteRelayerRewardRedeemedEvents_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)

	expectICMTableInit(mockConn, "icm_relayer_reward_redeemed_events_local", "icm_relayer_reward_redeemed_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `icm`.`icm_relayer_reward_redeemed_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(nil).
		Once()

	repo, err := NewRelayerRewardRedeemedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_relayer_reward_redeemed_events")
	require.NoError(t, err)
	err = repo.DeleteRelayerRewardRedeemedEvents(ctx, chainID)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRelayerRewardRedeemedEvents_DeleteRelayerRewardRedeemedEvents_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	chainID := uint64(43114)
	deleteErr := errors.New("delete failed")

	expectICMTableInit(mockConn, "icm_relayer_reward_redeemed_events_local", "icm_relayer_reward_redeemed_events")
	mockConn.
		On("Exec", mock.Anything,
			"DELETE FROM `icm`.`icm_relayer_reward_redeemed_events_local` ON CLUSTER 'default' WHERE evm_chain_id = ?\n",
			chainID,
		).
		Return(deleteErr).
		Once()

	repo, err := NewRelayerRewardRedeemedEvents(ctx, testutils.NewTestClient(mockConn), testCluster, testDatabase, "icm_relayer_reward_redeemed_events")
	require.NoError(t, err)
	err = repo.DeleteRelayerRewardRedeemedEvents(ctx, chainID)
	require.ErrorIs(t, err, deleteErr)
	mockConn.AssertExpectations(t)
}
