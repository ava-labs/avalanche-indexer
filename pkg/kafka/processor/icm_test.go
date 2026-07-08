package processor

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/icmrepo"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	teleportermessenger "github.com/ava-labs/icm-contracts/abi-bindings/go/teleporter/TeleporterMessenger"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ============================================================================
// Test constants
// ============================================================================

const icmTestContractHex = "0xaAbBcCdDeEfF001122334455667788990011aAbB"

var (
	icmContractAddr = common.HexToAddress(icmTestContractHex)
	icmMsgID        = [32]byte{0x01}
	icmSrcChainID   = [32]byte{0x02}
	icmDstChainID   = [32]byte{0x03}
	icmRelayerAddr  = common.HexToAddress("0x1111111111111111111111111111111111111111")
	icmAssetAddr    = common.HexToAddress("0x2222222222222222222222222222222222222222")
	icmDeliverer    = common.HexToAddress("0x3333333333333333333333333333333333333333")
	icmRedeemer     = common.HexToAddress("0x4444444444444444444444444444444444444444")
)

// ============================================================================
// Mock repos
// ============================================================================

type mockICMMessagesRepo struct {
	writePartialSendFunc            func(context.Context, *icmrepo.MessagePartialSendRow) error
	writePartialReceiveFunc         func(context.Context, *icmrepo.MessagePartialReceiveRow) error
	writePartialExecutedFunc        func(context.Context, *icmrepo.MessagePartialExecutedRow) error
	writePartialExecutionFailedFunc func(context.Context, *icmrepo.MessagePartialExecutionFailedRow) error
	writePartialReceiptFunc         func(context.Context, *icmrepo.MessagePartialReceiptRow) error
}

func (*mockICMMessagesRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (m *mockICMMessagesRepo) WritePartialSend(ctx context.Context, row *icmrepo.MessagePartialSendRow) error {
	if m.writePartialSendFunc != nil {
		return m.writePartialSendFunc(ctx, row)
	}
	return nil
}

func (m *mockICMMessagesRepo) WritePartialReceive(ctx context.Context, row *icmrepo.MessagePartialReceiveRow) error {
	if m.writePartialReceiveFunc != nil {
		return m.writePartialReceiveFunc(ctx, row)
	}
	return nil
}

func (m *mockICMMessagesRepo) WritePartialExecuted(ctx context.Context, row *icmrepo.MessagePartialExecutedRow) error {
	if m.writePartialExecutedFunc != nil {
		return m.writePartialExecutedFunc(ctx, row)
	}
	return nil
}

func (m *mockICMMessagesRepo) WritePartialExecutionFailed(ctx context.Context, row *icmrepo.MessagePartialExecutionFailedRow) error {
	if m.writePartialExecutionFailedFunc != nil {
		return m.writePartialExecutionFailedFunc(ctx, row)
	}
	return nil
}

func (m *mockICMMessagesRepo) WritePartialReceipt(ctx context.Context, row *icmrepo.MessagePartialReceiptRow) error {
	if m.writePartialReceiptFunc != nil {
		return m.writePartialReceiptFunc(ctx, row)
	}
	return nil
}

type mockICMSendEventsRepo struct {
	writeSendEventFunc func(context.Context, *icmrepo.SendEventRow) error
}

func (*mockICMSendEventsRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (*mockICMSendEventsRepo) BatchInsertSendEvents(context.Context, []*icmrepo.SendEventRow) error {
	return nil
}
func (*mockICMSendEventsRepo) DeleteSendEvents(context.Context, uint64) error { return nil }
func (m *mockICMSendEventsRepo) WriteSendEvent(ctx context.Context, row *icmrepo.SendEventRow) error {
	if m.writeSendEventFunc != nil {
		return m.writeSendEventFunc(ctx, row)
	}
	return nil
}

type mockICMReceiveEventsRepo struct {
	writeReceiveEventFunc func(context.Context, *icmrepo.ReceiveEventRow) error
}

func (*mockICMReceiveEventsRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (*mockICMReceiveEventsRepo) BatchInsertReceiveEvents(context.Context, []*icmrepo.ReceiveEventRow) error {
	return nil
}
func (*mockICMReceiveEventsRepo) DeleteReceiveEvents(context.Context, uint64) error { return nil }
func (m *mockICMReceiveEventsRepo) WriteReceiveEvent(ctx context.Context, row *icmrepo.ReceiveEventRow) error {
	if m.writeReceiveEventFunc != nil {
		return m.writeReceiveEventFunc(ctx, row)
	}
	return nil
}

type mockICMMessageExecutedEventsRepo struct {
	writeMessageExecutedEventFunc func(context.Context, *icmrepo.MessageExecutedEventRow) error
}

func (*mockICMMessageExecutedEventsRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (*mockICMMessageExecutedEventsRepo) BatchInsertMessageExecutedEvents(context.Context, []*icmrepo.MessageExecutedEventRow) error {
	return nil
}

func (*mockICMMessageExecutedEventsRepo) DeleteMessageExecutedEvents(context.Context, uint64) error {
	return nil
}

func (m *mockICMMessageExecutedEventsRepo) WriteMessageExecutedEvent(ctx context.Context, row *icmrepo.MessageExecutedEventRow) error {
	if m.writeMessageExecutedEventFunc != nil {
		return m.writeMessageExecutedEventFunc(ctx, row)
	}
	return nil
}

type mockICMMessageExecutionFailedEventsRepo struct {
	writeMessageExecutionFailedEventFunc func(context.Context, *icmrepo.MessageExecutionFailedEventRow) error
}

func (*mockICMMessageExecutionFailedEventsRepo) CreateTableIfNotExists(context.Context) error {
	return nil
}

func (*mockICMMessageExecutionFailedEventsRepo) BatchInsertMessageExecutionFailedEvents(context.Context, []*icmrepo.MessageExecutionFailedEventRow) error {
	return nil
}

func (*mockICMMessageExecutionFailedEventsRepo) DeleteMessageExecutionFailedEvents(context.Context, uint64) error {
	return nil
}

func (m *mockICMMessageExecutionFailedEventsRepo) WriteMessageExecutionFailedEvent(ctx context.Context, row *icmrepo.MessageExecutionFailedEventRow) error {
	if m.writeMessageExecutionFailedEventFunc != nil {
		return m.writeMessageExecutionFailedEventFunc(ctx, row)
	}
	return nil
}

type mockICMReceiptsEventsRepo struct {
	writeReceiptsEventFunc func(context.Context, *icmrepo.ReceiptEventRow) error
}

func (*mockICMReceiptsEventsRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (*mockICMReceiptsEventsRepo) BatchInsertReceiptEvents(context.Context, []*icmrepo.ReceiptEventRow) error {
	return nil
}
func (*mockICMReceiptsEventsRepo) DeleteReceiptEvents(context.Context, uint64) error { return nil }
func (m *mockICMReceiptsEventsRepo) WriteReceiptEvent(ctx context.Context, row *icmrepo.ReceiptEventRow) error {
	if m.writeReceiptsEventFunc != nil {
		return m.writeReceiptsEventFunc(ctx, row)
	}
	return nil
}

type mockICMFeeInfoEventsRepo struct {
	writeFeeInfoEventFunc func(context.Context, *icmrepo.AddFeeEventRow) error
}

func (*mockICMFeeInfoEventsRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (*mockICMFeeInfoEventsRepo) BatchInsertAddFeeEvents(context.Context, []*icmrepo.AddFeeEventRow) error {
	return nil
}
func (*mockICMFeeInfoEventsRepo) DeleteAddFeeEvents(context.Context, uint64) error { return nil }
func (m *mockICMFeeInfoEventsRepo) WriteAddFeeEvent(ctx context.Context, row *icmrepo.AddFeeEventRow) error {
	if m.writeFeeInfoEventFunc != nil {
		return m.writeFeeInfoEventFunc(ctx, row)
	}
	return nil
}

type mockICMFeeRedemptionsEventsRepo struct {
	writeFeeRedemptionsEventFunc func(context.Context, *icmrepo.RelayerRewardRedeemedEventRow) error
}

func (*mockICMFeeRedemptionsEventsRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (*mockICMFeeRedemptionsEventsRepo) BatchInsertRelayerRewardRedeemedEvents(context.Context, []*icmrepo.RelayerRewardRedeemedEventRow) error {
	return nil
}

func (*mockICMFeeRedemptionsEventsRepo) DeleteRelayerRewardRedeemedEvents(context.Context, uint64) error {
	return nil
}

func (m *mockICMFeeRedemptionsEventsRepo) WriteRelayerRewardRedeemedEvent(ctx context.Context, row *icmrepo.RelayerRewardRedeemedEventRow) error {
	if m.writeFeeRedemptionsEventFunc != nil {
		return m.writeFeeRedemptionsEventFunc(ctx, row)
	}
	return nil
}

// ============================================================================
// Test fixture and helpers
// ============================================================================

type icmTestFixture struct {
	proc               *ICMProcessor
	messages           *mockICMMessagesRepo
	sendRepo           *mockICMSendEventsRepo
	receiveRepo        *mockICMReceiveEventsRepo
	executedRepo       *mockICMMessageExecutedEventsRepo
	execFailedRepo     *mockICMMessageExecutionFailedEventsRepo
	receiptsRepo       *mockICMReceiptsEventsRepo
	feeInfoRepo        *mockICMFeeInfoEventsRepo
	feeRedemptionsRepo *mockICMFeeRedemptionsEventsRepo
}

func newICMTestFixture(t *testing.T) *icmTestFixture {
	t.Helper()
	f := &icmTestFixture{
		messages:           &mockICMMessagesRepo{},
		sendRepo:           &mockICMSendEventsRepo{},
		receiveRepo:        &mockICMReceiveEventsRepo{},
		executedRepo:       &mockICMMessageExecutedEventsRepo{},
		execFailedRepo:     &mockICMMessageExecutionFailedEventsRepo{},
		receiptsRepo:       &mockICMReceiptsEventsRepo{},
		feeInfoRepo:        &mockICMFeeInfoEventsRepo{},
		feeRedemptionsRepo: &mockICMFeeRedemptionsEventsRepo{},
	}
	proc, err := NewICMProcessor(
		zap.NewNop().Sugar(),
		f.messages,
		f.sendRepo,
		f.receiveRepo,
		f.executedRepo,
		f.execFailedRepo,
		f.receiptsRepo,
		f.feeInfoRepo,
		f.feeRedemptionsRepo,
		[]string{icmTestContractHex},
		nil,
	)
	require.NoError(t, err)
	f.proc = proc
	return f
}

// icmEventData ABI-packs the non-indexed arguments for the named Teleporter event.
// Returns nil for events with no non-indexed args (e.g. MessageExecuted).
func icmEventData(t *testing.T, eventName string, args ...interface{}) []byte {
	t.Helper()
	parsedABI, err := teleportermessenger.TeleporterMessengerMetaData.GetAbi()
	require.NoError(t, err)
	event, ok := parsedABI.Events[eventName]
	require.True(t, ok, "event %q not found in ABI", eventName)
	nonIndexed := event.Inputs.NonIndexed()
	if len(nonIndexed) == 0 {
		return nil
	}
	data, err := nonIndexed.Pack(args...)
	require.NoError(t, err)
	return data
}

// icmBuildBlock wraps a log in a complete EVMBlock with a single transaction.
func icmBuildBlock(l *kafkamsg.EVMLog) *kafkamsg.EVMBlock {
	blockchainID := testBlockchainID
	return &kafkamsg.EVMBlock{
		BlockchainID: &blockchainID,
		EVMChainID:   big.NewInt(43114),
		Number:       big.NewInt(100),
		Timestamp:    1_700_000_000,
		Transactions: []*kafkamsg.EVMTransaction{
			{
				Hash:     "0x1111111111111111111111111111111111111111111111111111111111111111",
				GasPrice: big.NewInt(25_000_000_000),
				Receipt: &kafkamsg.EVMTxReceipt{
					GasUsed:           100_000,
					EffectiveGasPrice: big.NewInt(30_000_000_000),
					Logs:              []*kafkamsg.EVMLog{l},
				},
			},
		},
	}
}

// icmKafkaMsg JSON-encodes a block and wraps it in a Kafka message.
func icmKafkaMsg(t *testing.T, block *kafkamsg.EVMBlock) *ckafka.Message {
	t.Helper()
	b, err := json.Marshal(block)
	require.NoError(t, err)
	return &ckafka.Message{Value: b}
}

// icmEVMLog creates a kafkamsg.EVMLog with the given address, topics, and data.
func icmEVMLog(addr common.Address, topics []common.Hash, data []byte) *kafkamsg.EVMLog {
	return &kafkamsg.EVMLog{
		Address: addr,
		Topics:  topics,
		Data:    data,
	}
}

// icmMinMsg returns a minimal TeleporterMessage for constructing test logs.
func icmMinMsg() teleportermessenger.TeleporterMessage {
	return teleportermessenger.TeleporterMessage{
		MessageNonce:            big.NewInt(1),
		OriginSenderAddress:     icmRelayerAddr,
		DestinationBlockchainID: icmDstChainID,
		DestinationAddress:      icmAssetAddr,
		RequiredGasLimit:        big.NewInt(100_000),
		AllowedRelayerAddresses: []common.Address{},
		Receipts:                []teleportermessenger.TeleporterMessageReceipt{},
		Message:                 []byte("hello"),
	}
}

// icmMinFeeInfo returns a minimal TeleporterFeeInfo for constructing test logs.
func icmMinFeeInfo() teleportermessenger.TeleporterFeeInfo {
	return teleportermessenger.TeleporterFeeInfo{
		FeeTokenAddress: icmRelayerAddr,
		Amount:          big.NewInt(1_000),
	}
}

// ============================================================================
// Constructor tests
// ============================================================================

func TestNewICMProcessor_EmptyAddrs(t *testing.T) {
	t.Parallel()
	_, err := NewICMProcessor(
		zap.NewNop().Sugar(),
		&mockICMMessagesRepo{},
		&mockICMSendEventsRepo{},
		&mockICMReceiveEventsRepo{},
		&mockICMMessageExecutedEventsRepo{},
		&mockICMMessageExecutionFailedEventsRepo{},
		&mockICMReceiptsEventsRepo{},
		&mockICMFeeInfoEventsRepo{},
		&mockICMFeeRedemptionsEventsRepo{},
		[]string{},
		nil,
	)
	require.ErrorIs(t, err, ErrNoContractAddresses)
}

func TestNewICMProcessor_InvalidHexAddress(t *testing.T) {
	t.Parallel()
	_, err := NewICMProcessor(
		zap.NewNop().Sugar(),
		&mockICMMessagesRepo{},
		&mockICMSendEventsRepo{},
		&mockICMReceiveEventsRepo{},
		&mockICMMessageExecutedEventsRepo{},
		&mockICMMessageExecutionFailedEventsRepo{},
		&mockICMReceiptsEventsRepo{},
		&mockICMFeeInfoEventsRepo{},
		&mockICMFeeRedemptionsEventsRepo{},
		[]string{"not-a-hex-address"},
		nil,
	)
	require.ErrorIs(t, err, ErrInvalidContractAddress)
}

// ============================================================================
// Process dispatch tests
// ============================================================================

func TestICMProcessor_Process_NilMessage(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	err := f.proc.Process(t.Context(), nil)
	require.ErrorIs(t, err, ErrNilMessage)
	assert.True(t, IsNonRetryable(err), "nil message should be NonRetryable")
}

func TestICMProcessor_Process_NilValue(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	err := f.proc.Process(t.Context(), &ckafka.Message{Value: nil})
	require.ErrorIs(t, err, ErrNilMessage)
	assert.True(t, IsNonRetryable(err), "nil value should be NonRetryable")
}

func TestICMProcessor_Process_InvalidJSON(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	err := f.proc.Process(t.Context(), &ckafka.Message{Value: []byte("not-json")})
	require.ErrorIs(t, err, ErrUnmarshalBlock)
	assert.True(t, IsNonRetryable(err), "invalid JSON should be NonRetryable")
}

func TestICMProcessor_Process_NilBlockchainID(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	block := &kafkamsg.EVMBlock{
		BlockchainID: nil,
		EVMChainID:   big.NewInt(43114),
	}
	msg := icmKafkaMsg(t, block)
	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, ErrBlockchainIDRequired)
	assert.True(t, IsNonRetryable(err), "nil blockchainID should be NonRetryable")
}

func TestICMProcessor_Process_AddressNotInSet(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var sendCalled bool
	f.sendRepo.writeSendEventFunc = func(_ context.Context, _ *icmrepo.SendEventRow) error {
		sendCalled = true
		return nil
	}

	// Log from an address NOT in the contract set.
	unknownAddr := common.HexToAddress("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead")
	l := icmEVMLog(unknownAddr, []common.Hash{
		common.HexToHash(eventSigSendCrossChainMessage),
		common.BytesToHash(icmMsgID[:]),
		common.BytesToHash(icmDstChainID[:]),
	}, nil)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.NoError(t, err)
	assert.False(t, sendCalled, "log from unknown address should be skipped")
}

func TestICMProcessor_Process_UnknownTopic0(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var sendCalled bool
	f.sendRepo.writeSendEventFunc = func(_ context.Context, _ *icmrepo.SendEventRow) error {
		sendCalled = true
		return nil
	}

	// Log from the correct address but with an unknown topic0.
	unknownTopic := common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	l := icmEVMLog(icmContractAddr, []common.Hash{unknownTopic}, nil)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.NoError(t, err)
	assert.False(t, sendCalled, "log with unknown topic0 should be skipped")
}

func TestICMProcessor_Process_NilReceipt(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	blockchainID := testBlockchainID
	block := &kafkamsg.EVMBlock{
		BlockchainID: &blockchainID,
		EVMChainID:   big.NewInt(43114),
		Number:       big.NewInt(100),
		Timestamp:    1_700_000_000,
		Transactions: []*kafkamsg.EVMTransaction{
			{Hash: "0xaaaa", Receipt: nil},
		},
	}
	msg := icmKafkaMsg(t, block)

	// Should skip the tx gracefully and not panic.
	require.NoError(t, f.proc.Process(t.Context(), msg))
}

func TestICMProcessor_Process_NilLogElement(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var sendCalled bool
	f.sendRepo.writeSendEventFunc = func(_ context.Context, _ *icmrepo.SendEventRow) error {
		sendCalled = true
		return nil
	}

	// A receipt whose Logs slice contains a nil pointer followed by a real log.
	// json.Unmarshal of [null, {...}] into []*EVMLog produces exactly this.
	blockchainID := testBlockchainID
	block := &kafkamsg.EVMBlock{
		BlockchainID: &blockchainID,
		EVMChainID:   big.NewInt(43114),
		Number:       big.NewInt(100),
		Timestamp:    1_700_000_000,
		Transactions: []*kafkamsg.EVMTransaction{{
			Hash:     "0x1111",
			GasPrice: big.NewInt(25_000_000_000),
			Receipt: &kafkamsg.EVMTxReceipt{
				GasUsed: 100_000,
				Logs:    []*kafkamsg.EVMLog{nil},
			},
		}},
	}
	msg := icmKafkaMsg(t, block)

	// Should skip nil logs gracefully without panicking.
	require.NoError(t, f.proc.Process(t.Context(), msg))
	assert.False(t, sendCalled, "nil log element should be skipped without dispatching")
}

func TestICMProcessor_Process_EmptyTopics(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var sendCalled bool
	f.sendRepo.writeSendEventFunc = func(_ context.Context, _ *icmrepo.SendEventRow) error {
		sendCalled = true
		return nil
	}

	// Log from the correct address but with no topics.
	l := icmEVMLog(icmContractAddr, nil, nil)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.NoError(t, err)
	assert.False(t, sendCalled, "log with no topics should be skipped")
}

// ============================================================================
// handleSend tests
// ============================================================================

func sendTopics() []common.Hash {
	return []common.Hash{
		common.HexToHash(eventSigSendCrossChainMessage),
		common.BytesToHash(icmMsgID[:]),
		common.BytesToHash(icmDstChainID[:]),
	}
}

func TestICMProcessor_HandleSend_ParseError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	// Correct topic0 but empty data — ABI parse will fail.
	l := icmEVMLog(icmContractAddr, sendTopics(), []byte("corrupted"))
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.True(t, IsNonRetryable(err), "ABI parse failure should be NonRetryable")
}

func TestICMProcessor_HandleSend_WriteEventError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("CH write failed")
	f.sendRepo.writeSendEventFunc = func(_ context.Context, _ *icmrepo.SendEventRow) error {
		return expectedErr
	}

	data := icmEventData(t, "SendCrossChainMessage", icmMinMsg(), icmMinFeeInfo())
	l := icmEVMLog(icmContractAddr, sendTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleSend_WritePartialError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("partial write failed")
	f.messages.writePartialSendFunc = func(_ context.Context, _ *icmrepo.MessagePartialSendRow) error {
		return expectedErr
	}

	data := icmEventData(t, "SendCrossChainMessage", icmMinMsg(), icmMinFeeInfo())
	l := icmEVMLog(icmContractAddr, sendTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleSend_Success(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var sendEventCalled, partialSendCalled bool
	f.sendRepo.writeSendEventFunc = func(_ context.Context, row *icmrepo.SendEventRow) error {
		sendEventCalled = true
		assert.Equal(t, testBlockchainID, row.BlockchainID)
		assert.Equal(t, common.Hash(icmMsgID).Hex(), row.MessageID)
		return nil
	}
	f.messages.writePartialSendFunc = func(_ context.Context, row *icmrepo.MessagePartialSendRow) error {
		partialSendCalled = true
		assert.Equal(t, testBlockchainID, row.SourceBlockchainID)
		assert.Equal(t, common.Hash(icmMsgID).Hex(), row.MessageID)
		return nil
	}

	data := icmEventData(t, "SendCrossChainMessage", icmMinMsg(), icmMinFeeInfo())
	l := icmEVMLog(icmContractAddr, sendTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	require.NoError(t, f.proc.Process(t.Context(), msg))
	assert.True(t, sendEventCalled, "WriteSendEvent should have been called")
	assert.True(t, partialSendCalled, "WritePartialSend should have been called")
}

// ============================================================================
// handleReceive tests
// ============================================================================

func receiveTopics() []common.Hash {
	return []common.Hash{
		common.HexToHash(eventSigReceiveCrossChainMessage),
		common.BytesToHash(icmMsgID[:]),
		common.BytesToHash(icmSrcChainID[:]),
		common.BytesToHash(icmDeliverer.Bytes()),
	}
}

func TestICMProcessor_HandleReceive_ParseError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	l := icmEVMLog(icmContractAddr, receiveTopics(), []byte("corrupted"))
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.True(t, IsNonRetryable(err), "ABI parse failure should be NonRetryable")
}

func TestICMProcessor_HandleReceive_WriteEventError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("receive write failed")
	f.receiveRepo.writeReceiveEventFunc = func(_ context.Context, _ *icmrepo.ReceiveEventRow) error {
		return expectedErr
	}

	// ReceiveCrossChainMessage non-indexed: rewardRedeemer (address), message (TeleporterMessage)
	data := icmEventData(t, "ReceiveCrossChainMessage", icmRelayerAddr, icmMinMsg())
	l := icmEVMLog(icmContractAddr, receiveTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleReceive_WritePartialError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("partial receive failed")
	f.messages.writePartialReceiveFunc = func(_ context.Context, _ *icmrepo.MessagePartialReceiveRow) error {
		return expectedErr
	}

	data := icmEventData(t, "ReceiveCrossChainMessage", icmRelayerAddr, icmMinMsg())
	l := icmEVMLog(icmContractAddr, receiveTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleReceive_Success(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var eventCalled, partialCalled bool
	f.receiveRepo.writeReceiveEventFunc = func(_ context.Context, _ *icmrepo.ReceiveEventRow) error {
		eventCalled = true
		return nil
	}
	f.messages.writePartialReceiveFunc = func(_ context.Context, _ *icmrepo.MessagePartialReceiveRow) error {
		partialCalled = true
		return nil
	}

	data := icmEventData(t, "ReceiveCrossChainMessage", icmRelayerAddr, icmMinMsg())
	l := icmEVMLog(icmContractAddr, receiveTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	require.NoError(t, f.proc.Process(t.Context(), msg))
	assert.True(t, eventCalled)
	assert.True(t, partialCalled)
}

// ============================================================================
// handleExecuted tests
// ============================================================================

func executedTopics() []common.Hash {
	return []common.Hash{
		common.HexToHash(eventSigMessageExecuted),
		common.BytesToHash(icmMsgID[:]),
		common.BytesToHash(icmSrcChainID[:]),
	}
}

func TestICMProcessor_HandleExecuted_WriteEventError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("executed write failed")
	f.executedRepo.writeMessageExecutedEventFunc = func(_ context.Context, _ *icmrepo.MessageExecutedEventRow) error {
		return expectedErr
	}

	// MessageExecuted has no non-indexed data.
	l := icmEVMLog(icmContractAddr, executedTopics(), nil)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleExecuted_WritePartialError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("partial executed failed")
	f.messages.writePartialExecutedFunc = func(_ context.Context, _ *icmrepo.MessagePartialExecutedRow) error {
		return expectedErr
	}

	l := icmEVMLog(icmContractAddr, executedTopics(), nil)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleExecuted_Success(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var eventCalled, partialCalled bool
	f.executedRepo.writeMessageExecutedEventFunc = func(_ context.Context, row *icmrepo.MessageExecutedEventRow) error {
		eventCalled = true
		assert.Equal(t, common.Hash(icmMsgID).Hex(), row.MessageID)
		return nil
	}
	f.messages.writePartialExecutedFunc = func(_ context.Context, row *icmrepo.MessagePartialExecutedRow) error {
		partialCalled = true
		assert.Equal(t, testBlockchainID, row.DestinationBlockchainID)
		return nil
	}

	l := icmEVMLog(icmContractAddr, executedTopics(), nil)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	require.NoError(t, f.proc.Process(t.Context(), msg))
	assert.True(t, eventCalled)
	assert.True(t, partialCalled)
}

// ============================================================================
// handleExecutionFailed tests
// ============================================================================

func executionFailedTopics() []common.Hash {
	return []common.Hash{
		common.HexToHash(eventSigMessageExecutionFailed),
		common.BytesToHash(icmMsgID[:]),
		common.BytesToHash(icmSrcChainID[:]),
	}
}

func TestICMProcessor_HandleExecutionFailed_ParseError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	l := icmEVMLog(icmContractAddr, executionFailedTopics(), []byte("corrupted"))
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.True(t, IsNonRetryable(err), "ABI parse failure should be NonRetryable")
}

func TestICMProcessor_HandleExecutionFailed_WriteEventError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("exec failed write")
	f.execFailedRepo.writeMessageExecutionFailedEventFunc = func(_ context.Context, _ *icmrepo.MessageExecutionFailedEventRow) error {
		return expectedErr
	}

	data := icmEventData(t, "MessageExecutionFailed", icmMinMsg())
	l := icmEVMLog(icmContractAddr, executionFailedTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleExecutionFailed_WritePartialError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("partial exec failed")
	f.messages.writePartialExecutionFailedFunc = func(_ context.Context, _ *icmrepo.MessagePartialExecutionFailedRow) error {
		return expectedErr
	}

	data := icmEventData(t, "MessageExecutionFailed", icmMinMsg())
	l := icmEVMLog(icmContractAddr, executionFailedTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleExecutionFailed_Success(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var eventCalled, partialCalled bool
	f.execFailedRepo.writeMessageExecutionFailedEventFunc = func(_ context.Context, _ *icmrepo.MessageExecutionFailedEventRow) error {
		eventCalled = true
		return nil
	}
	f.messages.writePartialExecutionFailedFunc = func(_ context.Context, _ *icmrepo.MessagePartialExecutionFailedRow) error {
		partialCalled = true
		return nil
	}

	data := icmEventData(t, "MessageExecutionFailed", icmMinMsg())
	l := icmEVMLog(icmContractAddr, executionFailedTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	require.NoError(t, f.proc.Process(t.Context(), msg))
	assert.True(t, eventCalled)
	assert.True(t, partialCalled)
}

// ============================================================================
// handleReceipt tests
// ============================================================================

func receiptTopics() []common.Hash {
	return []common.Hash{
		common.HexToHash(eventSigReceiptReceived),
		common.BytesToHash(icmMsgID[:]),
		common.BytesToHash(icmDstChainID[:]),
		common.BytesToHash(icmRelayerAddr.Bytes()),
	}
}

func TestICMProcessor_HandleReceipt_ParseError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	l := icmEVMLog(icmContractAddr, receiptTopics(), []byte("corrupted"))
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.True(t, IsNonRetryable(err), "ABI parse failure should be NonRetryable")
}

func TestICMProcessor_HandleReceipt_WriteEventError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("receipt write failed")
	f.receiptsRepo.writeReceiptsEventFunc = func(_ context.Context, _ *icmrepo.ReceiptEventRow) error {
		return expectedErr
	}

	// ReceiptReceived non-indexed: feeInfo (TeleporterFeeInfo)
	data := icmEventData(t, "ReceiptReceived", icmMinFeeInfo())
	l := icmEVMLog(icmContractAddr, receiptTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleReceipt_WritePartialError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("partial receipt failed")
	f.messages.writePartialReceiptFunc = func(_ context.Context, _ *icmrepo.MessagePartialReceiptRow) error {
		return expectedErr
	}

	data := icmEventData(t, "ReceiptReceived", icmMinFeeInfo())
	l := icmEVMLog(icmContractAddr, receiptTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleReceipt_Success(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var eventCalled, partialCalled bool
	f.receiptsRepo.writeReceiptsEventFunc = func(_ context.Context, _ *icmrepo.ReceiptEventRow) error {
		eventCalled = true
		return nil
	}
	f.messages.writePartialReceiptFunc = func(_ context.Context, row *icmrepo.MessagePartialReceiptRow) error {
		partialCalled = true
		assert.Equal(t, uint8(1), row.ReceiptDelivered)
		assert.Equal(t, testBlockchainID, row.SourceBlockchainID)
		return nil
	}

	data := icmEventData(t, "ReceiptReceived", icmMinFeeInfo())
	l := icmEVMLog(icmContractAddr, receiptTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	require.NoError(t, f.proc.Process(t.Context(), msg))
	assert.True(t, eventCalled)
	assert.True(t, partialCalled)
}

// ============================================================================
// handleFeeInfo tests (AddFeeAmount)
// ============================================================================

func feeInfoTopics() []common.Hash {
	return []common.Hash{
		common.HexToHash(eventSigAddFeeAmount),
		common.BytesToHash(icmMsgID[:]),
	}
}

func TestICMProcessor_HandleFeeInfo_ParseError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	l := icmEVMLog(icmContractAddr, feeInfoTopics(), []byte("corrupted"))
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.True(t, IsNonRetryable(err), "ABI parse failure should be NonRetryable")
}

func TestICMProcessor_HandleFeeInfo_WriteError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("fee info write failed")
	f.feeInfoRepo.writeFeeInfoEventFunc = func(_ context.Context, _ *icmrepo.AddFeeEventRow) error {
		return expectedErr
	}

	// AddFeeAmount non-indexed: updatedFeeInfo (TeleporterFeeInfo)
	data := icmEventData(t, "AddFeeAmount", icmMinFeeInfo())
	l := icmEVMLog(icmContractAddr, feeInfoTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleFeeInfo_Success(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var eventCalled bool
	f.feeInfoRepo.writeFeeInfoEventFunc = func(_ context.Context, row *icmrepo.AddFeeEventRow) error {
		eventCalled = true
		// DestinationBlockchainID must be empty — AddFeeAmount does not emit it.
		assert.Empty(t, row.DestinationBlockchainID)
		assert.Equal(t, common.Hash(icmMsgID).Hex(), row.MessageID)
		return nil
	}

	data := icmEventData(t, "AddFeeAmount", icmMinFeeInfo())
	l := icmEVMLog(icmContractAddr, feeInfoTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	require.NoError(t, f.proc.Process(t.Context(), msg))
	assert.True(t, eventCalled)
}

// ============================================================================
// handleFeeRedemption tests (RelayerRewardsRedeemed)
// ============================================================================

func feeRedemptionTopics() []common.Hash {
	return []common.Hash{
		common.HexToHash(eventSigRelayerRewardsRedeemed),
		common.BytesToHash(icmRedeemer.Bytes()),
		common.BytesToHash(icmAssetAddr.Bytes()),
	}
}

func TestICMProcessor_HandleFeeRedemption_ParseError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	l := icmEVMLog(icmContractAddr, feeRedemptionTopics(), []byte("corrupted"))
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.True(t, IsNonRetryable(err), "ABI parse failure should be NonRetryable")
}

func TestICMProcessor_HandleFeeRedemption_WriteError(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)
	expectedErr := errors.New("fee redemption write failed")
	f.feeRedemptionsRepo.writeFeeRedemptionsEventFunc = func(_ context.Context, _ *icmrepo.RelayerRewardRedeemedEventRow) error {
		return expectedErr
	}

	// RelayerRewardsRedeemed non-indexed: amount (uint256)
	data := icmEventData(t, "RelayerRewardsRedeemed", big.NewInt(5_000))
	l := icmEVMLog(icmContractAddr, feeRedemptionTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	err := f.proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestICMProcessor_HandleFeeRedemption_Success(t *testing.T) {
	t.Parallel()
	f := newICMTestFixture(t)

	var eventCalled bool
	f.feeRedemptionsRepo.writeFeeRedemptionsEventFunc = func(_ context.Context, row *icmrepo.RelayerRewardRedeemedEventRow) error {
		eventCalled = true
		assert.Equal(t, icmRedeemer.Hex(), row.RedeemerAddress)
		assert.Equal(t, icmAssetAddr.Hex(), row.FeeTokenAddress)
		return nil
	}

	data := icmEventData(t, "RelayerRewardsRedeemed", big.NewInt(5_000))
	l := icmEVMLog(icmContractAddr, feeRedemptionTopics(), data)
	msg := icmKafkaMsg(t, icmBuildBlock(l))

	require.NoError(t, f.proc.Process(t.Context(), msg))
	assert.True(t, eventCalled)
}
