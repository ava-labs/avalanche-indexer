package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/batchwriter"
	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/icmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	teleportermessenger "github.com/ava-labs/icm-contracts/abi-bindings/go/teleporter/TeleporterMessenger"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ErrBlockchainIDRequired is returned when the block message has no blockchainId field.
var ErrBlockchainIDRequired = errors.New("blockchainId is required")

// ErrNoContractAddresses is returned when NewICMProcessor is called with an empty address list.
var ErrNoContractAddresses = errors.New("at least one teleporter contract address is required")

// ErrInvalidContractAddress is returned when a contract address is not a valid 0x-prefixed hex string.
var ErrInvalidContractAddress = errors.New("invalid teleporter contract address")

const (
	// topic0 hashes for the seven tracked Teleporter events (lowercase 0x-prefixed).
	eventSigSendCrossChainMessage    = "0x2a211ad4a59ab9d003852404f9c57c690704ee755f3c79d2c2812ad32da99df8"
	eventSigReceiveCrossChainMessage = "0x292ee90bbaf70b5d4936025e09d56ba08f3e421156b6a568cf3c2840d9343e34"
	eventSigMessageExecuted          = "0x34795cc6b122b9a0ae684946319f1e14a577b4e8f9b3dda9ac94c21a54d3188c"
	eventSigMessageExecutionFailed   = "0x4619adc1017b82e02eaefac01a43d50d6d8de4460774bc370c3ff0210d40c985"
	eventSigReceiptReceived          = "0xd13a7935f29af029349bed0a2097455b91fd06190a30478c575db3f31e00bf57"
	eventSigAddFeeAmount             = "0xc1bfd1f1208927dfbd414041dcb5256e6c9ad90dd61aec3249facbd34ff7b3e1"
	eventSigRelayerRewardsRedeemed   = "0x3294c84e5b0f29d9803655319087207bc94f4db29f7927846944822773780b88"
)

// icmEventSigMap is a membership set of known Teleporter topic0 hashes used for filter 2.
var icmEventSigMap = map[string]struct{}{
	eventSigSendCrossChainMessage:    {},
	eventSigReceiveCrossChainMessage: {},
	eventSigMessageExecuted:          {},
	eventSigMessageExecutionFailed:   {},
	eventSigReceiptReceived:          {},
	eventSigAddFeeAmount:             {},
	eventSigRelayerRewardsRedeemed:   {},
}

// ICMProcessor implements Processor for Teleporter/ICM events.
// It applies a two-step filter (contract address + topic0) before ABI-decoding, then
// writes to the relevant event table and, where applicable, a partial row to icm_messages.
// When batchWriter is non-nil, event table rows are batched; partial rows to icm_messages
// are always written immediately regardless of mode.
// Safe for concurrent use.
type ICMProcessor struct {
	log                        *zap.SugaredLogger
	messagesRepo               icmrepo.Messages
	sendRepo                   icmrepo.SendEvents
	receiveRepo                icmrepo.ReceiveEvents
	messageExecutedRepo        icmrepo.MessageExecutedEvents
	messageExecutionFailedRepo icmrepo.MessageExecutionFailedEvents
	receiptsRepo               icmrepo.ReceiptEvents
	feeInfoRepo                icmrepo.AddFeeEvents
	feeRedemptionsRepo         icmrepo.RelayerRewardRedeemedEvents
	contractAddrs              map[common.Address]struct{}
	filterer                   *teleportermessenger.TeleporterMessengerFilterer
	metrics                    *metrics.Metrics
	batchWriter                *batchwriter.Writer
}

// NewICMProcessor creates a new ICMProcessor. contractAddrs must contain at least one
// address; the processor fails to start otherwise. bw may be nil to disable batch mode,
// in which case every event row is written to ClickHouse individually and synchronously.
func NewICMProcessor(
	log *zap.SugaredLogger,
	messagesRepo icmrepo.Messages,
	sendRepo icmrepo.SendEvents,
	receiveRepo icmrepo.ReceiveEvents,
	messageExecutedRepo icmrepo.MessageExecutedEvents,
	messageExecutionFailedRepo icmrepo.MessageExecutionFailedEvents,
	receiptsRepo icmrepo.ReceiptEvents,
	feeInfoRepo icmrepo.AddFeeEvents,
	feeRedemptionsRepo icmrepo.RelayerRewardRedeemedEvents,
	contractAddrs []string,
	m *metrics.Metrics,
	bw *batchwriter.Writer,
) (*ICMProcessor, error) {
	if len(contractAddrs) == 0 {
		return nil, ErrNoContractAddresses
	}

	// TeleporterMessengerFilterer only needs the ABI for Parse* calls; the nil filterer
	// backend is safe because UnpackLog never invokes it.
	filterer, err := teleportermessenger.NewTeleporterMessengerFilterer(common.Address{}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create teleporter messenger filterer: %w", err)
	}

	addrSet := make(map[common.Address]struct{}, len(contractAddrs))
	for _, a := range contractAddrs {
		if !common.IsHexAddress(a) {
			return nil, fmt.Errorf("%w %q: must be a 0x-prefixed 40-character hex string", ErrInvalidContractAddress, a)
		}
		addrSet[common.HexToAddress(a)] = struct{}{}
	}

	if m == nil {
		m = metrics.NewNoOp()
	}

	return &ICMProcessor{
		log:                        log,
		messagesRepo:               messagesRepo,
		sendRepo:                   sendRepo,
		receiveRepo:                receiveRepo,
		messageExecutedRepo:        messageExecutedRepo,
		messageExecutionFailedRepo: messageExecutionFailedRepo,
		receiptsRepo:               receiptsRepo,
		feeInfoRepo:                feeInfoRepo,
		feeRedemptionsRepo:         feeRedemptionsRepo,
		contractAddrs:              addrSet,
		filterer:                   filterer,
		metrics:                    m,
		batchWriter:                bw,
	}, nil
}

// Process implements Processor. It unmarshals an EVMBlock Kafka message, iterates every
// log in every transaction receipt, and dispatches matched Teleporter events to the
// appropriate handler.
func (p *ICMProcessor) Process(ctx context.Context, msg *ckafka.Message) error {
	if msg == nil || msg.Value == nil {
		p.metrics.IncError("icm_nil_message")
		return NonRetryable(ErrNilMessage)
	}

	var block kafkamsg.EVMBlock
	if err := json.Unmarshal(msg.Value, &block); err != nil {
		p.metrics.IncError("icm_unmarshal_error")
		return NonRetryable(fmt.Errorf("%w: %w", ErrUnmarshalBlock, err))
	}
	if block.BlockchainID == nil {
		p.metrics.IncError("icm_missing_blockchain_id")
		return NonRetryable(ErrBlockchainIDRequired)
	}

	p.log.Debugw("processing ICM block",
		"blockchainID", *block.BlockchainID,
		"blockNumber", blockNum(&block),
		"txCount", len(block.Transactions),
	)

	var req *batchwriter.WriteRequest
	if p.batchWriter != nil {
		req = &batchwriter.WriteRequest{}
	}

	for _, tx := range block.Transactions {
		if tx == nil || tx.Receipt == nil {
			continue
		}
		for _, l := range tx.Receipt.Logs {
			if l == nil {
				continue
			}
			if err := p.processLog(ctx, l, tx, &block, req); err != nil {
				return err
			}
		}
	}

	if req != nil {
		ch := p.batchWriter.Submit(ctx, req)
		select {
		case err := <-ch:
			if err != nil {
				return classifyWriteErr(fmt.Errorf("icm batch submit: %w", err))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// processLog applies the two-step filter (address, then topic0) and dispatches to the
// appropriate handler. Returns nil for logs that do not match — they are silently skipped.
// req is non-nil only in batch mode; handlers append event rows to it instead of writing.
func (p *ICMProcessor) processLog(
	ctx context.Context,
	log *kafkamsg.EVMLog,
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	req *batchwriter.WriteRequest,
) error {
	// Filter 1: contract address must be in the configured set.
	if _, ok := p.contractAddrs[log.Address]; !ok {
		return nil
	}
	// Filter 2: topic0 must match a known Teleporter event signature.
	if len(log.Topics) == 0 {
		return nil
	}
	// common.Hash.Hex() already returns lowercase 0x-prefixed hex; no ToLower needed.
	topic0 := log.Topics[0].Hex()
	if _, known := icmEventSigMap[topic0]; !known {
		return nil
	}

	evmLog := toSubnetEVMLog(log)

	switch topic0 {
	case eventSigSendCrossChainMessage:
		return p.handleSend(ctx, evmLog, tx, block, req)
	case eventSigReceiveCrossChainMessage:
		return p.handleReceive(ctx, evmLog, tx, block, req)
	case eventSigMessageExecuted:
		return p.handleExecuted(ctx, evmLog, tx, block, req)
	case eventSigMessageExecutionFailed:
		return p.handleExecutionFailed(ctx, evmLog, tx, block, req)
	case eventSigReceiptReceived:
		return p.handleReceipt(ctx, evmLog, tx, block, req)
	case eventSigAddFeeAmount:
		return p.handleFeeInfo(ctx, evmLog, tx, block, req)
	case eventSigRelayerRewardsRedeemed:
		return p.handleFeeRedemption(ctx, evmLog, tx, block, req)
	default:
		// Guard: a topic0 is in icmEventSigMap but has no case here.
		// This indicates a missing case after adding a new event to the map.
		p.log.Warnw("processLog: no handler for event in icmEventSigMap; add a switch case",
			"topic0", topic0,
		)
		return nil
	}
}

// toSubnetEVMLog converts a kafkamsg.EVMLog to the types.Log expected by the ABI parser.
// Both types share identical field types, so this is a direct struct copy.
func toSubnetEVMLog(l *kafkamsg.EVMLog) types.Log {
	return types.Log{
		Address:     l.Address,
		Topics:      l.Topics,
		Data:        l.Data,
		TxHash:      l.TxHash,
		BlockNumber: l.BlockNumber,
		BlockHash:   l.BlockHash,
		TxIndex:     l.TxIndex,
		Index:       l.Index,
		Removed:     l.Removed,
	}
}

// blockNum returns block.Number as uint64, defaulting to 0 if Number is nil.
func blockNum(block *kafkamsg.EVMBlock) uint64 {
	if block.Number == nil {
		return 0
	}
	return block.Number.Uint64()
}

// icmGasSpent computes the gas cost for a transaction.
// Uses effectiveGasPrice from the receipt (correct for EIP-1559 transactions), falling back
// to the tx-level gas price. Returns nil if no price source is available.
// Callers must ensure tx.Receipt != nil before calling.
func icmGasSpent(tx *kafkamsg.EVMTransaction) *big.Int {
	var price *big.Int
	switch {
	case tx.Receipt.EffectiveGasPrice != nil:
		price = tx.Receipt.EffectiveGasPrice
	case tx.GasPrice != nil:
		price = tx.GasPrice
	default:
		return nil
	}
	return new(big.Int).Mul(price, new(big.Int).SetUint64(tx.Receipt.GasUsed))
}

// hexAddrs converts a slice of common.Address to their hex string representations.
func hexAddrs(addrs []common.Address) []string {
	result := make([]string, len(addrs))
	for i, a := range addrs {
		result[i] = a.Hex()
	}
	return result
}

// splitReceipts extracts ReceivedMessageNonce and RelayerRewardAddress from each
// TeleporterMessageReceipt in a single pass, returning parallel slices.
func splitReceipts(receipts []teleportermessenger.TeleporterMessageReceipt) ([]*big.Int, []string) {
	nonces := make([]*big.Int, len(receipts))
	relayers := make([]string, len(receipts))
	for i, r := range receipts {
		nonces[i] = r.ReceivedMessageNonce
		relayers[i] = r.RelayerRewardAddress.Hex()
	}
	return nonces, relayers
}

// marshalReceipts JSON-encodes TeleporterMessageReceipts for storage in icm_messages.
func marshalReceipts(receipts []teleportermessenger.TeleporterMessageReceipt) string {
	type receiptJSON struct {
		Nonce   string `json:"nonce"`
		Relayer string `json:"relayer"`
	}
	rs := make([]receiptJSON, len(receipts))
	for i, r := range receipts {
		rs[i] = receiptJSON{
			Nonce:   r.ReceivedMessageNonce.String(),
			Relayer: r.RelayerRewardAddress.Hex(),
		}
	}
	b, _ := json.Marshal(rs) // simple struct; Marshal only fails on unmarshalable types
	return string(b)
}

// chainID converts a [32]byte blockchain ID from ABI events to the CB58 string format
// used by block.BlockchainID, preserving consistency in the icm_messages merge key.
func chainID(id [32]byte) string {
	return ids.ID(id).String()
}

func (p *ICMProcessor) handleSend(
	ctx context.Context,
	evmLog types.Log,
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	req *batchwriter.WriteRequest,
) error {
	parsed, err := p.filterer.ParseSendCrossChainMessage(evmLog)
	if err != nil {
		return NonRetryable(fmt.Errorf("ParseSendCrossChainMessage: %w", err))
	}

	msgID := common.Hash(parsed.MessageID).Hex()
	dstChainID := chainID(parsed.DestinationBlockchainID)
	blockTime := time.Unix(int64(block.Timestamp), 0).UTC()
	allowedRelayers := hexAddrs(parsed.Message.AllowedRelayerAddresses)
	receiptNonces, receiptRelayers := splitReceipts(parsed.Message.Receipts)

	eventRow := &icmrepo.SendEventRow{
		BlockchainID:             *block.BlockchainID,
		EVMChainID:               block.EVMChainID,
		BlockNumber:              blockNum(block),
		BlockTime:                blockTime,
		TxHash:                   tx.Hash,
		TxIndex:                  uint32(evmLog.TxIndex),
		LogIndex:                 uint32(evmLog.Index),
		ContractAddress:          evmLog.Address.Hex(),
		MessageID:                msgID,
		DestinationBlockchainID:  dstChainID,
		SenderAddress:            parsed.Message.OriginSenderAddress.Hex(),
		DestinationAddress:       parsed.Message.DestinationAddress.Hex(),
		RequiredGasLimit:         parsed.Message.RequiredGasLimit,
		AllowedRelayerAddresses:  allowedRelayers,
		FeeTokenAddress:          parsed.FeeInfo.FeeTokenAddress.Hex(),
		FeeAmount:                parsed.FeeInfo.Amount,
		MessageNonce:             parsed.Message.MessageNonce,
		MessageData:              parsed.Message.Message,
		ReceiptsMessageNonces:    receiptNonces,
		ReceiptsRelayerAddresses: receiptRelayers,
	}

	partialRow := &icmrepo.MessagePartialSendRow{
		SourceBlockchainID:      *block.BlockchainID,
		DestinationBlockchainID: dstChainID,
		MessageID:               msgID,
		SourceBlockTime:         blockTime,
		SourceTxHash:            tx.Hash,
		EVMChainID:              block.EVMChainID,
		ContractAddress:         evmLog.Address.Hex(),
		MessageNonce:            parsed.Message.MessageNonce,
		SenderAddress:           parsed.Message.OriginSenderAddress.Hex(),
		DestinationAddress:      parsed.Message.DestinationAddress.Hex(),
		RequiredGasLimit:        parsed.Message.RequiredGasLimit,
		AllowedRelayerAddresses: allowedRelayers,
		FeeTokenAddress:         parsed.FeeInfo.FeeTokenAddress.Hex(),
		FeeAmount:               parsed.FeeInfo.Amount,
		MessageData:             string(parsed.Message.Message),
		SourceGasSpent:          icmGasSpent(tx),
		MessageReceipts:         marshalReceipts(parsed.Message.Receipts),
	}

	if req == nil {
		writeStart := time.Now()
		err = p.sendRepo.WriteSendEvent(ctx, eventRow)
		recordClickHouseWrite(p.metrics, clickhouse.DefaultICMSendEventsTableName, err, writeStart)
		if err != nil {
			p.metrics.IncError("icm_write_error")
			return classifyWriteErr(fmt.Errorf("write send event: %w", err))
		}
	} else {
		req.ICMSendEvents = append(req.ICMSendEvents, eventRow)
	}
	writeStart := time.Now()
	err = p.messagesRepo.WritePartialSend(ctx, partialRow)
	recordClickHouseWrite(p.metrics, clickhouse.DefaultICMMessagesTableName, err, writeStart)
	if err != nil {
		p.metrics.IncError("icm_write_error")
		return classifyWriteErr(fmt.Errorf("write partial send: %w", err))
	}
	return nil
}

func (p *ICMProcessor) handleReceive(
	ctx context.Context,
	evmLog types.Log,
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	req *batchwriter.WriteRequest,
) error {
	parsed, err := p.filterer.ParseReceiveCrossChainMessage(evmLog)
	if err != nil {
		return NonRetryable(fmt.Errorf("ParseReceiveCrossChainMessage: %w", err))
	}

	msgID := common.Hash(parsed.MessageID).Hex()
	srcChainID := chainID(parsed.SourceBlockchainID)
	dstChainID := chainID(parsed.Message.DestinationBlockchainID)
	blockTime := time.Unix(int64(block.Timestamp), 0).UTC()
	receiptNonces, receiptRelayers := splitReceipts(parsed.Message.Receipts)

	eventRow := &icmrepo.ReceiveEventRow{
		BlockchainID:             *block.BlockchainID,
		EVMChainID:               block.EVMChainID,
		BlockNumber:              blockNum(block),
		BlockTime:                blockTime,
		TxHash:                   tx.Hash,
		TxIndex:                  uint32(evmLog.TxIndex),
		LogIndex:                 uint32(evmLog.Index),
		ContractAddress:          evmLog.Address.Hex(),
		MessageID:                msgID,
		SourceBlockchainID:       srcChainID,
		DelivererAddress:         parsed.Deliverer.Hex(),
		RewardRedeemerAddress:    parsed.RewardRedeemer.Hex(),
		MessageNonce:             parsed.Message.MessageNonce,
		OriginSenderAddress:      parsed.Message.OriginSenderAddress.Hex(),
		DestinationBlockchainID:  dstChainID,
		DestinationAddress:       parsed.Message.DestinationAddress.Hex(),
		RequiredGasLimit:         parsed.Message.RequiredGasLimit,
		AllowedRelayerAddresses:  hexAddrs(parsed.Message.AllowedRelayerAddresses),
		MessageData:              parsed.Message.Message,
		ReceiptsMessageNonces:    receiptNonces,
		ReceiptsRelayerAddresses: receiptRelayers,
	}

	partialRow := &icmrepo.MessagePartialReceiveRow{
		SourceBlockchainID:      srcChainID,
		DestinationBlockchainID: *block.BlockchainID,
		MessageID:               msgID,
		ReceiveBlockTime:        blockTime,
		ReceiveTxHash:           tx.Hash,
		DelivererAddress:        parsed.Deliverer.Hex(),
		RewardRedeemerAddress:   parsed.RewardRedeemer.Hex(),
		DestinationEVMChainID:   block.EVMChainID,
		DestinationGasSpent:     icmGasSpent(tx),
	}

	if req == nil {
		writeStart := time.Now()
		err = p.receiveRepo.WriteReceiveEvent(ctx, eventRow)
		recordClickHouseWrite(p.metrics, clickhouse.DefaultICMReceiveEventsTableName, err, writeStart)
		if err != nil {
			p.metrics.IncError("icm_write_error")
			return classifyWriteErr(fmt.Errorf("write receive event: %w", err))
		}
	} else {
		req.ICMReceiveEvents = append(req.ICMReceiveEvents, eventRow)
	}
	writeStart := time.Now()
	err = p.messagesRepo.WritePartialReceive(ctx, partialRow)
	recordClickHouseWrite(p.metrics, clickhouse.DefaultICMMessagesTableName, err, writeStart)
	if err != nil {
		p.metrics.IncError("icm_write_error")
		return classifyWriteErr(fmt.Errorf("write partial receive: %w", err))
	}
	return nil
}

func (p *ICMProcessor) handleExecuted(
	ctx context.Context,
	evmLog types.Log,
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	req *batchwriter.WriteRequest,
) error {
	parsed, err := p.filterer.ParseMessageExecuted(evmLog)
	if err != nil {
		return NonRetryable(fmt.Errorf("ParseMessageExecuted: %w", err))
	}

	msgID := common.Hash(parsed.MessageID).Hex()
	srcChainID := chainID(parsed.SourceBlockchainID)
	blockTime := time.Unix(int64(block.Timestamp), 0).UTC()

	eventRow := &icmrepo.MessageExecutedEventRow{
		BlockchainID:       *block.BlockchainID,
		EVMChainID:         block.EVMChainID,
		BlockNumber:        blockNum(block),
		BlockTime:          blockTime,
		TxHash:             tx.Hash,
		TxIndex:            uint32(evmLog.TxIndex),
		LogIndex:           uint32(evmLog.Index),
		ContractAddress:    evmLog.Address.Hex(),
		MessageID:          msgID,
		SourceBlockchainID: srcChainID,
	}

	// MessageExecuted is emitted on the destination chain; block.BlockchainID is that chain.
	partialRow := &icmrepo.MessagePartialExecutedRow{
		SourceBlockchainID:      srcChainID,
		DestinationBlockchainID: *block.BlockchainID,
		MessageID:               msgID,
		ExecutedBlockTime:       blockTime,
		ExecutedTxHash:          tx.Hash,
	}

	if req == nil {
		writeStart := time.Now()
		err = p.messageExecutedRepo.WriteMessageExecutedEvent(ctx, eventRow)
		recordClickHouseWrite(p.metrics, clickhouse.DefaultICMMessageExecutedEventsTableName, err, writeStart)
		if err != nil {
			p.metrics.IncError("icm_write_error")
			return classifyWriteErr(fmt.Errorf("write message executed event: %w", err))
		}
	} else {
		req.ICMMessageExecutedEvents = append(req.ICMMessageExecutedEvents, eventRow)
	}
	writeStart := time.Now()
	err = p.messagesRepo.WritePartialExecuted(ctx, partialRow)
	recordClickHouseWrite(p.metrics, clickhouse.DefaultICMMessagesTableName, err, writeStart)
	if err != nil {
		p.metrics.IncError("icm_write_error")
		return classifyWriteErr(fmt.Errorf("write partial executed: %w", err))
	}
	return nil
}

func (p *ICMProcessor) handleExecutionFailed(
	ctx context.Context,
	evmLog types.Log,
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	req *batchwriter.WriteRequest,
) error {
	parsed, err := p.filterer.ParseMessageExecutionFailed(evmLog)
	if err != nil {
		return NonRetryable(fmt.Errorf("ParseMessageExecutionFailed: %w", err))
	}

	msgID := common.Hash(parsed.MessageID).Hex()
	srcChainID := chainID(parsed.SourceBlockchainID)
	dstChainID := chainID(parsed.Message.DestinationBlockchainID)
	blockTime := time.Unix(int64(block.Timestamp), 0).UTC()
	receiptNonces, receiptRelayers := splitReceipts(parsed.Message.Receipts)

	eventRow := &icmrepo.MessageExecutionFailedEventRow{
		BlockchainID:             *block.BlockchainID,
		EVMChainID:               block.EVMChainID,
		BlockNumber:              blockNum(block),
		BlockTime:                blockTime,
		TxHash:                   tx.Hash,
		TxIndex:                  uint32(evmLog.TxIndex),
		LogIndex:                 uint32(evmLog.Index),
		ContractAddress:          evmLog.Address.Hex(),
		MessageID:                msgID,
		SourceBlockchainID:       srcChainID,
		MessageNonce:             parsed.Message.MessageNonce,
		OriginSenderAddress:      parsed.Message.OriginSenderAddress.Hex(),
		DestinationBlockchainID:  dstChainID,
		DestinationAddress:       parsed.Message.DestinationAddress.Hex(),
		RequiredGasLimit:         parsed.Message.RequiredGasLimit,
		AllowedRelayerAddresses:  hexAddrs(parsed.Message.AllowedRelayerAddresses),
		MessageData:              parsed.Message.Message,
		ReceiptsMessageNonces:    receiptNonces,
		ReceiptsRelayerAddresses: receiptRelayers,
	}

	// MessageExecutionFailed is emitted on the destination chain; block.BlockchainID is that chain.
	partialRow := &icmrepo.MessagePartialExecutionFailedRow{
		SourceBlockchainID:      srcChainID,
		DestinationBlockchainID: *block.BlockchainID,
		MessageID:               msgID,
		LastExecutionFailedTime: blockTime,
	}

	if req == nil {
		writeStart := time.Now()
		err = p.messageExecutionFailedRepo.WriteMessageExecutionFailedEvent(ctx, eventRow)
		recordClickHouseWrite(p.metrics, clickhouse.DefaultICMMessageExecutionFailedEventsTableName, err, writeStart)
		if err != nil {
			p.metrics.IncError("icm_write_error")
			return classifyWriteErr(fmt.Errorf("write message execution failed event: %w", err))
		}
	} else {
		req.ICMMessageExecutionFailed = append(req.ICMMessageExecutionFailed, eventRow)
	}
	writeStart := time.Now()
	err = p.messagesRepo.WritePartialExecutionFailed(ctx, partialRow)
	recordClickHouseWrite(p.metrics, clickhouse.DefaultICMMessagesTableName, err, writeStart)
	if err != nil {
		p.metrics.IncError("icm_write_error")
		return classifyWriteErr(fmt.Errorf("write partial execution failed: %w", err))
	}
	return nil
}

func (p *ICMProcessor) handleReceipt(
	ctx context.Context,
	evmLog types.Log,
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	req *batchwriter.WriteRequest,
) error {
	parsed, err := p.filterer.ParseReceiptReceived(evmLog)
	if err != nil {
		return NonRetryable(fmt.Errorf("ParseReceiptReceived: %w", err))
	}

	msgID := common.Hash(parsed.MessageID).Hex()
	dstChainID := chainID(parsed.DestinationBlockchainID)
	blockTime := time.Unix(int64(block.Timestamp), 0).UTC()

	eventRow := &icmrepo.ReceiptEventRow{
		BlockchainID:            *block.BlockchainID,
		EVMChainID:              block.EVMChainID,
		BlockNumber:             blockNum(block),
		BlockTime:               blockTime,
		TxHash:                  tx.Hash,
		TxIndex:                 uint32(evmLog.TxIndex),
		LogIndex:                uint32(evmLog.Index),
		ContractAddress:         evmLog.Address.Hex(),
		MessageID:               msgID,
		DestinationBlockchainID: dstChainID,
		RelayerRewardAddress:    parsed.RelayerRewardAddress.Hex(),
		FeeTokenAddress:         parsed.FeeInfo.FeeTokenAddress.Hex(),
		FeeAmount:               parsed.FeeInfo.Amount,
	}

	// ReceiptReceived is emitted on the source chain; block.BlockchainID is the source chain.
	partialRow := &icmrepo.MessagePartialReceiptRow{
		SourceBlockchainID:      *block.BlockchainID,
		DestinationBlockchainID: dstChainID,
		MessageID:               msgID,
		ReceiptDelivered:        1,
	}

	if req == nil {
		writeStart := time.Now()
		err = p.receiptsRepo.WriteReceiptEvent(ctx, eventRow)
		recordClickHouseWrite(p.metrics, clickhouse.DefaultICMReceiptEventsTableName, err, writeStart)
		if err != nil {
			p.metrics.IncError("icm_write_error")
			return classifyWriteErr(fmt.Errorf("write receipts event: %w", err))
		}
	} else {
		req.ICMReceiptEvents = append(req.ICMReceiptEvents, eventRow)
	}
	writeStart := time.Now()
	err = p.messagesRepo.WritePartialReceipt(ctx, partialRow)
	recordClickHouseWrite(p.metrics, clickhouse.DefaultICMMessagesTableName, err, writeStart)
	if err != nil {
		p.metrics.IncError("icm_write_error")
		return classifyWriteErr(fmt.Errorf("write partial receipt: %w", err))
	}
	return nil
}

func (p *ICMProcessor) handleFeeInfo(
	ctx context.Context,
	evmLog types.Log,
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	req *batchwriter.WriteRequest,
) error {
	parsed, err := p.filterer.ParseAddFeeAmount(evmLog)
	if err != nil {
		return NonRetryable(fmt.Errorf("ParseAddFeeAmount: %w", err))
	}

	// AddFeeAmount does not emit a destination blockchain ID; the column is left empty.
	// (The analytics pipeline had a bug here, writing the fee amount into the chain ID column.)
	eventRow := &icmrepo.AddFeeEventRow{
		BlockchainID:            *block.BlockchainID,
		EVMChainID:              block.EVMChainID,
		BlockNumber:             blockNum(block),
		BlockTime:               time.Unix(int64(block.Timestamp), 0).UTC(),
		TxHash:                  tx.Hash,
		TxIndex:                 uint32(evmLog.TxIndex),
		LogIndex:                uint32(evmLog.Index),
		ContractAddress:         evmLog.Address.Hex(),
		MessageID:               common.Hash(parsed.MessageID).Hex(),
		DestinationBlockchainID: "",
		FeeTokenAddress:         parsed.UpdatedFeeInfo.FeeTokenAddress.Hex(),
		AdditionalFeeAmount:     parsed.UpdatedFeeInfo.Amount,
	}

	if req == nil {
		writeStart := time.Now()
		err = p.feeInfoRepo.WriteAddFeeEvent(ctx, eventRow)
		recordClickHouseWrite(p.metrics, clickhouse.DefaultICMAddFeeEventsTableName, err, writeStart)
		if err != nil {
			p.metrics.IncError("icm_write_error")
			return classifyWriteErr(fmt.Errorf("write fee info event: %w", err))
		}
	} else {
		req.ICMAddFeeEvents = append(req.ICMAddFeeEvents, eventRow)
	}
	return nil
}

func (p *ICMProcessor) handleFeeRedemption(
	ctx context.Context,
	evmLog types.Log,
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	req *batchwriter.WriteRequest,
) error {
	parsed, err := p.filterer.ParseRelayerRewardsRedeemed(evmLog)
	if err != nil {
		return NonRetryable(fmt.Errorf("ParseRelayerRewardsRedeemed: %w", err))
	}

	eventRow := &icmrepo.RelayerRewardRedeemedEventRow{
		BlockchainID:    *block.BlockchainID,
		EVMChainID:      block.EVMChainID,
		BlockNumber:     blockNum(block),
		BlockTime:       time.Unix(int64(block.Timestamp), 0).UTC(),
		TxHash:          tx.Hash,
		TxIndex:         uint32(evmLog.TxIndex),
		LogIndex:        uint32(evmLog.Index),
		ContractAddress: evmLog.Address.Hex(),
		RedeemerAddress: parsed.Redeemer.Hex(),
		FeeTokenAddress: parsed.Asset.Hex(),
		Amount:          parsed.Amount,
	}

	if req == nil {
		writeStart := time.Now()
		err = p.feeRedemptionsRepo.WriteRelayerRewardRedeemedEvent(ctx, eventRow)
		recordClickHouseWrite(p.metrics, clickhouse.DefaultICMRelayerRewardRedeemedEventsTableName, err, writeStart)
		if err != nil {
			p.metrics.IncError("icm_write_error")
			return classifyWriteErr(fmt.Errorf("write fee redemptions event: %w", err))
		}
	} else {
		req.ICMRelayerRewardRedeemed = append(req.ICMRelayerRewardRedeemed, eventRow)
	}
	return nil
}

var _ Processor = (*ICMProcessor)(nil)
