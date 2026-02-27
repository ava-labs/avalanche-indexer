package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	ErrUnmarshalBlockTrace = errors.New("failed to unmarshal block trace")
	ErrMissingBlockchainID = errors.New("blockchainID is required for block trace")
)

type CorethTracesProcessor struct {
	log                      *zap.SugaredLogger
	internalTransactionsRepo evmrepo.InternalTransactions
	metrics                  *metrics.Metrics
}

// Compile-time check that CorethTracesProcessor implements Processor.
var _ Processor = (*CorethTracesProcessor)(nil)

func NewCorethTracesProcessor(
	log *zap.SugaredLogger,
	internalTransactionsRepo evmrepo.InternalTransactions,
	metrics *metrics.Metrics,
) *CorethTracesProcessor {
	return &CorethTracesProcessor{
		log:                      log,
		internalTransactionsRepo: internalTransactionsRepo,
		metrics:                  metrics,
	}
}

// Process unmarshals msg.Value into an EVMBlockTrace and logs its details.
// Returns an error if msg or msg.Value is nil, or if unmarshaling fails.
// Records processing duration and errors to metrics if configured.
func (p *CorethTracesProcessor) Process(ctx context.Context, msg *cKafka.Message) error {
	start := time.Now()

	if msg == nil || msg.Value == nil {
		p.metrics.IncError("coreth_traces_nil_message")
		return ErrNilMessage
	}

	var blockTrace kafkamsg.EVMBlockTrace
	if err := json.Unmarshal(msg.Value, &blockTrace); err != nil {
		p.metrics.IncError("coreth_traces_unmarshal_error")
		return fmt.Errorf("%w: %w", ErrUnmarshalBlockTrace, err)
	}

	// Validate block trace (BlockchainID is required)
	if blockTrace.BlockchainID == nil {
		return ErrMissingBlockchainID
	}

	p.log.Debugw("processing coreth block trace",
		"evmChainID", blockTrace.EVMChainID,
		"bcID", blockTrace.BlockchainID,
		"blockNumber", blockTrace.BlockNumber,
		"traceCount", len(blockTrace.Traces),
	)

	// Persist traces to ClickHouse if repository is configured
	if p.internalTransactionsRepo != nil {
		if err := p.processTraces(ctx, &blockTrace); err != nil {
			return fmt.Errorf("failed to process traces: %w", err)
		}
	}

	// Record successful processing duration
	p.metrics.ObserveBlockProcessingDuration(time.Since(start).Seconds())

	return nil
}

// processTraces writes each trace as an internal transaction to ClickHouse
func (p *CorethTracesProcessor) processTraces(
	ctx context.Context,
	blockTrace *kafkamsg.EVMBlockTrace,
) error {
	for _, rawTrace := range blockTrace.Traces {
		txHash, traces, err := GetTracesForTransaction(rawTrace)
		if err != nil {
			return fmt.Errorf("failed to get traces for transaction: %w", err)
		}

		for _, trace := range traces {
			txRow := &evmrepo.InternalTransactionRow{
				BlockchainID:    blockTrace.BlockchainID,
				EVMChainID:      blockTrace.EVMChainID,
				BlockNumber:     blockTrace.BlockNumber,
				TransactionHash: txHash,
				Type:            trace.Type,
				From:            trace.From,
				To:              trace.To,
				Value:           trace.Value,
				Gas:             trace.Gas,
				GasUsed:         trace.GasUsed,
				Revert:          trace.Revert,
				Error:           trace.Error,
				RevertReason:    trace.RevertReason,
				Input:           trace.Input,
				Output:          trace.Output,
				CallIndex:       trace.CallIndex,
			}
			if err := p.internalTransactionsRepo.WriteInternalTransaction(ctx, txRow); err != nil {
				return fmt.Errorf("failed to write trace: %w", err)
			}
		}
	}

	p.log.Debugw("successfully wrote traces",
		"blockchainID", blockTrace.BlockchainID,
		"evmChainID", blockTrace.EVMChainID,
		"blockNumber", blockTrace.BlockNumber,
		"traceCount", len(blockTrace.Traces),
	)

	return nil
}
