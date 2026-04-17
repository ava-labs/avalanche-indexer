package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/batchwriter"
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
	enableBatchWrites        bool
	batchWriter              *batchwriter.Writer
}

// Compile-time check that CorethTracesProcessor implements Processor.
var _ Processor = (*CorethTracesProcessor)(nil)

func NewCorethTracesProcessor(
	log *zap.SugaredLogger,
	internalTransactionsRepo evmrepo.InternalTransactions,
	bw *batchwriter.Writer,
	enableBatchWrites bool,
	m *metrics.Metrics,
) *CorethTracesProcessor {
	if m == nil {
		m = metrics.NewNoOp()
	}
	return &CorethTracesProcessor{
		log:                      log,
		internalTransactionsRepo: internalTransactionsRepo,
		metrics:                  m,
		batchWriter:              bw,
		enableBatchWrites:        enableBatchWrites,
	}
}

// Process unmarshals msg.Value into an EVMBlockTrace and logs its details.
// Returns an error if msg or msg.Value is nil, or if unmarshaling fails.
// Records processing duration and errors to metrics if configured.
func (p *CorethTracesProcessor) Process(ctx context.Context, msg *cKafka.Message) error {
	start := time.Now()

	if msg == nil || msg.Value == nil {
		p.metrics.IncError("coreth_traces_nil_message")
		return NonRetryable(ErrNilMessage)
	}

	var blockTrace kafkamsg.EVMBlockTrace
	if err := json.Unmarshal(msg.Value, &blockTrace); err != nil {
		p.metrics.IncError("coreth_traces_unmarshal_error")
		return NonRetryable(fmt.Errorf("%w: %w", ErrUnmarshalBlockTrace, err))
	}

	// Validate block trace (BlockchainID is required)
	if blockTrace.BlockchainID == nil {
		return NonRetryable(ErrMissingBlockchainID)
	}

	p.log.Debugw("processing coreth block trace",
		"evmChainID", blockTrace.EVMChainID,
		"bcID", blockTrace.BlockchainID,
		"blockNumber", blockTrace.BlockNumber,
		"traceCount", len(blockTrace.Traces),
	)

	if p.batchWriter != nil {
		return p.submitToBatchWriter(ctx, start, &blockTrace)
	}

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

// submitToBatchWriter converts all traces into internal transaction rows and
// submits them via the batch writer. Blocks until the batch is flushed.
func (p *CorethTracesProcessor) submitToBatchWriter(
	ctx context.Context,
	start time.Time,
	blockTrace *kafkamsg.EVMBlockTrace,
) error {
	var rows []*evmrepo.InternalTransactionRow
	for _, rawTrace := range blockTrace.Traces {
		txHash, traces, err := GetTracesForTransaction(rawTrace)
		if err != nil {
			return NonRetryable(fmt.Errorf("failed to get traces for transaction: %w", err))
		}

		for _, trace := range traces {
			rows = append(rows, &evmrepo.InternalTransactionRow{
				BlockchainID:    blockTrace.BlockchainID,
				EVMChainID:      blockTrace.EVMChainID,
				BlockNumber:     blockTrace.BlockNumber,
				BlockTime:       time.Unix(int64(blockTrace.BlockTimestamp), 0).UTC(),
				TimestampMs:     blockTrace.TimestampMs,
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
			})
		}
	}

	if len(rows) == 0 {
		return nil
	}

	req := &batchwriter.WriteRequest{
		InternalTxns: rows,
	}

	result := p.batchWriter.Submit(ctx, req)
	select {
	case err := <-result:
		if err != nil {
			return classifyWriteErr(fmt.Errorf("batch writer flush: %w", err))
		}
		p.metrics.ObserveBlockProcessingDuration(time.Since(start).Seconds())
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processTraces writes each trace as an internal transaction to ClickHouse
func (p *CorethTracesProcessor) processTraces(
	ctx context.Context,
	blockTrace *kafkamsg.EVMBlockTrace,
) error {
	var internalTransactions []*evmrepo.InternalTransactionRow
	for _, rawTrace := range blockTrace.Traces {
		txHash, traces, err := GetTracesForTransaction(rawTrace)
		if err != nil {
			return NonRetryable(fmt.Errorf("failed to get traces for transaction: %w", err))
		}

		for _, trace := range traces {
			txRow := &evmrepo.InternalTransactionRow{
				BlockchainID:    blockTrace.BlockchainID,
				EVMChainID:      blockTrace.EVMChainID,
				BlockNumber:     blockTrace.BlockNumber,
				BlockTime:       time.Unix(int64(blockTrace.BlockTimestamp), 0).UTC(),
				TimestampMs:     blockTrace.TimestampMs,
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
			internalTransactions = append(internalTransactions, txRow)
		}
	}

	if len(internalTransactions) == 0 {
		p.log.Debugw("no internal transactions to write",
			"blockchainID", blockTrace.BlockchainID,
			"evmChainID", blockTrace.EVMChainID,
			"blockNumber", blockTrace.BlockNumber,
			"traceCount", len(blockTrace.Traces),
		)
		return nil
	}

	if p.enableBatchWrites {
		err := p.internalTransactionsRepo.BatchInsertInternalTransactions(ctx, internalTransactions)
		if err != nil {
			return classifyWriteErr(fmt.Errorf("failed to batch insert internal transactions: %w", err))
		}
	} else {
		for _, tx := range internalTransactions {
			err := p.internalTransactionsRepo.WriteInternalTransaction(ctx, tx)
			if err != nil {
				return classifyWriteErr(fmt.Errorf("failed to write internal transaction: %w", err))
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
