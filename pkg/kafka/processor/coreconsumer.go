package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/data/dynamodb/evmrepo"
	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// CoreConsumerProcessor consumes EVMBlock Kafka messages and writes them
// to DynamoDB in the format that glacier-api expects.
// Safe for concurrent use.
type CoreConsumerProcessor struct {
	log     *zap.SugaredLogger
	repo    *evmrepo.Repository
	metrics *metrics.Metrics
}

// NewCoreConsumerProcessor creates a new CoreConsumerProcessor.
func NewCoreConsumerProcessor(
	log *zap.SugaredLogger,
	repo *evmrepo.Repository,
	m *metrics.Metrics,
) *CoreConsumerProcessor {
	if m == nil {
		m = metrics.NewNoOp()
	}
	return &CoreConsumerProcessor{
		log:     log,
		repo:    repo,
		metrics: m,
	}
}

// Process unmarshals an EVMBlock from a Kafka message and writes it to DynamoDB.
func (p *CoreConsumerProcessor) Process(ctx context.Context, msg *ckafka.Message) error {
	start := time.Now()

	if msg == nil || msg.Value == nil {
		p.metrics.IncError("coreconsumer_nil_message")
		return NonRetryable(ErrNilMessage)
	}

	var block kafkamsg.EVMBlock
	if err := json.Unmarshal(msg.Value, &block); err != nil {
		p.metrics.IncError("coreconsumer_unmarshal_error")
		return NonRetryable(fmt.Errorf("%w: %w", ErrUnmarshalBlock, err))
	}

	if block.BlockchainID == nil {
		return NonRetryable(fmt.Errorf("blockchainId is required"))
	}

	blockNumber := uint64(0)
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	p.log.Debugw("processing block for DynamoDB",
		"evmChainID", block.EVMChainID,
		"blockNumber", blockNumber,
		"txCount", len(block.Transactions),
	)

	// Write to DynamoDB
	// cumulativeTxs is set to 0 — tracking cumulative tx count would require
	// maintaining state across blocks which adds complexity. The legacy indexer
	// tracks this but glacier-api does not critically depend on it.
	if err := p.repo.WriteBlock(ctx, &block, 0); err != nil {
		p.metrics.IncError("coreconsumer_write_error")
		return fmt.Errorf("failed to write block %d to DynamoDB: %w", blockNumber, err)
	}

	p.metrics.ObserveBlockProcessingDuration(time.Since(start).Seconds())

	p.log.Debugw("successfully processed block for DynamoDB",
		"blockNumber", blockNumber,
		"duration", time.Since(start),
	)

	return nil
}

// Compile-time check that CoreConsumerProcessor implements Processor.
var _ Processor = (*CoreConsumerProcessor)(nil)
