package processor

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

// CorethProcessor processes Kafka messages containing Coreth blocks.
type CorethProcessor struct {
	log *zap.SugaredLogger
}

// NewCorethProcessor creates a new CorethProcessor.
func NewCorethProcessor(log *zap.SugaredLogger) *CorethProcessor {
	return &CorethProcessor{log: log}
}

// Process parses a Kafka message into a Coreth Block and processes it.
func (p *CorethProcessor) Process(ctx context.Context, msg *confluentKafka.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if msg == nil || msg.Value == nil {
		return fmt.Errorf("received nil message or empty value")
	}

	var block coreth.Block
	if err := block.Unmarshal(msg.Value); err != nil {
		return fmt.Errorf("failed to unmarshal coreth block: %w", err)
	}

	p.log.Debugw("processed coreth block",
		"number", block.Number,
		"hash", block.Hash,
		"txCount", len(block.Transactions),
		"timestamp", block.Timestamp,
		"timestampMs", block.TimestampMs,
		"minDelayExcess", block.MinDelayExcess,
		"size", block.Size,
		"difficulty", block.Difficulty,
		"mixHash", block.MixHash,
		"nonce", block.Nonce,
		"logsBloom", block.LogsBloom,
		"miner", block.Miner,
		"stateRoot", block.StateRoot,
		"parentHash", block.ParentHash,
		"transactionsRoot", block.TransactionsRoot,
		"receiptsRoot", block.ReceiptsRoot,
		"uncleHash", block.UncleHash,
		"extraData", block.ExtraData,
		"parentBeaconBlockRoot", block.ParentBeaconBlockRoot,
		"excessBlobGas", block.ExcessBlobGas,
		"blobGasUsed", block.BlobGasUsed,
		"withdrawals", block.Withdrawals,
		"transactions", block.Transactions,
	)

	// TODO: Add block processing logic here (e.g., indexing, storage)

	return nil
}

// Ensure CorethProcessor implements Processor interface.
var _ Processor = (*CorethProcessor)(nil)
