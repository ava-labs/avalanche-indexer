package processor

import (
	"context"
	"errors"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/types/coreth"
	"go.uber.org/zap"

	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// CorethProcessor unmarshals and logs Coreth blocks from Kafka messages.
// Safe for concurrent use.
type CorethProcessor struct {
	log *zap.SugaredLogger
}

// NewCorethProcessor creates a new CorethProcessor with the given logger.
func NewCorethProcessor(log *zap.SugaredLogger) *CorethProcessor {
	return &CorethProcessor{log: log}
}

// Process unmarshals msg.Value into a Coreth Block and logs its details.
// Returns an error if msg or msg.Value is nil, or if unmarshaling fails.
// Currently only logs block data - actual indexing/storage logic is TODO.
//
//nolint:revive // ctx will be used in future indexing/storage implementation
func (p *CorethProcessor) Process(ctx context.Context, msg *cKafka.Message) error {
	if msg == nil || msg.Value == nil {
		return errors.New("received nil message or empty value")
	}

	var block coreth.Block
	if err := block.Unmarshal(msg.Value); err != nil {
		return fmt.Errorf("failed to unmarshal coreth block: %w", err)
	}

	p.log.Debugw("processed coreth block",
		"number", block.Number,
		"hash", block.Hash,
		"txCount", len(block.Transactions),
		"withdrawalCount", len(block.Withdrawals),
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

// Compile-time check that CorethProcessor implements Processor.
var _ Processor = (*CorethProcessor)(nil)
