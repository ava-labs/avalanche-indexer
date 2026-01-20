package processor

import (
	"context"
	"errors"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/models"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/types/coreth"
	"go.uber.org/zap"

	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// CorethProcessor unmarshals and logs Coreth blocks from Kafka messages.
// If a repository is provided, persists blocks to ClickHouse.
// Safe for concurrent use.
type CorethProcessor struct {
	log  *zap.SugaredLogger
	repo models.Repository
}

// NewCorethProcessor creates a new CorethProcessor with the given logger.
// If repo is provided, blocks will be persisted to ClickHouse after processing.
func NewCorethProcessor(log *zap.SugaredLogger, repo models.Repository) *CorethProcessor {
	return &CorethProcessor{
		log:  log,
		repo: repo,
	}
}

// Process unmarshals msg.Value into a Coreth Block and logs its details.
// Returns an error if msg or msg.Value is nil, or if unmarshaling fails.
// Currently only logs block data - actual indexing/storage logic is TODO.
func (p *CorethProcessor) Process(ctx context.Context, msg *cKafka.Message) error {
	if msg == nil || msg.Value == nil {
		return errors.New("received nil message or empty value")
	}

	var block coreth.Block
	if err := block.Unmarshal(msg.Value); err != nil {
		return fmt.Errorf("failed to unmarshal coreth block: %w", err)
	}

	p.log.Debugw("processing coreth block",
		"evmChainID", block.EVMChainID,
		"bcID", block.BlockchainID,
		"blockNumber", block.Number,
		"hash", block.Hash,
	)

	// Persist to ClickHouse if repository is configured
	if p.repo != nil {
		clickhouseBlock, err := models.ParseBlockFromJSON(msg.Value)
		if err != nil {
			return fmt.Errorf("failed to parse block for storage: %w", err)
		}

		if err := p.repo.WriteBlock(ctx, clickhouseBlock); err != nil {
			return fmt.Errorf("failed to write block to ClickHouse: %w", err)
		}

		p.log.Debugw("successfully persisted block to ClickHouse",
			"evmChainID", clickhouseBlock.EVMChainID,
			"bcID", clickhouseBlock.BlockchainID,
			"blockNumber", clickhouseBlock.BlockNumber,
			"hash", clickhouseBlock.Hash,
		)
	}

	return nil
}

// Compile-time check that CorethProcessor implements Processor.
var _ Processor = (*CorethProcessor)(nil)
