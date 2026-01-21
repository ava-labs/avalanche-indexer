package processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/models"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/types/coreth"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"go.uber.org/zap"

	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// CorethProcessor unmarshals and logs Coreth blocks from Kafka messages.
// If a repository is provided, persists blocks to ClickHouse.
// Safe for concurrent use.
type CorethProcessor struct {
	log     *zap.SugaredLogger
	repo    models.Repository
	metrics *metrics.Metrics // nil-safe: all Metrics methods handle nil receivers gracefully
}

// NewCorethProcessor creates a new CorethProcessor with the given logger.
// If repo is provided, blocks will be persisted to ClickHouse after processing.
// If metrics is provided, processing duration and errors will be recorded.
func NewCorethProcessor(log *zap.SugaredLogger, repo models.Repository, m *metrics.Metrics) *CorethProcessor {
	return &CorethProcessor{
		log:     log,
		repo:    repo,
		metrics: m,
	}
}

// Process unmarshals msg.Value into a Coreth Block and logs its details.
// Returns an error if msg or msg.Value is nil, or if unmarshaling fails.
// Records processing duration and errors to metrics if configured.
func (p *CorethProcessor) Process(ctx context.Context, msg *cKafka.Message) error {
	start := time.Now()

	if msg == nil || msg.Value == nil {
		p.metrics.IncError("coreth_nil_message")
		return errors.New("received nil message or empty value")
	}

	var block coreth.Block
	if err := block.Unmarshal(msg.Value); err != nil {
		p.metrics.IncError("coreth_unmarshal_error")
		return fmt.Errorf("failed to unmarshal coreth block: %w", err)
	}

	p.log.Debugw("processing coreth block",
		"chainID", block.ChainID,
		"blockNumber", block.Number,
		"hash", block.Hash,
	)

	// Persist to ClickHouse if repository is configured
	if p.repo != nil {
		clickhouseBlock, err := models.ParseBlockFromJSON(msg.Value)
		if err != nil {
			p.metrics.IncError("coreth_parse_error")
			return fmt.Errorf("failed to parse block for storage: %w", err)
		}

		if err := p.repo.WriteBlock(ctx, clickhouseBlock); err != nil {
			p.metrics.IncError("coreth_write_error")
			return fmt.Errorf("failed to write block to ClickHouse: %w", err)
		}

		p.log.Debugw("successfully persisted block to ClickHouse",
			"chainID", clickhouseBlock.ChainID,
			"blockNumber", clickhouseBlock.BlockNumber,
			"hash", clickhouseBlock.Hash,
		)
	}

	// Record successful processing duration
	p.metrics.ObserveBlockProcessingDuration(time.Since(start).Seconds())

	return nil
}

// Compile-time check that CorethProcessor implements Processor.
var _ Processor = (*CorethProcessor)(nil)
