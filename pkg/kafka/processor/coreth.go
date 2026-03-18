package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ErrNilMessage is returned when a nil message or empty value is received.
var (
	ErrNilMessage     = errors.New("received nil message or empty value")
	ErrUnmarshalBlock = errors.New("failed to unmarshal coreth block")
)

const (
	clickhouseErrAuthenticationFailed = 516 // AUTHENTICATION_FAILED
	clickhouseErrAccessDenied         = 497 // ACCESS_DENIED
	clickhouseErrUnknownTable         = 60  // UNKNOWN_TABLE
	clickhouseErrUnknownDatabase      = 81  // UNKNOWN_DATABASE
)

// CorethProcessor unmarshals and logs Coreth blocks from Kafka messages.
// If repositories are provided, persists blocks, transactions, and logs to ClickHouse.
// Safe for concurrent use.
type CorethProcessor struct {
	log        *zap.SugaredLogger
	blocksRepo evmrepo.Blocks
	txsRepo    evmrepo.Transactions
	logsRepo   evmrepo.Logs
	metrics    *metrics.Metrics
}

// NewCorethProcessor creates a new CorethProcessor with the given logger.
// If repositories are provided, blocks, transactions, and logs will be persisted to ClickHouse.
func NewCorethProcessor(
	log *zap.SugaredLogger,
	blocksRepo evmrepo.Blocks,
	txsRepo evmrepo.Transactions,
	logsRepo evmrepo.Logs,
	m *metrics.Metrics,
) *CorethProcessor {
	if m == nil {
		m = metrics.NewNoOp()
	}
	return &CorethProcessor{
		log:        log,
		blocksRepo: blocksRepo,
		txsRepo:    txsRepo,
		logsRepo:   logsRepo,
		metrics:    m,
	}
}

// Process unmarshals msg.Value into a Coreth Block and logs its details.
// Returns an error if msg or msg.Value is nil, or if unmarshaling fails.
// Records processing duration and errors to metrics if configured.
func (p *CorethProcessor) Process(ctx context.Context, msg *ckafka.Message) error {
	start := time.Now()

	if msg == nil || msg.Value == nil {
		p.metrics.IncError("coreth_nil_message")
		return NonRetryable(ErrNilMessage)
	}

	var block kafkamsg.EVMBlock
	if err := json.Unmarshal(msg.Value, &block); err != nil {
		p.metrics.IncError("coreth_unmarshal_error")
		return NonRetryable(fmt.Errorf("%w: %w", ErrUnmarshalBlock, err))
	}

	if block.BlockchainID == nil {
		return NonRetryable(evmrepo.ErrBlockChainIDRequired)
	}

	p.log.Debugw("processing coreth block",
		"evmChainID", block.EVMChainID,
		"bcID", block.BlockchainID,
		"blockNumber", block.Number,
		"hash", block.Hash,
	)

	// Persist block to ClickHouse if repository is configured
	if p.blocksRepo != nil {
		blockRow, err := CorethBlockToBlockRow(&block)
		if err != nil {
			p.metrics.IncError("coreth_parse_error")
			return NonRetryable(fmt.Errorf("failed to parse block for storage: %w", err))
		}

		writeStart := time.Now()
		err = p.blocksRepo.WriteBlock(ctx, blockRow)
		recordClickHouseWrite(p.metrics, clickhouse.DefaultRawBlocksTableName, err, writeStart)
		if err != nil {
			p.metrics.IncError("coreth_write_error")
			return classifyWriteErr(fmt.Errorf("failed to write block to ClickHouse: %w", err))
		}

		p.log.Debugw("successfully persisted block to ClickHouse",
			"evmChainID", blockRow.EVMChainID,
			"blockchainID", blockRow.BlockchainID,
			"blockNumber", blockRow.BlockNumber,
			"hash", blockRow.Hash,
		)
	}

	if p.txsRepo != nil && len(block.Transactions) > 0 {
		if err := p.processTransactions(ctx, &block); err != nil {
			return err
		}
	}

	if p.logsRepo != nil && len(block.Transactions) > 0 {
		if err := p.processLogs(ctx, &block); err != nil {
			return err
		}
	}

	// Record successful end-to-end processing duration (block + transactions + logs)
	p.metrics.ObserveBlockProcessingDuration(time.Since(start).Seconds())

	return nil
}

// CorethBlockToBlockRow converts a kafkamsg.EVMBlock to BlockRow.
// Exported for testing purposes.
func CorethBlockToBlockRow(block *kafkamsg.EVMBlock) (*evmrepo.BlockRow, error) {
	// Validate blockchain ID
	if block.BlockchainID == nil {
		return nil, evmrepo.ErrBlockChainIDRequired
	}

	// Set BlockchainID and EVMChainID (default EVMChainID to 0 if not set)
	blockchainID := block.BlockchainID
	evmChainID := block.EVMChainID
	if evmChainID == nil {
		evmChainID = big.NewInt(0)
	}

	// Default BlockNumber to 0 if not set
	blockNumber := block.Number
	if blockNumber == nil {
		blockNumber = big.NewInt(0)
	}

	// Set difficulty from big.Int (keep as *big.Int)
	var difficulty, totalDifficulty *big.Int
	if block.Difficulty != nil {
		difficulty = block.Difficulty
		// TotalDifficulty: for now use Difficulty, but this should be cumulative in production
		totalDifficulty = new(big.Int).Set(block.Difficulty)
	} else {
		difficulty = big.NewInt(0)
		totalDifficulty = big.NewInt(0)
	}

	blockRow := &evmrepo.BlockRow{
		BlockchainID:    blockchainID,
		EVMChainID:      evmChainID,
		BlockNumber:     blockNumber,
		Hash:            block.Hash,
		ParentHash:      block.ParentHash,
		BlockTime:       time.Unix(int64(block.Timestamp), 0).UTC(),
		TimestampMs:     block.TimestampMs,
		Miner:           block.Miner,
		Difficulty:      difficulty,
		TotalDifficulty: totalDifficulty,
		Size:            block.Size,
		GasLimit:        block.GasLimit,
		GasUsed:         block.GasUsed,
		BaseFeePerGas:   block.BaseFee,
		NumTxns:         uint32(len(block.Transactions)),
	}

	// Direct string assignments - no conversions needed
	blockRow.Hash = block.Hash
	blockRow.ParentHash = block.ParentHash
	blockRow.StateRoot = block.StateRoot
	blockRow.TransactionsRoot = block.TransactionsRoot
	blockRow.ReceiptsRoot = block.ReceiptsRoot
	blockRow.Sha3Uncles = block.UncleHash
	blockRow.MixHash = block.MixHash
	blockRow.Miner = block.Miner

	// Parse nonce - convert uint64 to hex string
	blockRow.Nonce = strconv.FormatUint(block.Nonce, 16)

	// Optional fields - keep as *big.Int
	if block.BaseFee != nil {
		blockRow.BaseFeePerGas = block.BaseFee
	} else {
		blockRow.BaseFeePerGas = big.NewInt(0)
	}
	// BlockGasCost defaults to 0 for now (not in kafkamsg.CorethBlock yet)
	blockRow.BlockGasCost = big.NewInt(0)
	if block.BlobGasUsed != nil {
		blockRow.BlobGasUsed = *block.BlobGasUsed
	}
	if block.ExcessBlobGas != nil {
		blockRow.ExcessBlobGas = *block.ExcessBlobGas
	}
	if block.ParentBeaconBlockRoot != "" {
		blockRow.ParentBeaconBlockRoot = block.ParentBeaconBlockRoot
	}
	if block.MinDelayExcess != 0 {
		blockRow.MinDelayExcess = block.MinDelayExcess
	}

	return blockRow, nil
}

// CorethTransactionToTransactionRow converts a kafkamsg.EVMTransaction to TransactionRow.
// Exported for testing purposes.
func CorethTransactionToTransactionRow(
	tx *kafkamsg.EVMTransaction,
	block *kafkamsg.EVMBlock,
	txIndex uint64,
) (*evmrepo.TransactionRow, error) {
	// Extract blockchain ID from block
	if block.BlockchainID == nil {
		return nil, evmrepo.ErrBlockChainIDRequiredForTx
	}

	// Extract block number
	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	// Set BlockchainID and EVMChainID from block (default EVMChainID to 0 if not set)
	blockchainID := block.BlockchainID
	evmChainID := block.EVMChainID
	if evmChainID == nil {
		evmChainID = big.NewInt(0)
	}

	// Determine number of logs from receipt
	var numLogs uint32
	if tx.Receipt != nil {
		numLogs = uint32(len(tx.Receipt.Logs))
	}

	txRow := &evmrepo.TransactionRow{
		BlockchainID:     blockchainID,
		EVMChainID:       evmChainID,
		BlockNumber:      blockNumber,
		BlockHash:        block.Hash,
		BlockTime:        time.Unix(int64(block.Timestamp), 0).UTC(),
		TimestampMs:      block.TimestampMs,
		Hash:             tx.Hash,
		From:             tx.From,
		Nonce:            tx.Nonce,
		Gas:              tx.Gas,
		Input:            tx.Input,
		Type:             tx.Type,
		TransactionIndex: txIndex,
		Success:          0, // TODO: Extract from transaction receipt when available in CorethBlock
		NumLogs:          numLogs,
	}

	// Handle nullable To field
	if tx.To != "" {
		txRow.To = &tx.To
	}

	// Set big.Int values directly (keep as *big.Int)
	if tx.Value != nil {
		txRow.Value = tx.Value
	} else {
		txRow.Value = big.NewInt(0)
	}

	if tx.GasPrice != nil {
		txRow.GasPrice = tx.GasPrice
	} else {
		txRow.GasPrice = big.NewInt(0)
	}

	// Handle nullable MaxFeePerGas
	if tx.MaxFeePerGas != nil {
		txRow.MaxFeePerGas = tx.MaxFeePerGas
	}

	// Handle nullable MaxPriorityFee
	if tx.MaxPriorityFee != nil {
		txRow.MaxPriorityFee = tx.MaxPriorityFee
	}

	return txRow, nil
}

// processTransactions converts transactions from a kafkamsg.CorethBlock to TransactionRow and writes
// them to ClickHouse
func (p *CorethProcessor) processTransactions(
	ctx context.Context,
	block *kafkamsg.EVMBlock,
) error {
	// TODO: Add batching (in a future PR)
	totalLogs := 0
	for i, tx := range block.Transactions {
		txRow, err := CorethTransactionToTransactionRow(tx, block, uint64(i))
		if err != nil {
			return NonRetryable(fmt.Errorf("failed to convert transaction %d: %w", i, err))
		}

		writeStart := time.Now()
		err = p.txsRepo.WriteTransaction(ctx, txRow)
		recordClickHouseWrite(p.metrics, clickhouse.DefaultRawTransactionsTableName, err, writeStart)
		if err != nil {
			return classifyWriteErr(fmt.Errorf("failed to write transaction %s: %w", tx.Hash, err))
		}

		// Count logs from this transaction's receipt
		if tx.Receipt != nil {
			totalLogs += len(tx.Receipt.Logs)
		}
	}

	p.metrics.AddLogsProcessed(totalLogs)

	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	p.log.Debugw("successfully wrote transactions",
		"blockchainID", block.BlockchainID,
		"evmChainID", block.EVMChainID,
		"blockNumber", blockNumber,
		"transactionCount", len(block.Transactions),
		"logCount", totalLogs,
	)

	return nil
}

// processLogs extracts logs from transaction receipts and writes them to ClickHouse
func (p *CorethProcessor) processLogs(
	ctx context.Context,
	block *kafkamsg.EVMBlock,
) error {
	totalLogs := 0
	for _, tx := range block.Transactions {
		if tx.Receipt == nil || len(tx.Receipt.Logs) == 0 {
			continue
		}

		for _, log := range tx.Receipt.Logs {
			logRow, err := CorethLogToLogRow(log, block)
			if err != nil {
				return NonRetryable(fmt.Errorf("failed to convert log: %w", err))
			}

			writeStart := time.Now()
			err = p.logsRepo.WriteLog(ctx, logRow)
			recordClickHouseWrite(p.metrics, clickhouse.DefaultRawLogsTableName, err, writeStart)
			if err != nil {
				return classifyWriteErr(fmt.Errorf("failed to write log (tx: %s, index: %d): %w", tx.Hash, log.Index, err))
			}
			totalLogs++
		}
	}

	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	p.log.Debugw("successfully wrote logs",
		"blockchainID", block.BlockchainID,
		"evmChainID", block.EVMChainID,
		"blockNumber", blockNumber,
		"logCount", totalLogs,
	)

	return nil
}

// CorethLogToLogRow converts a CorethLog to LogRow
// Exported for testing purposes
func CorethLogToLogRow(
	log *kafkamsg.EVMLog,
	block *kafkamsg.EVMBlock,
) (*evmrepo.LogRow, error) {
	if block.BlockchainID == nil {
		return nil, evmrepo.ErrBlockChainIDRequired
	}

	// Set BlockchainID and EVMChainID from block
	blockchainID := block.BlockchainID
	evmChainID := block.EVMChainID
	if evmChainID == nil {
		return nil, evmrepo.ErrEvmChainIDRequired
	}

	// Convert topics from []common.Hash to individual topic fields
	var topic0 string
	var topic1, topic2, topic3 *string
	if len(log.Topics) > 0 {
		topic0 = log.Topics[0].Hex()
	}
	if len(log.Topics) > 1 {
		t := log.Topics[1].Hex()
		topic1 = &t
	}
	if len(log.Topics) > 2 {
		t := log.Topics[2].Hex()
		topic2 = &t
	}
	if len(log.Topics) > 3 {
		t := log.Topics[3].Hex()
		topic3 = &t
	}

	return &evmrepo.LogRow{
		BlockchainID: blockchainID,
		EVMChainID:   evmChainID,
		BlockNumber:  log.BlockNumber,
		BlockHash:    log.BlockHash.Hex(),
		BlockTime:    time.Unix(int64(block.Timestamp), 0).UTC(),
		TimestampMs:  block.TimestampMs,
		TxHash:       log.TxHash.Hex(),
		TxIndex:      uint32(log.TxIndex),
		Address:      log.Address.Hex(),
		Topic0:       topic0,
		Topic1:       topic1,
		Topic2:       topic2,
		Topic3:       topic3,
		Data:         log.Data,
		LogIndex:     uint32(log.Index),
		Removed:      log.Removed,
	}, nil
}

// classifyWriteErr inspects a ClickHouse write error and wraps it as Fatal
// for permanent infrastructure failures (authentication, authorization),
// or returns it as-is (retryable by default) for transient errors.
//
// Error codes: https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
func classifyWriteErr(err error) error {
	var chErr *chdriver.Exception
	if errors.As(err, &chErr) {
		switch chErr.Code {
		case clickhouseErrAuthenticationFailed,
			clickhouseErrAccessDenied,
			clickhouseErrUnknownTable,
			clickhouseErrUnknownDatabase:
			return Fatal(err)
		}
	}
	return err
}

// recordClickHouseWrite records a ClickHouse write duration and status for a table.
func recordClickHouseWrite(m *metrics.Metrics, table string, err error, writeStart time.Time) {
	m.RecordClickHouseWrite(table, err, time.Since(writeStart).Seconds())
}

// Compile-time check that CorethProcessor implements Processor.
var _ Processor = (*CorethProcessor)(nil)
