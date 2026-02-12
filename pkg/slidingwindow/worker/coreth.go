package worker

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"

	evmclient "github.com/ava-labs/coreth/plugin/evm/customethclient"
)

type CorethWorker struct {
	client         *evmclient.Client
	producer       *kafka.Producer
	topic          string
	evmChainID     *big.Int
	blockchainID   *string
	log            *zap.SugaredLogger
	metrics        *metrics.Metrics
	receiptTimeout time.Duration // Timeout for fetching block receipts
}

func NewCorethWorker(
	client *evmclient.Client,
	producer *kafka.Producer,
	topic string,
	evmChainID uint64,
	blockchainID string,
	log *zap.SugaredLogger,
	metrics *metrics.Metrics,
	receiptTimeout time.Duration,
) (*CorethWorker, error) {
	RegisterCustomTypesOnce.Do(func() {
		customtypes.Register()
	})

	return &CorethWorker{
		client:         client,
		producer:       producer,
		topic:          topic,
		evmChainID:     new(big.Int).SetUint64(evmChainID),
		blockchainID:   &blockchainID,
		log:            log,
		metrics:        metrics,
		receiptTimeout: receiptTimeout,
	}, nil
}

func (cw *CorethWorker) Process(ctx context.Context, height uint64) error {
	cw.log.Debugw("worker starting block processing", "height", height)

	corethBlock, err := cw.GetBlock(ctx, height)
	if err != nil {
		return fmt.Errorf("fetch block failed %d: %w", height, err)
	}

	cw.log.Debugw("block fetched, serializing", "height", height, "txs", len(corethBlock.Transactions))
	bytes, err := corethBlock.Marshal()
	if err != nil {
		return fmt.Errorf("serialize block failed %d: %w", height, err)
	}

	cw.log.Debugw("block serialized, producing to kafka", "height", height, "bytes", len(bytes))
	produceStart := time.Now()
	err = cw.producer.Produce(ctx, kafka.Msg{
		Topic: cw.topic,
		Value: bytes,
		Key:   []byte(corethBlock.Number.String()),
	})
	if err != nil {
		return fmt.Errorf("produce block failed %d: %w", height, err)
	}
	cw.log.Debugw("kafka produce completed", "height", height, "duration_ms", time.Since(produceStart).Milliseconds())

	cw.log.Debugw("processed block",
		"height", corethBlock.Number.Uint64(),
		"hash", corethBlock.Hash,
		"txs", len(corethBlock.Transactions),
	)

	return nil
}

// GetBlock fetches the block and transaction logs from the coreth rpc
// and converts it to a messages.CorethBlock.
func (cw *CorethWorker) GetBlock(ctx context.Context, height uint64) (*messages.EVMBlock, error) {
	const method = "eth_getBlockByNumber"
	start := time.Now()

	if cw.metrics != nil {
		cw.metrics.IncRPCInFlight()
		defer cw.metrics.DecRPCInFlight()
	}

	h := new(big.Int).SetUint64(height)
	cw.log.Debugw("calling eth_getBlockByNumber", "height", height)
	block, err := cw.client.BlockByNumber(ctx, h)
	rpcDuration := time.Since(start)

	if cw.metrics != nil {
		cw.metrics.RecordRPCCall(method, err, rpcDuration.Seconds())
	}

	if err != nil {
		cw.log.Warnw("eth_getBlockByNumber failed", "height", height, "error", err, "duration_ms", rpcDuration.Milliseconds())
		return nil, fmt.Errorf("fetch block failed %d: %w", height, err)
	}

	cw.log.Debugw("eth_getBlockByNumber succeeded", "height", height, "duration_ms", rpcDuration.Milliseconds(), "txs", len(block.Transactions()))

	corethBlock, err := messages.EVMBlockFromLibevmCoreth(block, cw.evmChainID, cw.blockchainID)
	if err != nil {
		return nil, fmt.Errorf("convert block failed %d: %w", height, err)
	}

	if len(corethBlock.Transactions) > 0 {
		cw.log.Debugw("fetching receipts", "height", height, "tx_count", len(corethBlock.Transactions), "receipt_timeout", cw.receiptTimeout)
		err = cw.FetchBlockReceipts(ctx, corethBlock.Transactions, block.Number().Int64())
		if err != nil {
			return nil, err
		}
	} else {
		cw.log.Debugw("no transactions, skipping receipt fetch", "height", height)
	}
	return corethBlock, nil
}

// FetchBlockReceipts fetches the receipts for the given transactions and block number.
func (cw *CorethWorker) FetchBlockReceipts(ctx context.Context, transactions []*messages.EVMTransaction, blockNumber int64) error {
	start := time.Now()
	if cw.metrics != nil {
		cw.metrics.IncReceiptFetchInFlight()
		defer cw.metrics.DecReceiptFetchInFlight()
	}

	cw.log.Debugw("calling BlockReceipts", "block", blockNumber, "timeout", cw.receiptTimeout)
	ctxTimeout, cancel := context.WithTimeout(ctx, cw.receiptTimeout)
	defer cancel()
	bn := rpc.BlockNumber(blockNumber)
	r, err := cw.client.BlockReceipts(ctxTimeout, rpc.BlockNumberOrHash{
		BlockNumber: &bn,
	})
	receiptDuration := time.Since(start)

	if err != nil {
		cw.log.Warnw("BlockReceipts failed", "block", blockNumber, "error", err, "duration_ms", receiptDuration.Milliseconds())
		if cw.metrics != nil {
			cw.metrics.RecordReceiptFetch(err, receiptDuration.Seconds(), 0)
		}
		return fmt.Errorf("%w for block %d: %w", ErrReceiptFetchFailed, blockNumber, err)
	}

	cw.log.Debugw("BlockReceipts succeeded", "block", blockNumber, "receipt_count", len(r), "duration_ms", receiptDuration.Milliseconds())

	if len(r) != len(transactions) {
		err := fmt.Errorf("%w for block %d: got %d receipts, expected %d transactions",
			ErrReceiptCountMismatch, blockNumber, len(r), len(transactions))
		cw.log.Warnw("receipt count mismatch", "block", blockNumber, "receipts", len(r), "transactions", len(transactions))
		if cw.metrics != nil {
			cw.metrics.RecordReceiptFetch(err, receiptDuration.Seconds(), 0)
		}
		return err
	}

	logCount := 0
	for i, receipt := range r {
		transactions[i].Receipt = messages.EVMTxReceiptFromLibevm(receipt)
		logCount += len(receipt.Logs)
	}

	if cw.metrics != nil {
		cw.metrics.RecordReceiptFetch(nil, time.Since(start).Seconds(), logCount)
	}
	return nil
}
