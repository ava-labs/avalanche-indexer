package worker

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
	"go.uber.org/zap"

	evmclient "github.com/ava-labs/coreth/plugin/evm/customethclient"
)

var registerCustomTypesOnce sync.Once

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
	ctx context.Context,
	url string,
	producer *kafka.Producer,
	topic string,
	evmChainID uint64,
	blockchainID string,
	log *zap.SugaredLogger,
	metrics *metrics.Metrics,
	receiptTimeout time.Duration,
) (*CorethWorker, error) {
	registerCustomTypesOnce.Do(func() {
		customtypes.Register()
	})

	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dial coreth rpc: %w", err)
	}

	return &CorethWorker{
		client:         evmclient.New(c),
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
	corethBlock, err := cw.GetBlock(ctx, height)
	if err != nil {
		return fmt.Errorf("fetch block failed %d: %w", height, err)
	}

	bytes, err := corethBlock.Marshal()
	if err != nil {
		return fmt.Errorf("serialize block failed %d: %w", height, err)
	}

	err = cw.producer.Produce(ctx, kafka.Msg{
		Topic: cw.topic,
		Value: bytes,
		Key:   []byte(corethBlock.Number.String()),
	})
	if err != nil {
		return fmt.Errorf("produce block failed %d: %w", height, err)
	}

	cw.log.Debugw("processed block",
		"height", corethBlock.Number.Uint64(),
		"hash", corethBlock.Hash,
		"txs", len(corethBlock.Transactions),
	)

	return nil
}

// GetBlock fetches the block and transaction logs from the coreth rpc
// and converts it to a messages.CorethBlock.
func (cw *CorethWorker) GetBlock(ctx context.Context, height uint64) (*messages.CorethBlock, error) {
	const method = "eth_getBlockByNumber"
	start := time.Now()

	if cw.metrics != nil {
		cw.metrics.IncRPCInFlight()
		defer cw.metrics.DecRPCInFlight()
	}

	h := new(big.Int).SetUint64(height)
	block, err := cw.client.BlockByNumber(ctx, h)

	if cw.metrics != nil {
		cw.metrics.RecordRPCCall(method, err, time.Since(start).Seconds())
	}

	if err != nil {
		return nil, fmt.Errorf("fetch block failed %d: %w", height, err)
	}

	corethBlock, err := messages.CorethBlockFromLibevm(block, cw.evmChainID, cw.blockchainID)
	if err != nil {
		return nil, fmt.Errorf("convert block failed %d: %w", height, err)
	}

	err = cw.FetchBlockReceipts(ctx, corethBlock.Transactions, block.Number().Int64())
	if err != nil {
		return nil, err
	}
	return corethBlock, nil
}

// FetchBlockReceipts fetches the receipts for the given transactions and block number.
func (cw *CorethWorker) FetchBlockReceipts(ctx context.Context, transactions []*messages.CorethTransaction, blockNumber int64) error {
	start := time.Now()
	if cw.metrics != nil {
		cw.metrics.IncReceiptFetchInFlight()
		defer cw.metrics.DecReceiptFetchInFlight()
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, cw.receiptTimeout)
	defer cancel()
	bn := rpc.BlockNumber(blockNumber)
	r, err := cw.client.BlockReceipts(ctxTimeout, rpc.BlockNumberOrHash{
		BlockNumber: &bn,
	})
	if err != nil {
		if cw.metrics != nil {
			cw.metrics.RecordReceiptFetch(err, time.Since(start).Seconds(), 0)
		}
		return fmt.Errorf("fetch block receipts failed for block %d: %w", blockNumber, err)
	}

	if len(r) != len(transactions) {
		err := fmt.Errorf("receipt count mismatch for block %d: got %d receipts, expected %d transactions",
			blockNumber, len(r), len(transactions))
		if cw.metrics != nil {
			cw.metrics.RecordReceiptFetch(err, time.Since(start).Seconds(), 0)
		}
		return err
	}

	logCount := 0
	for i, receipt := range r {
		transactions[i].Receipt = messages.CorethTxReceiptFromLibevm(receipt)
		logCount += len(receipt.Logs)
	}

	if cw.metrics != nil {
		cw.metrics.RecordReceiptFetch(nil, time.Since(start).Seconds(), logCount)
	}
	return nil
}
