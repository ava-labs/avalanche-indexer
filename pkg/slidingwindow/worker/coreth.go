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
	"github.com/ava-labs/libevm/common"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	evmclient "github.com/ava-labs/coreth/plugin/evm/customethclient"
)

var registerCustomTypesOnce sync.Once

type CorethWorker struct {
	client                  *evmclient.Client
	producer                *kafka.Producer
	topic                   string
	evmChainID              *big.Int
	blockchainID            *string
	log                     *zap.SugaredLogger
	metrics                 *metrics.Metrics
	receiptConcurrencyLimit int64         // Maximum number of concurrent receipt fetches per block
	receiptTimeout          time.Duration // Timeout for fetching a transaction receipt
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
	receiptConcurrencyLimit int64,
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
		client:                  evmclient.New(c),
		producer:                producer,
		topic:                   topic,
		evmChainID:              new(big.Int).SetUint64(evmChainID),
		blockchainID:            &blockchainID,
		log:                     log,
		metrics:                 metrics,
		receiptConcurrencyLimit: receiptConcurrencyLimit,
		receiptTimeout:          receiptTimeout,
	}, nil
}

func (cw *CorethWorker) Process(ctx context.Context, height uint64) error {
	corethBlock, err := cw.GetBlock(ctx, height)
	if err != nil {
		return fmt.Errorf("fetch block %d: %w", height, err)
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

	err = cw.FetchReceipts(ctx, corethBlock.Transactions)
	if err != nil {
		return nil, fmt.Errorf("fetch receipts failed %d: %w", height, err)
	}
	return corethBlock, nil
}

// FetchReceipts fetches the receipts for the given transactions
// and populates the Receipt field of the transactions.
// It returns an error if any of the receipts fail to fetch.
func (cw *CorethWorker) FetchReceipts(
	ctx context.Context,
	txs []*messages.CorethTransaction,
) error {
	concurrencyLimiter := semaphore.NewWeighted(cw.receiptConcurrencyLimit)

	g, gctx := errgroup.WithContext(ctx)
	for _, tx := range txs {
		if err := concurrencyLimiter.Acquire(gctx, 1); err != nil {
			return err
		}
		thisTx := tx
		g.Go(func() error {
			defer concurrencyLimiter.Release(1)
			r, err := cw.tryFetchReceipt(gctx, thisTx.Hash)
			if err != nil {
				return err
			}
			thisTx.Receipt = r
			return nil
		})
	}
	return g.Wait()
}

// tryFetchReceipt performs the call to the coreth rpc to fetch
// the receipt for the given transaction hash.
func (cw *CorethWorker) tryFetchReceipt(
	ctx context.Context,
	tx string,
) (*messages.CorethTxReceipt, error) {
	txHash := common.HexToHash(tx)
	// ctxTimeout used to set timeouts separate from global context, rpc calls are prone to infinitely hang
	ctxTimeout, cancel := context.WithTimeout(ctx, cw.receiptTimeout)
	defer cancel()

	start := time.Now()
	if cw.metrics != nil {
		cw.metrics.IncReceiptFetchInFlight()
		defer cw.metrics.DecReceiptFetchInFlight()
	}

	r, err := cw.client.TransactionReceipt(ctxTimeout, txHash)
	if cw.metrics != nil {
		logCount := 0
		if r != nil {
			logCount = len(r.Logs)
		}
		cw.metrics.RecordReceiptFetch(err, time.Since(start).Seconds(), logCount)
	}
	if err != nil {
		return nil, fmt.Errorf("fetch receipt failed %s: %w", tx, err)
	}
	return messages.CorethTxReceiptFromLibevm(r), nil
}
