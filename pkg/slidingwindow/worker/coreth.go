package worker

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
	"go.uber.org/zap"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	evmclient "github.com/ava-labs/coreth/plugin/evm/customethclient"
)

var registerCustomTypesOnce sync.Once

type CorethWorker struct {
	client       *evmclient.Client
	producer     *kafka.Producer
	topic        string
	evmChainID   *big.Int
	blockchainID *string
	log          *zap.SugaredLogger
	metrics      *metrics.Metrics
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
) (*CorethWorker, error) {
	registerCustomTypesOnce.Do(func() {
		customtypes.Register()
	})

	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dial coreth rpc: %w", err)
	}

	return &CorethWorker{
		client:       evmclient.New(c),
		producer:     producer,
		topic:        topic,
		evmChainID:   new(big.Int).SetUint64(evmChainID),
		blockchainID: &blockchainID,
		log:          log,
		metrics:      metrics,
	}, nil
}

func (w *CorethWorker) Process(ctx context.Context, height uint64) error {
	const method = "eth_getBlockByNumber"
	start := time.Now()

	if w.metrics != nil {
		w.metrics.IncRPCInFlight()
		defer w.metrics.DecRPCInFlight()
	}

	h := new(big.Int).SetUint64(height)
	block, err := w.client.BlockByNumber(ctx, h)

	if w.metrics != nil {
		w.metrics.RecordRPCCall(method, err, time.Since(start).Seconds())
	}

	if err != nil {
		return fmt.Errorf("fetch block %d: %w", height, err)
	}

	corethBlock, err := kafkamsg.CorethBlockFromLibevm(block, w.evmChainID, w.blockchainID)
	if err != nil {
		return fmt.Errorf("convert block %d: %w", height, err)
	}

	bytes, err := corethBlock.Marshal()
	if err != nil {
		return fmt.Errorf("serialize block %d: %w", height, err)
	}

	err = w.producer.Produce(ctx, kafka.Msg{
		Topic: w.topic,
		Value: bytes,
		Key:   []byte(block.Number().String()),
	})
	if err != nil {
		return fmt.Errorf("failed to produce block %d: %w", height, err)
	}

	w.log.Debugw("processed block",
		"height", height,
		"hash", block.Hash().Hex(),
		"txs", len(block.Transactions()),
	)

	return nil
}
