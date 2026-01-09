package worker

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka"
	"github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
	evmclient "github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
	"go.uber.org/zap"
)

type CorethWorker struct {
	client   *evmclient.Client
	producer *kafka.Producer
	topic    string
	log      *zap.SugaredLogger
}

func NewCorethWorker(
	ctx context.Context,
	url string,
	producer *kafka.Producer,
	topic string,
	log *zap.SugaredLogger,
) (*CorethWorker, error) {
	customtypes.Register()

	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dial coreth rpc: %w", err)
	}
	return &CorethWorker{
		client:   evmclient.New(c),
		producer: producer,
		topic:    topic,
		log:      log,
	}, nil
}

func (w *CorethWorker) Process(ctx context.Context, height uint64) error {
	h := new(big.Int).SetUint64(height)
	block, err := w.client.BlockByNumber(ctx, h)
	if err != nil {
		return fmt.Errorf("fetch block %d: %w", height, err)
	}

	corethBlock, err := coreth.BlockFromLibevm(block)
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
