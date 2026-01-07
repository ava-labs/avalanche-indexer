package worker

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ava-labs/avalanche-indexer/internal/metrics"
	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
)

type CorethWorker struct {
	client  *customethclient.Client
	metrics *metrics.Metrics
}

func NewCorethWorker(ctx context.Context, url string, m *metrics.Metrics) (*CorethWorker, error) {
	customtypes.Register()

	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dial coreth rpc: %w", err)
	}
	return &CorethWorker{
		client:  customethclient.New(c),
		metrics: m,
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

	fmt.Printf("processed block %d | hash=%s | txs=%d\n",
		height, block.Hash().Hex(), len(block.Transactions()))
	return nil
}
