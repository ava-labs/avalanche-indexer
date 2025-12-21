package worker

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
)

type CorethWorker struct {
	client *customethclient.Client
}

func NewCorethWorker(ctx context.Context, url string) (*CorethWorker, error) {
	customtypes.Register()

	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dial coreth rpc: %w", err)
	}
	return &CorethWorker{
		client: customethclient.New(c),
	}, nil
}

func (w *CorethWorker) Process(ctx context.Context, height uint64) error {
	h := new(big.Int).SetUint64(height)
	block, err := w.client.BlockByNumber(ctx, h)
	if err != nil {
		return fmt.Errorf("fetch block %d: %w", height, err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(300 * time.Millisecond):
	}

	fmt.Printf("processed block %d | hash=%s | txs=%d\n",
		height, block.Hash().Hex(), len(block.Transactions()))
	return nil
}
