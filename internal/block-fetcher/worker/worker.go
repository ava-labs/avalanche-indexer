package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/internal/chainclient"
	"github.com/ava-labs/avalanche-indexer/internal/types"
)

type Worker interface {
	Process(ctx context.Context, height uint64) error
}

type worker struct {
	rpcClient chainclient.ChainClient
}

func New(rpcClient chainclient.ChainClient) Worker {
	return &worker{
		rpcClient: rpcClient,
	}
}

func (w *worker) Process(ctx context.Context, height uint64) error {
	block, err := w.rpcClient.BlockByNumber(ctx, height)
	if err != nil {
		return fmt.Errorf("fetch block %d: %w", height, err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(300 * time.Millisecond):
	}

	switch block.GetType() {
	case types.BlockTypeEVM:
		evmBlock := block.(*types.EVMBlock)
		fmt.Printf("processed block %d | hash=%s | txs=%d\n",
			height, evmBlock.Hash.Hex(), len(evmBlock.Transactions))
	}

	return nil
}
