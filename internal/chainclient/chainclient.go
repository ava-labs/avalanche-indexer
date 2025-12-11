package chainclient

import (
	"context"

	"github.com/ava-labs/avalanche-indexer/internal/types"
)

type ChainClient interface {
	GetBlockByNumber(ctx context.Context, height uint64) (types.Block, error)
}
