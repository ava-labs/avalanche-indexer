package coreth

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ava-labs/avalanche-indexer/internal/chainclient"
	"github.com/ava-labs/avalanche-indexer/internal/types"
	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
)

// Client wraps the underlying RPC and eth clients.
type Client struct {
	rpc *rpc.Client
	eth *customethclient.Client
}

var _ chainclient.ChainClient = (*Client)(nil)

func New(ctx context.Context, url string) (*Client, error) {
	customtypes.Register()

	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dial coreth rpc: %w", err)
	}
	return &Client{
		rpc: c,
		eth: customethclient.New(c),
	}, nil
}

func (c *Client) BlockByNumber(ctx context.Context, number uint64) (types.Block, error) {
	n := new(big.Int).SetUint64(number)
	block, err := c.eth.BlockByNumber(ctx, n)
	if err != nil {
		return nil, fmt.Errorf("get block by number: %w", err)
	}
	return mapToInternalBlock(block), nil
}

// Close closes the underlying RPC client.
func (c *Client) Close() {
	c.rpc.Close()
}
