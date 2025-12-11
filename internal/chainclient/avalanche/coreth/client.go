package coreth

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ava-labs/avalanche-indexer/internal/chainclient"
	"github.com/ava-labs/avalanche-indexer/internal/metrics"
	"github.com/ava-labs/avalanche-indexer/internal/types"
	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/coreth/rpc"
)

// Client wraps the underlying RPC and eth clients.
type Client struct {
	rpc     *rpc.Client
	eth     *customethclient.Client
	metrics *metrics.Metrics // nil if metrics disabled
}

var _ chainclient.ChainClient = (*Client)(nil)

// Option configures the Client.
type Option func(*Client)

// WithMetrics enables metrics collection for the client.
func WithMetrics(m *metrics.Metrics) Option {
	return func(c *Client) {
		c.metrics = m
	}
}

// New creates a new Coreth client.
func New(ctx context.Context, url string, opts ...Option) (*Client, error) {
	customtypes.Register()

	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dial coreth rpc: %w", err)
	}

	client := &Client{
		rpc: c,
		eth: customethclient.New(c),
	}

	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

func (c *Client) BlockByNumber(ctx context.Context, number uint64) (types.Block, error) {
	const method = "BlockByNumber"
	start := time.Now()

	if c.metrics != nil {
		c.metrics.RPCInFlight.Inc()
		defer c.metrics.RPCInFlight.Dec()
	}

	n := new(big.Int).SetUint64(number)
	block, err := c.eth.BlockByNumber(ctx, n)

	// Record metrics if enabled
	if c.metrics != nil {
		c.metrics.RecordRPCCall(method, err, time.Since(start).Seconds())
	}

	if err != nil {
		return nil, fmt.Errorf("get block by number %d: %w", number, err)
	}
	return mapToInternalBlock(block), nil
}

// Close closes the underlying RPC client.
func (c *Client) Close() {
	c.rpc.Close()
}
