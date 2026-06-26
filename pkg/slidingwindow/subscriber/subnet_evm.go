package subscriber

import (
	"context"

	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/subnet-evm/ethclient"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
)

type SubnetEVM struct {
	log    *zap.SugaredLogger
	client ethclient.Client
}

func NewSubnetEVM(log *zap.SugaredLogger, client ethclient.Client) *SubnetEVM {
	return &SubnetEVM{
		log:    log,
		client: client,
	}
}

// Subscribe is BLOCKING. It subscribes to new heads and submits them to the
// manager, reconnecting with backoff on failure; it returns only when ctx is done.
func (s *SubnetEVM) Subscribe(ctx context.Context, capacity int, manager *slidingwindow.Manager) error {
	return runWithReconnect(ctx, s.log, capacity, manager,
		func(ctx context.Context, ch chan<- *types.Header) (subscription, error) {
			return s.client.SubscribeNewHead(ctx, ch)
		})
}
