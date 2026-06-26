package subscriber

import (
	"context"

	"github.com/ava-labs/coreth/plugin/evm/customethclient"
	"github.com/ava-labs/libevm/core/types"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
)

type Coreth struct {
	log    *zap.SugaredLogger
	client *customethclient.Client
}

func NewCoreth(log *zap.SugaredLogger, client *customethclient.Client) *Coreth {
	return &Coreth{
		log:    log,
		client: client,
	}
}

// Subscribe is a BLOCKING function. It subscribes to new heads and submits them
// to the manager, reconnecting with backoff on failure. It returns only when
// ctx is done.
func (s *Coreth) Subscribe(ctx context.Context, capacity int, manager *slidingwindow.Manager) error {
	return runWithReconnect(ctx, s.log, capacity, manager,
		func(ctx context.Context, ch chan<- *types.Header) (subscription, error) {
			return s.client.SubscribeNewHead(ctx, ch)
		})
}
