package subscriber

import (
	"context"
	"fmt"

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

// Subscribe is a BLOCKING function. It subscribes to new heads and submits them to the manager.
// It returns when failed to subscribe, ctx is done or when the subscription errors.
func (s *Coreth) Subscribe(ctx context.Context, capacity int, manager *slidingwindow.Manager) error {
	ch := make(chan *types.Header, capacity)
	sub, err := s.client.SubscribeNewHead(ctx, ch)
	if err != nil {
		return fmt.Errorf("subscribe new heads: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			sub.Unsubscribe()
			return ctx.Err()
		case header := <-ch:
			h := header.Number.Uint64()
			s.log.Debugw("received new block from subscription", "height", h)
			if !manager.SubmitHeight(h) {
				s.log.Debugw("dropped realtime height; queued for backfill", "height", h)
			}
		case err := <-sub.Err():
			return fmt.Errorf("subscribe new heads: %w", err)
		}
	}
}
