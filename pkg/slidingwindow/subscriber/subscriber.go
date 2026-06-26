package subscriber

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ava-labs/libevm/core/types"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
)

type Subscriber interface {
	Subscribe(ctx context.Context, capacity int, manager *slidingwindow.Manager) error
}

const (
	// initialReconnectBackoff is the wait before the first reconnect attempt.
	initialReconnectBackoff = 1 * time.Second
	// maxReconnectBackoff caps the exponential backoff between reconnects.
	maxReconnectBackoff = 30 * time.Second
)

// subscription is the minimal surface we need from an eth new-heads
// subscription. Both the coreth and subnet-evm clients return concrete types
// that satisfy this interface.
type subscription interface {
	Unsubscribe()
	Err() <-chan error
}

// subscribeNewHeadFn establishes a new-heads subscription, delivering headers
// on ch. It mirrors the client SubscribeNewHead methods.
type subscribeNewHeadFn func(ctx context.Context, ch chan<- *types.Header) (subscription, error)

// runWithReconnect subscribes to new heads and forwards block heights to the
// manager, transparently reconnecting with capped exponential backoff whenever
// the subscription cannot be established or drops. It only returns when ctx is
// cancelled, so an RPC that is not yet live (or a transient connection reset)
// no longer crashes the fetcher; missed heights are recovered by backfill.
func runWithReconnect(
	ctx context.Context,
	log *zap.SugaredLogger,
	capacity int,
	manager *slidingwindow.Manager,
	subscribe subscribeNewHeadFn,
) error {
	backoff := initialReconnectBackoff
	for {
		connected, err := subscribeOnce(ctx, log, capacity, manager, subscribe)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// A previously healthy subscription that dropped should reconnect
		// promptly, so reset the backoff. A subscription that never connected
		// keeps backing off to avoid hammering an RPC that is not yet live.
		if connected {
			backoff = initialReconnectBackoff
		}

		log.Warnw("new heads subscription unavailable; reconnecting",
			"error", err, "retryIn", backoff.String())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff = nextBackoff(backoff)
	}
}

// subscribeOnce establishes a single subscription and pumps headers into the
// manager until ctx is cancelled or the subscription errors. The returned
// connected flag reports whether the subscription was successfully established
// before the error occurred.
func subscribeOnce(
	ctx context.Context,
	log *zap.SugaredLogger,
	capacity int,
	manager *slidingwindow.Manager,
	subscribe subscribeNewHeadFn,
) (connected bool, err error) {
	ch := make(chan *types.Header, capacity)
	sub, err := subscribe(ctx, ch)
	if err != nil {
		return false, fmt.Errorf("subscribe new heads: %w", err)
	}
	defer sub.Unsubscribe()

	log.Infow("subscribed to new heads")

	for {
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		case header := <-ch:
			h := header.Number.Uint64()
			log.Debugw("received new block from subscription", "height", h)
			if !manager.SubmitHeight(h) {
				log.Debugw("dropped realtime height; queued for backfill", "height", h)
			}
		case subErr := <-sub.Err():
			if subErr == nil {
				return true, errors.New("subscribe new heads: subscription closed")
			}
			return true, fmt.Errorf("subscribe new heads: %w", subErr)
		}
	}
}

// nextBackoff doubles the backoff, capped at maxReconnectBackoff.
func nextBackoff(d time.Duration) time.Duration {
	next := d * 2
	if next > maxReconnectBackoff {
		return maxReconnectBackoff
	}
	return next
}
