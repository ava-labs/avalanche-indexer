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
// subscription; both client types satisfy it.
type subscription interface {
	Unsubscribe()
	Err() <-chan error
}

// subscribeNewHeadFn establishes a new-heads subscription, delivering headers
// on ch. It mirrors the client SubscribeNewHead methods.
type subscribeNewHeadFn func(ctx context.Context, ch chan<- *types.Header) (subscription, error)

// runWithReconnect subscribes to new heads and forwards heights to the manager,
// reconnecting with capped backoff on failure; it returns only when ctx is cancelled.
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

		// Reset backoff after a healthy session; keep backing off if we never
		// connected, to avoid hammering an RPC that is not yet live.
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

// subscribeOnce pumps headers into the manager until ctx is cancelled or the
// subscription errors; connected reports whether the subscription was established.
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
		case header, ok := <-ch:
			if !ok || header == nil {
				return true, errors.New("subscribe new heads: header channel closed")
			}
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
