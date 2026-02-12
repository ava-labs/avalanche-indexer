package subscriber

import (
	"context"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
)

type Subscriber interface {
	Subscribe(ctx context.Context, capacity int, manager *slidingwindow.Manager) error
}
