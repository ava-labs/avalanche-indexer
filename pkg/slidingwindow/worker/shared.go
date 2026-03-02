package worker

import (
	"errors"
	"sync"

	metricslib "github.com/ava-labs/avalanche-indexer/pkg/metrics"
)

var (
	RegisterCustomTypesOnce sync.Once
	ErrReceiptCountMismatch = errors.New("receipt count mismatch")
	ErrReceiptFetchFailed   = errors.New("fetch block receipts failed")
	ErrBlockFetchFailed     = errors.New("fetch block failed")
	ErrTracesFetchFailed    = errors.New("fetch block traces failed")
)

// observeProducedMessageSize records the size of a Kafka message produced by a worker.
func observeProducedMessageSize(m *metricslib.Metrics, sizeBytes int) {
	m.ObserveKafkaMessageSize(metricslib.DirectionProduced, sizeBytes)
}
