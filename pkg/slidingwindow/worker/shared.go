package worker

import (
	"errors"

	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
)

var (
	ErrReceiptCountMismatch = errors.New("receipt count mismatch")
	ErrReceiptFetchFailed   = errors.New("fetch block receipts failed")
	ErrBlockFetchFailed     = errors.New("fetch block failed")
	ErrTracesFetchFailed    = errors.New("fetch block traces failed")
)

// observeProducedMessageSize records the size of a Kafka message produced by a worker.
func observeProducedMessageSize(m *metrics.Metrics, sizeBytes int) {
	m.ObserveKafkaMessageSize(metrics.DirectionProduced, sizeBytes)
}
