package worker

import (
	metricslib "github.com/ava-labs/avalanche-indexer/pkg/metrics"
)

// observeProducedMessageSize records the size of a Kafka message produced by a worker.
func observeProducedMessageSize(m *metricslib.Metrics, sizeBytes int) {
	m.ObserveKafkaMessageSize(metricslib.DirectionProduced, sizeBytes)
}
