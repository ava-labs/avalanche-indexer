package kafka

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PrometheusConsumerMetrics implements ConsumerMetrics using Prometheus.
// Register this with your Prometheus registry and export via /metrics endpoint.
type PrometheusConsumerMetrics struct {
	messagesProcessed  prometheus.Counter
	processingErrors   prometheus.Counter
	dlqPublished       prometheus.Counter
	droppedErrors      prometheus.Counter
	processingDuration prometheus.Histogram
	semaphoreWaitTime  prometheus.Histogram
}

// NewPrometheusConsumerMetrics creates a new metrics instance with sensible defaults.
// namespace: typically your service name (e.g. "avalanche_indexer")
// subsystem: typically "kafka_consumer"
// labels: additional labels (e.g. topic, consumer_group)
func NewPrometheusConsumerMetrics(namespace, subsystem string, labels prometheus.Labels) *PrometheusConsumerMetrics {
	return &PrometheusConsumerMetrics{
		messagesProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "messages_processed_total",
			Help:        "Total number of messages successfully processed",
			ConstLabels: labels,
		}),
		processingErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "processing_errors_total",
			Help:        "Total number of message processing errors",
			ConstLabels: labels,
		}),
		dlqPublished: promauto.NewCounter(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "dlq_published_total",
			Help:        "Total number of messages published to DLQ",
			ConstLabels: labels,
		}),
		droppedErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "dropped_errors_total",
			Help:        "Total number of errors dropped due to channel congestion",
			ConstLabels: labels,
		}),
		processingDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "processing_duration_seconds",
			Help:        "Message processing duration in seconds",
			ConstLabels: labels,
			Buckets:     []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		semaphoreWaitTime: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "semaphore_wait_duration_seconds",
			Help:        "Time spent waiting to acquire semaphore slot",
			ConstLabels: labels,
			Buckets:     []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
	}
}

func (m *PrometheusConsumerMetrics) IncMessagesProcessed() {
	m.messagesProcessed.Inc()
}

func (m *PrometheusConsumerMetrics) IncProcessingErrors() {
	m.processingErrors.Inc()
}

func (m *PrometheusConsumerMetrics) IncDLQPublished() {
	m.dlqPublished.Inc()
}

func (m *PrometheusConsumerMetrics) IncDroppedErrors() {
	m.droppedErrors.Inc()
}

func (m *PrometheusConsumerMetrics) ObserveProcessingDuration(duration time.Duration) {
	m.processingDuration.Observe(duration.Seconds())
}

func (m *PrometheusConsumerMetrics) ObserveSemaphoreWaitTime(duration time.Duration) {
	m.semaphoreWaitTime.Observe(duration.Seconds())
}

// Example Grafana queries for these metrics:
//
// Message throughput (per second):
//   rate(avalanche_indexer_kafka_consumer_messages_processed_total[1m])
//
// Error rate:
//   rate(avalanche_indexer_kafka_consumer_processing_errors_total[1m])
//
// DLQ rate:
//   rate(avalanche_indexer_kafka_consumer_dlq_published_total[1m])
//
// P99 processing latency:
//   histogram_quantile(0.99, rate(avalanche_indexer_kafka_consumer_processing_duration_seconds_bucket[5m]))
//
// Semaphore contention (P99 wait time):
//   histogram_quantile(0.99, rate(avalanche_indexer_kafka_consumer_semaphore_wait_duration_seconds_bucket[5m]))
//
// Success rate:
//   rate(avalanche_indexer_kafka_consumer_messages_processed_total[1m]) /
//   (rate(avalanche_indexer_kafka_consumer_messages_processed_total[1m]) + rate(avalanche_indexer_kafka_consumer_processing_errors_total[1m]))

// Example usage in your application:
//
// import (
//     "github.com/prometheus/client_golang/prometheus"
//     "github.com/ava-labs/avalanche-indexer/pkg/kafka"
// )
//
// func main() {
//     // Create metrics
//     metrics := kafka.NewPrometheusConsumerMetrics(
//         "avalanche_indexer",
//         "kafka_consumer",
//         prometheus.Labels{
//             "topic":          "blocks",
//             "consumer_group": "indexer-group",
//         },
//     )
//
//     // Create consumer with metrics
//     cfg := kafka.ConsumerConfigImproved{
//         Topic:            "blocks",
//         GroupID:          "indexer-group",
//         BootstrapServers: "localhost:9092",
//         MaxConcurrency:   10,
//         Metrics:          metrics,  // Inject metrics!
//     }
//
//     consumer, err := kafka.NewConsumerImproved(ctx, log, cfg, processor)
//     if err != nil {
//         log.Fatal(err)
//     }
//
//     // Start consumer
//     if err := consumer.Start(ctx); err != nil {
//         log.Fatal(err)
//     }
// }
//
// // Expose metrics endpoint:
// import "net/http"
// import "github.com/prometheus/client_golang/prometheus/promhttp"
//
// func main() {
//     // ... consumer setup ...
//
//     http.Handle("/metrics", promhttp.Handler())
//     go http.ListenAndServe(":9090", nil)
// }
