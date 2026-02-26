package metrics

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const Namespace = "indexer"

// Labels holds constant labels applied to all metrics.
// These are useful for distinguishing metrics from multiple indexer instances.
type Labels struct {
	EVMChainID    uint64 // EVM chain ID (e.g., 43114 for C-Chain mainnet)
	Environment   string // Deployment environment (e.g., "production", "staging", "development")
	Region        string // Cloud region (e.g., "us-east-1", "eu-west-1")
	CloudProvider string // Cloud provider (e.g., "aws", "oci", "gcp")
}

// toPrometheusLabels converts Labels to prometheus.Labels map.
// Only non-empty labels are included to avoid empty label values.
func (l Labels) toPrometheusLabels() prometheus.Labels {
	labels := prometheus.Labels{}
	if l.EVMChainID != 0 {
		labels["evm_chain_id"] = strconv.FormatUint(l.EVMChainID, 10)
	}
	if l.Environment != "" {
		labels["environment"] = l.Environment
	}
	if l.Region != "" {
		labels["region"] = l.Region
	}
	if l.CloudProvider != "" {
		labels["cloud_provider"] = l.CloudProvider
	}
	return labels
}

type Metrics struct {
	// Sliding window state
	lowest           prometheus.Gauge
	highest          prometheus.Gauge
	processedSetSize prometheus.Gauge

	// Processing counters
	blocksProcessed prometheus.Counter
	lubAdvances     prometheus.Counter
	errors          *prometheus.CounterVec

	// RPC metrics
	rpcCalls    *prometheus.CounterVec
	rpcDuration *prometheus.HistogramVec
	rpcInFlight prometheus.Gauge

	// Processing latency
	blockProcessingDuration prometheus.Histogram

	// Receipt metrics
	receiptsFetched        *prometheus.CounterVec
	receiptFetchDuration   prometheus.Histogram
	receiptFetchesInFlight prometheus.Gauge

	// Log metrics
	logsFetched   prometheus.Counter
	logsProcessed prometheus.Counter

	// Producer metrics
	producerMessages        *prometheus.CounterVec
	producerProduceDuration prometheus.Histogram
	producerErrors          *prometheus.CounterVec
	blockToPublishDuration  prometheus.Histogram

	// Retry/failure metrics
	blockRetries  prometheus.Counter
	blockFailures *prometheus.CounterVec

	// Kafka consumer metrics
	kafkaConsumerGroupLag            *prometheus.GaugeVec
	kafkaMessageSize                 *prometheus.HistogramVec
	consumerMessageProcessingLatency prometheus.Histogram

	// ClickHouse write metrics
	clickHouseWrites        *prometheus.CounterVec
	clickHouseWriteDuration *prometheus.HistogramVec
}

// New creates a new Metrics instance and registers all metrics with the provided registerer.
// Returns an error if any metric registration fails.
// For metrics with constant labels (e.g., evm_chain_id), use NewWithLabels instead.
func New(reg prometheus.Registerer) (*Metrics, error) {
	return NewWithLabels(reg, Labels{})
}

// NewWithLabels creates a new Metrics instance with constant labels applied to all metrics.
// This is useful when running multiple indexer instances and needing to filter by dimensions like evm_chain_id.
func NewWithLabels(reg prometheus.Registerer, labels Labels) (*Metrics, error) {
	// Wrap the registerer with constant labels if any are provided
	promLabels := labels.toPrometheusLabels()
	if len(promLabels) > 0 {
		reg = prometheus.WrapRegistererWith(promLabels, reg)
	}

	return newMetrics(reg)
}

// newMetrics is the internal constructor that creates and registers all metrics.
func newMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		lowest: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "lowest",
			Help:      "Lowest unprocessed block height (window lower bound)",
		}),
		highest: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "highest",
			Help:      "Highest ingested block height (window upper bound)",
		}),
		processedSetSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "processed_set_size",
			Help:      "Number of blocks in the in-memory processed set",
		}),
		blocksProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "blocks_processed_total",
			Help:      "Total number of blocks processed and committed",
		}),
		lubAdvances: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "lowest_advances_total",
			Help:      "Total number of times LUB was advanced",
		}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "errors_total",
			Help:      "Total errors by type",
		}, []string{"type"}),
		rpcCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "rpc",
			Name:      "calls_total",
			Help:      "Total RPC calls by method and status",
		}, []string{"method", "status"}),
		rpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "rpc",
			Name:      "duration_seconds",
			Help:      "RPC call duration in seconds",
			// Buckets cover typical RPC latencies: 1ms, 5ms, 10ms, 25ms, 50ms,
			// 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"method"}),
		rpcInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "rpc",
			Name:      "in_flight",
			Help:      "Number of RPC calls currently in progress",
		}),
		blockProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "block_processing_duration_seconds",
			Help:      "Time to process a single block end-to-end",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		receiptsFetched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "receipts",
			Name:      "fetched_total",
			Help:      "Total transaction receipts fetched by status",
		}, []string{"status"}),
		receiptFetchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "receipts",
			Name:      "fetch_duration_seconds",
			Help:      "Time to fetch all receipts for a block",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		receiptFetchesInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "receipts",
			Name:      "fetches_in_flight",
			Help:      "Number of receipt fetches currently in progress",
		}),
		logsFetched: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "logs",
			Name:      "fetched_total",
			Help:      "Total transaction logs fetched from receipts",
		}),
		logsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "logs",
			Name:      "processed_total",
			Help:      "Total transaction logs processed and persisted",
		}),
		producerMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "producer",
			Name:      "messages_total",
			Help:      "Total Kafka producer messages by status",
		}, []string{"status"}),
		producerProduceDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "producer",
			Name:      "produce_duration_seconds",
			Help:      "Time spent waiting for Kafka delivery acknowledgement",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		producerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "producer",
			Name:      "errors_total",
			Help:      "Total Kafka producer errors by type",
		}, []string{"type"}),
		blockToPublishDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "block_to_publish_duration_seconds",
			Help:      "Time from block fetch start to Kafka publish confirmation",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		blockRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "block_retries_total",
			Help:      "Total block retries after failed processing attempts",
		}),
		blockFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "block_failures_total",
			Help:      "Total block processing failures by stage",
		}, []string{"stage"}),
		kafkaConsumerGroupLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "kafka",
			Name:      "consumer_group_lag",
			Help:      "Kafka consumer group lag to partition high watermark",
		}, []string{"partition"}),
		kafkaMessageSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "kafka",
			Name:      "message_size_bytes",
			Help:      "Kafka message size in bytes by direction",
			Buckets: []float64{
				128, 256, 512, 1024, 2048, 4096, 8192,
				16384, 32768, 65536, 131072, 262144,
				524288, 1048576, 2097152, 4194304,
				8388608, 16777216, 33554432,
			},
		}, []string{"direction"}),
		consumerMessageProcessingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "consumer",
			Name:      "message_processing_duration_seconds",
			Help:      "Time from message subscription to processing/write completion",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		clickHouseWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "clickhouse_writes_total",
			Help:      "Total ClickHouse write attempts by table and status",
		}, []string{"table", "status"}),
		clickHouseWriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "clickhouse_write_duration_seconds",
			Help:      "ClickHouse write duration in seconds by table",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"table"}),
	}

	err := errors.Join(
		reg.Register(m.lowest),
		reg.Register(m.highest),
		reg.Register(m.processedSetSize),
		reg.Register(m.blocksProcessed),
		reg.Register(m.lubAdvances),
		reg.Register(m.errors),
		reg.Register(m.rpcCalls),
		reg.Register(m.rpcDuration),
		reg.Register(m.rpcInFlight),
		reg.Register(m.blockProcessingDuration),
		reg.Register(m.receiptsFetched),
		reg.Register(m.receiptFetchDuration),
		reg.Register(m.receiptFetchesInFlight),
		reg.Register(m.logsFetched),
		reg.Register(m.logsProcessed),
		reg.Register(m.producerMessages),
		reg.Register(m.producerProduceDuration),
		reg.Register(m.producerErrors),
		reg.Register(m.blockToPublishDuration),
		reg.Register(m.blockRetries),
		reg.Register(m.blockFailures),
		reg.Register(m.kafkaConsumerGroupLag),
		reg.Register(m.kafkaMessageSize),
		reg.Register(m.consumerMessageProcessingLatency),
		reg.Register(m.clickHouseWrites),
		reg.Register(m.clickHouseWriteDuration),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Error type constants for non-RPC errors (RPC errors are tracked via rpcCalls{status="error"}).
const (
	ErrTypeOutOfWindow      = "out_of_window"
	ErrTypeInvalidWatermark = "invalid_watermark"
)

// IncError increments the error counter for the given error type.
func (m *Metrics) IncError(errType string) {
	if m == nil {
		return
	}
	m.errors.WithLabelValues(errType).Inc()
}

// CommitBlocks records blocks being committed when LUB advances.
func (m *Metrics) CommitBlocks(count uint64, lub, hib uint64, processedSetSize int) {
	if m == nil {
		return
	}
	m.lubAdvances.Inc()
	m.blocksProcessed.Add(float64(count))
	m.UpdateWindowMetrics(lub, hib, processedSetSize)
}

// UpdateWindowMetrics updates sliding window state gauges.
func (m *Metrics) UpdateWindowMetrics(lowest, highest uint64, processedSetSize int) {
	if m == nil {
		return
	}
	m.lowest.Set(float64(lowest))
	m.highest.Set(float64(highest))
	m.processedSetSize.Set(float64(processedSetSize))
}

// IncRPCInFlight increments the in-flight RPC gauge.
func (m *Metrics) IncRPCInFlight() {
	if m == nil {
		return
	}
	m.rpcInFlight.Inc()
}

// DecRPCInFlight decrements the in-flight RPC gauge.
func (m *Metrics) DecRPCInFlight() {
	if m == nil {
		return
	}
	m.rpcInFlight.Dec()
}

// RecordRPCCall records an RPC call outcome.
func (m *Metrics) RecordRPCCall(method string, err error, durationSeconds float64) {
	if m == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	m.rpcCalls.WithLabelValues(method, status).Inc()
	m.rpcDuration.WithLabelValues(method).Observe(durationSeconds)
}

// ObserveBlockProcessingDuration records a block processing duration.
func (m *Metrics) ObserveBlockProcessingDuration(seconds float64) {
	if m == nil {
		return
	}
	m.blockProcessingDuration.Observe(seconds)
}

// IncReceiptFetchInFlight increments the in-flight receipt fetch gauge.
func (m *Metrics) IncReceiptFetchInFlight() {
	if m == nil {
		return
	}
	m.receiptFetchesInFlight.Inc()
}

// DecReceiptFetchInFlight decrements the in-flight receipt fetch gauge.
func (m *Metrics) DecReceiptFetchInFlight() {
	if m == nil {
		return
	}
	m.receiptFetchesInFlight.Dec()
}

// RecordReceiptFetch records a receipt fetch RPC call outcome with duration and log count.
func (m *Metrics) RecordReceiptFetch(err error, durationSeconds float64, logCount int) {
	if m == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	m.receiptsFetched.WithLabelValues(status).Inc()
	m.receiptFetchDuration.Observe(durationSeconds)
	if logCount > 0 {
		m.logsFetched.Add(float64(logCount))
	}
}

// AddLogsProcessed records logs that have been processed and persisted.
func (m *Metrics) AddLogsProcessed(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.logsProcessed.Add(float64(count))
}

// RecordProducerResult records a Kafka produce attempt duration and status.
func (m *Metrics) RecordProducerResult(err error, durationSeconds float64) {
	if m == nil {
		return
	}

	status := "success"
	if err != nil {
		status = "error"
		m.producerErrors.WithLabelValues(classifyProducerErrorType(err)).Inc()
	}

	m.producerMessages.WithLabelValues(status).Inc()
	m.producerProduceDuration.Observe(durationSeconds)
}

// ObserveBlockToPublishDuration records end-to-end block-to-kafka publish latency.
func (m *Metrics) ObserveBlockToPublishDuration(seconds float64) {
	if m == nil {
		return
	}
	m.blockToPublishDuration.Observe(seconds)
}

// IncBlockRetry increments the block retry counter.
func (m *Metrics) IncBlockRetry() {
	if m == nil {
		return
	}
	m.blockRetries.Inc()
}

// IncBlockFailure increments the block failure counter for a processing stage.
func (m *Metrics) IncBlockFailure(stage string) {
	if m == nil {
		return
	}
	if stage == "" {
		stage = "unknown"
	}
	m.blockFailures.WithLabelValues(stage).Inc()
}

// SetKafkaConsumerGroupLag sets true consumer lag for a partition.
func (m *Metrics) SetKafkaConsumerGroupLag(partition int32, lag int64) {
	if m == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}
	m.kafkaConsumerGroupLag.WithLabelValues(strconv.FormatInt(int64(partition), 10)).Set(float64(lag))
}

// DeleteKafkaConsumerGroupLag removes lag metric series for a partition.
func (m *Metrics) DeleteKafkaConsumerGroupLag(partition int32) {
	if m == nil {
		return
	}
	m.kafkaConsumerGroupLag.DeleteLabelValues(strconv.FormatInt(int64(partition), 10))
}

// ObserveKafkaMessageSize records the kafka message size in bytes.
func (m *Metrics) ObserveKafkaMessageSize(direction string, sizeBytes int) {
	if m == nil || sizeBytes < 0 {
		return
	}
	if direction == "" {
		direction = "unknown"
	}
	m.kafkaMessageSize.WithLabelValues(direction).Observe(float64(sizeBytes))
}

// ObserveConsumerMessageProcessingDuration records end-to-end consumer processing duration.
func (m *Metrics) ObserveConsumerMessageProcessingDuration(seconds float64) {
	if m == nil {
		return
	}
	m.consumerMessageProcessingLatency.Observe(seconds)
}

// RecordClickHouseWrite records ClickHouse write duration and status for a table.
func (m *Metrics) RecordClickHouseWrite(table string, err error, durationSeconds float64) {
	if m == nil {
		return
	}

	if table == "" {
		table = "unknown"
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	m.clickHouseWrites.WithLabelValues(table, status).Inc()
	m.clickHouseWriteDuration.WithLabelValues(table).Observe(durationSeconds)
}

func classifyProducerErrorType(err error) string {
	if err == nil {
		return "none"
	}

	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline_exceeded"
	}

	msg := strings.ToLower(err.Error())
	switch {
	case containsAll(msg, "broker", "not available"):
		return "broker_not_available"
	case containsAll(msg, "invalid", "message", "size"):
		return "invalid_message_size"
	case containsAll(msg, "invalid", "message"):
		return "invalid_message"
	case containsAll(msg, "unknown", "topic"):
		return "unknown_topic"
	case containsAll(msg, "authentication"):
		return "authentication"
	case containsAll(msg, "delivery failed"):
		return "delivery_failed"
	default:
		return "produce_failed"
	}
}

func containsAll(msg string, substrings ...string) bool {
	for _, sub := range substrings {
		if !strings.Contains(msg, sub) {
			return false
		}
	}
	return true
}
