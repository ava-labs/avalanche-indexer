package metrics

import (
	"errors"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	Namespace = "indexer"

	// Status label values for success/error metrics
	StatusSuccess = "success"
	StatusError   = "error"

	KafkaOffset   = "kafka_offset"
	KafkaConsumer = "kafka_consumer"
	Logs          = "logs"
	Receipts      = "receipts"
	Consumer      = "consumer"
)

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

	// Kafka offset manager metrics
	lastCommittedOffset   *prometheus.GaugeVec
	latestProcessedOffset *prometheus.GaugeVec
	offsetLag             *prometheus.GaugeVec
	offsetWindowSize      *prometheus.GaugeVec
	offsetCommits         *prometheus.CounterVec
	commitDuration        *prometheus.HistogramVec
	offsetInserts         *prometheus.CounterVec

	// Kafka consumer rebalance metrics
	rebalanceEvents      *prometheus.CounterVec
	partitionAssignments *prometheus.CounterVec
	partitionRevocations *prometheus.CounterVec
	assignedPartitions   prometheus.Gauge

	// Consumer message processing metrics
	messagesReceived          *prometheus.CounterVec   // by partition
	messagesProcessed         *prometheus.CounterVec   // by partition, status
	messageProcessingDuration *prometheus.HistogramVec // by partition
	messagesInFlight          prometheus.Gauge

	// DLQ production metrics
	dlqProduced           *prometheus.CounterVec // by status
	dlqProductionDuration prometheus.Histogram

	// Consumer error metrics
	kafkaErrors   *prometheus.CounterVec // by severity (fatal/non_fatal)
	unknownEvents prometheus.Counter     // total count of unknown events
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
			Subsystem: Receipts,
			Name:      "fetched_total",
			Help:      "Total transaction receipts fetched by status",
		}, []string{"status"}),
		receiptFetchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: Receipts,
			Name:      "fetch_duration_seconds",
			Help:      "Time to fetch all receipts for a block",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		receiptFetchesInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: Receipts,
			Name:      "fetches_in_flight",
			Help:      "Number of receipt fetches currently in progress",
		}),
		logsFetched: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: Logs,
			Name:      "fetched_total",
			Help:      "Total transaction logs fetched from receipts",
		}),
		logsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: Logs,
			Name:      "processed_total",
			Help:      "Total transaction logs processed and persisted",
		}),
		lastCommittedOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: KafkaOffset,
			Name:      "last_committed",
			Help:      "Last offset successfully committed to Kafka for each partition",
		}, []string{"partition"}),
		latestProcessedOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: KafkaOffset,
			Name:      "latest_processed",
			Help:      "Latest offset processed and inserted into commit window for each partition",
		}, []string{"partition"}),
		offsetLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: KafkaOffset,
			Name:      "lag",
			Help:      "Number of uncommitted offsets (latestProcessed - lastCommitted) for each partition",
		}, []string{"partition"}),
		offsetWindowSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: KafkaOffset,
			Name:      "window_size",
			Help:      "Number of offsets currently in the sliding window awaiting commit for each partition",
		}, []string{"partition"}),
		offsetCommits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: KafkaOffset,
			Name:      "commits_total",
			Help:      "Total number of offset commit attempts by partition and status",
		}, []string{"partition", "status"}),
		commitDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: KafkaOffset,
			Name:      "commit_duration_seconds",
			Help:      "Time taken to commit offsets to Kafka by partition",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"partition"}),
		offsetInserts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: KafkaOffset,
			Name:      "inserts_total",
			Help:      "Total number of offsets inserted into the commit window by partition",
		}, []string{"partition"}),
		rebalanceEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: KafkaConsumer,
			Name:      "rebalance_events_total",
			Help:      "Total number of consumer group rebalance events by type",
		}, []string{"type"}),
		partitionAssignments: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: KafkaConsumer,
			Name:      "partition_assignments_total",
			Help:      "Total number of times a partition has been assigned to this consumer",
		}, []string{"partition"}),
		partitionRevocations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: KafkaConsumer,
			Name:      "partition_revocations_total",
			Help:      "Total number of times a partition has been revoked from this consumer",
		}, []string{"partition"}),
		assignedPartitions: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: KafkaConsumer,
			Name:      "assigned_partitions",
			Help:      "Current number of partitions assigned to this consumer",
		}),

		// Consumer message processing metrics
		messagesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: Consumer,
			Name:      "messages_received_total",
			Help:      "Total number of messages polled from Kafka by partition",
		}, []string{"partition"}),
		messagesProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: Consumer,
			Name:      "messages_processed_total",
			Help:      "Total number of messages processed by partition and status",
		}, []string{"partition", "status"}),
		messageProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: Consumer,
			Name:      "message_processing_duration_seconds",
			Help:      "End-to-end message dispatch duration including processing, offset insertion, and DLQ publish by partition",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"partition"}),
		messagesInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: Consumer,
			Name:      "messages_in_flight",
			Help:      "Number of messages currently being processed",
		}),

		// DLQ production metrics
		dlqProduced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: Consumer,
			Name:      "dlq_produced_total",
			Help:      "Total number of messages published to the dead letter queue by status",
		}, []string{"status"}),
		dlqProductionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: Consumer,
			Name:      "dlq_production_duration_seconds",
			Help:      "Time taken to publish a message to the dead letter queue",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}),

		// Consumer error metrics
		kafkaErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: Consumer,
			Name:      "kafka_errors_total",
			Help:      "Total number of Kafka errors received by severity (fatal/non_fatal)",
		}, []string{"severity"}),
		unknownEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: Consumer,
			Name:      "unknown_events_total",
			Help:      "Total number of unknown events received by consumer",
		}),
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
		reg.Register(m.lastCommittedOffset),
		reg.Register(m.latestProcessedOffset),
		reg.Register(m.offsetLag),
		reg.Register(m.offsetWindowSize),
		reg.Register(m.offsetCommits),
		reg.Register(m.commitDuration),
		reg.Register(m.offsetInserts),
		reg.Register(m.rebalanceEvents),
		reg.Register(m.partitionAssignments),
		reg.Register(m.partitionRevocations),
		reg.Register(m.assignedPartitions),
		reg.Register(m.messagesReceived),
		reg.Register(m.messagesProcessed),
		reg.Register(m.messageProcessingDuration),
		reg.Register(m.messagesInFlight),
		reg.Register(m.dlqProduced),
		reg.Register(m.dlqProductionDuration),
		reg.Register(m.kafkaErrors),
		reg.Register(m.unknownEvents),
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
	status := StatusSuccess
	if err != nil {
		status = StatusError
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
	status := StatusSuccess
	if err != nil {
		status = StatusError
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

// UpdateOffsetMetrics updates all offset manager metrics for a partition.
func (m *Metrics) UpdateOffsetMetrics(partition int32, lastCommitted, latestProcessed int64, windowSize int) {
	if m == nil {
		return
	}
	partitionLabel := strconv.Itoa(int(partition))

	m.lastCommittedOffset.WithLabelValues(partitionLabel).Set(float64(lastCommitted))
	m.latestProcessedOffset.WithLabelValues(partitionLabel).Set(float64(latestProcessed))
	m.offsetWindowSize.WithLabelValues(partitionLabel).Set(float64(windowSize))

	lag := max(latestProcessed-lastCommitted, 0)
	m.offsetLag.WithLabelValues(partitionLabel).Set(float64(lag))
}

// RecordOffsetCommit records an offset commit attempt for a partition.
// Pass nil error for successful commits, non-nil for failures.
func (m *Metrics) RecordOffsetCommit(partition int32, err error, durationSeconds float64) {
	if m == nil {
		return
	}
	partitionLabel := strconv.Itoa(int(partition))

	status := StatusSuccess
	if err != nil {
		status = StatusError
	}

	m.offsetCommits.WithLabelValues(partitionLabel, status).Inc()
	m.commitDuration.WithLabelValues(partitionLabel).Observe(durationSeconds)
}

// RecordOffsetInsert records an offset being inserted into the commit window.
func (m *Metrics) RecordOffsetInsert(partition int32) {
	if m == nil {
		return
	}
	partitionLabel := strconv.Itoa(int(partition))
	m.offsetInserts.WithLabelValues(partitionLabel).Inc()
}

// RecordPartitionAssignment records when partitions are assigned during a consumer group rebalance.
// This tracks both the rebalance event and per-partition assignment counts.
func (m *Metrics) RecordPartitionAssignment(partitions []int32) {
	if m == nil {
		return
	}

	m.rebalanceEvents.WithLabelValues("assigned").Inc()

	for _, partition := range partitions {
		partitionLabel := strconv.Itoa(int(partition))
		m.partitionAssignments.WithLabelValues(partitionLabel).Inc()
	}

	m.assignedPartitions.Set(float64(len(partitions)))
}

// RecordPartitionRevocation records when partitions are revoked during a consumer group rebalance.
// This tracks both the rebalance event and per-partition revocation counts.
func (m *Metrics) RecordPartitionRevocation(partitions []int32) {
	if m == nil {
		return
	}

	m.rebalanceEvents.WithLabelValues("revoked").Inc()

	for _, partition := range partitions {
		partitionLabel := strconv.Itoa(int(partition))
		m.partitionRevocations.WithLabelValues(partitionLabel).Inc()
	}

	// Clear the assigned partitions gauge (will be updated on next assignment)
	m.assignedPartitions.Set(0)
}

// RecordMessageReceived increments the received counter when a message is polled from Kafka.
func (m *Metrics) RecordMessageReceived(partition int32) {
	if m == nil {
		return
	}
	m.messagesReceived.WithLabelValues(strconv.Itoa(int(partition))).Inc()
}

// RecordMessageProcessed records a message processing outcome with duration.
// Pass nil error for successful processing, non-nil for failures.
func (m *Metrics) RecordMessageProcessed(partition int32, err error, durationSeconds float64) {
	if m == nil {
		return
	}
	partitionLabel := strconv.Itoa(int(partition))

	status := StatusSuccess
	if err != nil {
		status = StatusError
	}

	m.messagesProcessed.WithLabelValues(partitionLabel, status).Inc()
	m.messageProcessingDuration.WithLabelValues(partitionLabel).Observe(durationSeconds)
}

// IncMessagesInFlight increments the in-flight message processing gauge.
func (m *Metrics) IncMessagesInFlight() {
	if m == nil {
		return
	}
	m.messagesInFlight.Inc()
}

// DecMessagesInFlight decrements the in-flight message processing gauge.
func (m *Metrics) DecMessagesInFlight() {
	if m == nil {
		return
	}
	m.messagesInFlight.Dec()
}

// RecordDLQProduction records a DLQ publish attempt with duration.
// Pass nil error for successful publishes, non-nil for failures.
func (m *Metrics) RecordDLQProduction(err error, durationSeconds float64) {
	if m == nil {
		return
	}
	status := StatusSuccess
	if err != nil {
		status = StatusError
	}
	m.dlqProduced.WithLabelValues(status).Inc()
	m.dlqProductionDuration.Observe(durationSeconds)
}

// RecordKafkaError records a Kafka error by severity.
// fatal=true for fatal errors, false for non-fatal.
func (m *Metrics) RecordKafkaError(fatal bool) {
	if m == nil {
		return
	}
	severity := "non_fatal"
	if fatal {
		severity = "fatal"
	}
	m.kafkaErrors.WithLabelValues(severity).Inc()
}

// IncreaseUnknownEventCount increases the unknown event counter.
func (m *Metrics) IncreaseUnknownEventCount() {
	if m == nil {
		return
	}
	m.unknownEvents.Inc()
}
