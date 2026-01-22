package metrics

import (
	"errors"
	"strconv"

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
