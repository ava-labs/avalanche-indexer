package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

const Namespace = "indexer"

type Metrics struct {
	// Sliding window state
	lub              prometheus.Gauge
	hib              prometheus.Gauge
	windowSize       prometheus.Gauge
	processedSetSize prometheus.Gauge

	// Processing counters
	blocksProcessed prometheus.Counter
	blocksMarked    prometheus.Counter
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
func New(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		lub: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "lub",
			Help:      "Lowest Unprocessed Block height",
		}),
		hib: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "hib",
			Help:      "Highest Ingested Block height",
		}),
		windowSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "window_size",
			Help:      "Current window size (HIB - LUB), represents backlog",
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
		blocksMarked: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "blocks_marked_total",
			Help:      "Total number of blocks marked as processed",
		}),
		lubAdvances: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "lub_advances_total",
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
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
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
		reg.Register(m.lub),
		reg.Register(m.hib),
		reg.Register(m.windowSize),
		reg.Register(m.processedSetSize),
		reg.Register(m.blocksProcessed),
		reg.Register(m.blocksMarked),
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

// Error type constants.
const (
	ErrTypeRPC              = "rpc"
	ErrTypeOutOfWindow      = "out_of_window"
	ErrTypeInvalidWatermark = "invalid_watermark"
)

// IncError increments the error counter for the given error type.
func (m *Metrics) IncError(errType string) {
	m.errors.WithLabelValues(errType).Inc()
}

// MarkBlockProcessed records a block being marked as processed.
func (m *Metrics) MarkBlockProcessed(processedSetSize int) {
	m.blocksMarked.Inc()
	m.processedSetSize.Set(float64(processedSetSize))
}

// CommitBlocks records blocks being committed when LUB advances.
func (m *Metrics) CommitBlocks(count int, lub, hib uint64, processedSetSize int) {
	m.lubAdvances.Inc()
	m.blocksProcessed.Add(float64(count))
	m.UpdateWindowMetrics(lub, hib, processedSetSize)
}

// UpdateWindowMetrics updates sliding window state gauges.
func (m *Metrics) UpdateWindowMetrics(lub, hib uint64, processedSetSize int) {
	m.lub.Set(float64(lub))
	m.hib.Set(float64(hib))
	m.windowSize.Set(float64(hib - lub))
	m.processedSetSize.Set(float64(processedSetSize))
}

// IncRPCInFlight increments the in-flight RPC gauge.
func (m *Metrics) IncRPCInFlight() {
	m.rpcInFlight.Inc()
}

// DecRPCInFlight decrements the in-flight RPC gauge.
func (m *Metrics) DecRPCInFlight() {
	m.rpcInFlight.Dec()
}

// RecordRPCCall records an RPC call outcome.
func (m *Metrics) RecordRPCCall(method string, err error, durationSeconds float64) {
	status := "success"
	if err != nil {
		status = "error"
		m.errors.WithLabelValues(ErrTypeRPC).Inc()
	}
	m.rpcCalls.WithLabelValues(method, status).Inc()
	m.rpcDuration.WithLabelValues(method).Observe(durationSeconds)
}

// ObserveBlockProcessingDuration records a block processing duration.
func (m *Metrics) ObserveBlockProcessingDuration(seconds float64) {
	m.blockProcessingDuration.Observe(seconds)
}
