package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const Namespace = "indexer"

// Metrics contains all indexer metrics.
// Create with New and pass the prometheus.Registerer.
type Metrics struct {
	// Sliding window state
	LUB              prometheus.Gauge
	HIB              prometheus.Gauge
	WindowSize       prometheus.Gauge
	ProcessedSetSize prometheus.Gauge

	// Processing counters
	BlocksProcessed prometheus.Counter
	BlocksMarked    prometheus.Counter
	LUBAdvances     prometheus.Counter
	Errors          *prometheus.CounterVec

	// RPC metrics
	RPCCalls    *prometheus.CounterVec
	RPCDuration *prometheus.HistogramVec
	RPCInFlight prometheus.Gauge

	// Processing latency
	BlockProcessingDuration prometheus.Histogram
}

// New creates and registers all indexer metrics.
// Returns an error if registration fails.
func New(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		// Sliding window gauges
		LUB: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "lub",
			Help:      "Lowest Unprocessed Block height",
		}),
		HIB: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "hib",
			Help:      "Highest Ingested Block height",
		}),
		WindowSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "window_size",
			Help:      "Current window size (HIB - LUB), represents backlog",
		}),
		ProcessedSetSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "processed_set_size",
			Help:      "Number of blocks in the in-memory processed set",
		}),

		// Processing counters
		BlocksProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "blocks_processed_total",
			Help:      "Total number of blocks processed and committed",
		}),
		BlocksMarked: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "blocks_marked_total",
			Help:      "Total number of blocks marked as processed",
		}),
		LUBAdvances: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "lub_advances_total",
			Help:      "Total number of times LUB was advanced",
		}),
		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "errors_total",
			Help:      "Total errors by type",
		}, []string{"type"}),

		// RPC metrics
		RPCCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "rpc",
			Name:      "calls_total",
			Help:      "Total RPC calls by method and status",
		}, []string{"method", "status"}),
		RPCDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "rpc",
			Name:      "duration_seconds",
			Help:      "RPC call duration in seconds",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"method"}),
		RPCInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "rpc",
			Name:      "in_flight",
			Help:      "Number of RPC calls currently in progress",
		}),

		// Processing latency
		BlockProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "block_processing_duration_seconds",
			Help:      "Time to process a single block end-to-end",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
	}

	// Register all metrics
	collectors := []prometheus.Collector{
		m.LUB,
		m.HIB,
		m.WindowSize,
		m.ProcessedSetSize,
		m.BlocksProcessed,
		m.BlocksMarked,
		m.LUBAdvances,
		m.Errors,
		m.RPCCalls,
		m.RPCDuration,
		m.RPCInFlight,
		m.BlockProcessingDuration,
	}

	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}

	return m, nil
}

// Error type constants for consistent labeling
const (
	ErrTypeRPC              = "rpc"
	ErrTypeOutOfWindow      = "out_of_window"
	ErrTypeInvalidWatermark = "invalid_watermark"
)

// RPC status constants
const (
	StatusSuccess = "success"
	StatusError   = "error"
)

// RecordRPCCall is a convenience method to record an RPC call outcome.
func (m *Metrics) RecordRPCCall(method string, err error, durationSeconds float64) {
	status := StatusSuccess
	if err != nil {
		status = StatusError
		m.Errors.WithLabelValues(ErrTypeRPC).Inc()
	}
	m.RPCCalls.WithLabelValues(method, status).Inc()
	m.RPCDuration.WithLabelValues(method).Observe(durationSeconds)
}

// UpdateWindowMetrics updates all sliding window related metrics.
func (m *Metrics) UpdateWindowMetrics(lub, hib uint64, processedSetSize int) {
	m.LUB.Set(float64(lub))
	m.HIB.Set(float64(hib))
	m.WindowSize.Set(float64(hib - lub))
	m.ProcessedSetSize.Set(float64(processedSetSize))
}
