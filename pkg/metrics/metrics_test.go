package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestLabels_toPrometheusLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   Labels
		expected prometheus.Labels
	}{
		{
			name:     "empty labels",
			labels:   Labels{},
			expected: prometheus.Labels{},
		},
		{
			name: "all labels set",
			labels: Labels{
				EVMChainID:    43114,
				Environment:   "production",
				Region:        "us-east-1",
				CloudProvider: "aws",
			},
			expected: prometheus.Labels{
				"evm_chain_id":   "43114",
				"environment":    "production",
				"region":         "us-east-1",
				"cloud_provider": "aws",
			},
		},
		{
			name: "partial labels",
			labels: Labels{
				EVMChainID:  43114,
				Environment: "staging",
			},
			expected: prometheus.Labels{
				"evm_chain_id": "43114",
				"environment":  "staging",
			},
		},
		{
			name: "zero chain ID excluded",
			labels: Labels{
				EVMChainID:  0,
				Environment: "test",
			},
			expected: prometheus.Labels{
				"environment": "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.labels.toPrometheusLabels()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNew(t *testing.T) {
	reg := prometheus.NewRegistry()

	m, err := New(reg)
	require.NoError(t, err)
	require.NotNil(t, m)

	// Verify metrics are registered by checking the registry
	metricFamilies, err := reg.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metricFamilies)
}

func TestNewWithLabels(t *testing.T) {
	reg := prometheus.NewRegistry()

	labels := Labels{
		EVMChainID:  43114,
		Environment: "test",
	}

	m, err := NewWithLabels(reg, labels)
	require.NoError(t, err)
	require.NotNil(t, m)

	// Update a metric and verify the labels are applied
	m.UpdateWindowMetrics(100, 200, 50)

	metricFamilies, err := reg.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metricFamilies)

	// Find the lowest metric and verify labels
	for _, mf := range metricFamilies {
		if mf.GetName() == "indexer_lowest" {
			require.NotEmpty(t, mf.GetMetric())
			metric := mf.GetMetric()[0]

			// Check that our constant labels are present
			labelMap := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labelMap[label.GetName()] = label.GetValue()
			}
			require.Equal(t, "43114", labelMap["evm_chain_id"])
			require.Equal(t, "test", labelMap["environment"])
		}
	}
}

func TestNew_RegistrationError(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Register first instance
	_, err := New(reg)
	require.NoError(t, err)

	// Second registration should fail (duplicate metrics)
	m, err := New(reg)
	require.Nil(t, m, "expected nil metrics on duplicate registration")

	var alreadyRegistered prometheus.AlreadyRegisteredError
	require.ErrorAs(t, err, &alreadyRegistered)
}

func TestMetrics_NilReceiver(t *testing.T) {
	// All methods should handle nil receiver gracefully (no panic)
	var m *Metrics

	// These should not panic
	require.NotPanics(t, func() {
		m.IncError("test")
	})
	require.NotPanics(t, func() {
		m.CommitBlocks(10, 100, 200, 50)
	})
	require.NotPanics(t, func() {
		m.UpdateWindowMetrics(100, 200, 50)
	})
	require.NotPanics(t, func() {
		m.IncRPCInFlight()
	})
	require.NotPanics(t, func() {
		m.DecRPCInFlight()
	})
	require.NotPanics(t, func() {
		m.RecordRPCCall("eth_getBlock", nil, 0.5)
	})
	require.NotPanics(t, func() {
		m.ObserveBlockProcessingDuration(0.1)
	})
}

func TestMetrics_IncError(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	m.IncError(ErrTypeOutOfWindow)
	m.IncError(ErrTypeOutOfWindow)
	m.IncError(ErrTypeInvalidWatermark)

	// Verify counter values
	count := testutil.ToFloat64(m.errors.WithLabelValues(ErrTypeOutOfWindow))
	require.Equal(t, float64(2), count)

	count = testutil.ToFloat64(m.errors.WithLabelValues(ErrTypeInvalidWatermark))
	require.Equal(t, float64(1), count)
}

func TestMetrics_CommitBlocks(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	m.CommitBlocks(10, 100, 200, 50)

	// Verify LUB advances counter
	require.Equal(t, float64(1), testutil.ToFloat64(m.lubAdvances))

	// Verify blocks processed counter
	require.Equal(t, float64(10), testutil.ToFloat64(m.blocksProcessed))

	// Verify window metrics
	require.Equal(t, float64(100), testutil.ToFloat64(m.lowest))
	require.Equal(t, float64(200), testutil.ToFloat64(m.highest))
	require.Equal(t, float64(50), testutil.ToFloat64(m.processedSetSize))

	// Commit more blocks
	m.CommitBlocks(5, 110, 210, 45)

	require.Equal(t, float64(2), testutil.ToFloat64(m.lubAdvances))
	require.Equal(t, float64(15), testutil.ToFloat64(m.blocksProcessed))
	require.Equal(t, float64(110), testutil.ToFloat64(m.lowest))
}

func TestMetrics_UpdateWindowMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	m.UpdateWindowMetrics(500, 1000, 100)

	require.Equal(t, float64(500), testutil.ToFloat64(m.lowest))
	require.Equal(t, float64(1000), testutil.ToFloat64(m.highest))
	require.Equal(t, float64(100), testutil.ToFloat64(m.processedSetSize))

	// Update again
	m.UpdateWindowMetrics(600, 1100, 80)

	require.Equal(t, float64(600), testutil.ToFloat64(m.lowest))
	require.Equal(t, float64(1100), testutil.ToFloat64(m.highest))
	require.Equal(t, float64(80), testutil.ToFloat64(m.processedSetSize))
}

func TestMetrics_RPCInFlight(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	require.Equal(t, float64(0), testutil.ToFloat64(m.rpcInFlight))

	m.IncRPCInFlight()
	m.IncRPCInFlight()
	require.Equal(t, float64(2), testutil.ToFloat64(m.rpcInFlight))

	m.DecRPCInFlight()
	require.Equal(t, float64(1), testutil.ToFloat64(m.rpcInFlight))
}

func TestMetrics_RecordRPCCall(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	// Record successful call
	m.RecordRPCCall("eth_getBlockByNumber", nil, 0.05)

	count := testutil.ToFloat64(m.rpcCalls.WithLabelValues("eth_getBlockByNumber", "success"))
	require.Equal(t, float64(1), count)

	// Record failed call
	m.RecordRPCCall("eth_getBlockByNumber", errors.New("connection refused"), 1.0)

	count = testutil.ToFloat64(m.rpcCalls.WithLabelValues("eth_getBlockByNumber", "error"))
	require.Equal(t, float64(1), count)

	// Multiple successful calls
	m.RecordRPCCall("eth_getLogs", nil, 0.1)
	m.RecordRPCCall("eth_getLogs", nil, 0.2)

	count = testutil.ToFloat64(m.rpcCalls.WithLabelValues("eth_getLogs", "success"))
	require.Equal(t, float64(2), count)
}

func TestMetrics_ObserveBlockProcessingDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	m.ObserveBlockProcessingDuration(0.05)
	m.ObserveBlockProcessingDuration(0.1)
	m.ObserveBlockProcessingDuration(0.5)

	// Verify histogram has observations
	metricFamilies, err := reg.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range metricFamilies {
		if mf.GetName() == "indexer_block_processing_duration_seconds" {
			found = true
			metric := mf.GetMetric()[0]
			histogram := metric.GetHistogram()
			require.Equal(t, uint64(3), histogram.GetSampleCount())
		}
	}
	require.True(t, found, "histogram metric not found")
}

func TestNamespace(t *testing.T) {
	require.Equal(t, "indexer", Namespace)
}

func TestErrorTypeConstants(t *testing.T) {
	require.Equal(t, "out_of_window", ErrTypeOutOfWindow)
	require.Equal(t, "invalid_watermark", ErrTypeInvalidWatermark)
}
