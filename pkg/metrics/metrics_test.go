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

func TestMetrics_UpdateOffsetMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("normal_update", func(t *testing.T) {
		m.UpdateOffsetMetrics(0, 100, 150, 20)

		require.Equal(t, float64(100), testutil.ToFloat64(m.lastCommittedOffset.WithLabelValues("0")))
		require.Equal(t, float64(150), testutil.ToFloat64(m.latestProcessedOffset.WithLabelValues("0")))
		require.Equal(t, float64(20), testutil.ToFloat64(m.offsetWindowSize.WithLabelValues("0")))
		require.Equal(t, float64(50), testutil.ToFloat64(m.offsetLag.WithLabelValues("0")))
	})

	t.Run("zero_lag", func(t *testing.T) {
		m.UpdateOffsetMetrics(1, 200, 200, 5)

		require.Equal(t, float64(200), testutil.ToFloat64(m.lastCommittedOffset.WithLabelValues("1")))
		require.Equal(t, float64(200), testutil.ToFloat64(m.latestProcessedOffset.WithLabelValues("1")))
		require.Equal(t, float64(5), testutil.ToFloat64(m.offsetWindowSize.WithLabelValues("1")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.offsetLag.WithLabelValues("1")))
	})

	t.Run("negative_lag_clamped_to_zero", func(t *testing.T) {
		// Edge case: latestProcessed < lastCommitted shouldn't happen normally,
		// but the function should handle it gracefully by clamping to 0
		m.UpdateOffsetMetrics(2, 300, 250, 10)

		require.Equal(t, float64(300), testutil.ToFloat64(m.lastCommittedOffset.WithLabelValues("2")))
		require.Equal(t, float64(250), testutil.ToFloat64(m.latestProcessedOffset.WithLabelValues("2")))
		require.Equal(t, float64(10), testutil.ToFloat64(m.offsetWindowSize.WithLabelValues("2")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.offsetLag.WithLabelValues("2")), "negative lag should be clamped to 0")
	})

	t.Run("large_lag", func(t *testing.T) {
		m.UpdateOffsetMetrics(3, 1000, 5000, 100)

		require.Equal(t, float64(1000), testutil.ToFloat64(m.lastCommittedOffset.WithLabelValues("3")))
		require.Equal(t, float64(5000), testutil.ToFloat64(m.latestProcessedOffset.WithLabelValues("3")))
		require.Equal(t, float64(100), testutil.ToFloat64(m.offsetWindowSize.WithLabelValues("3")))
		require.Equal(t, float64(4000), testutil.ToFloat64(m.offsetLag.WithLabelValues("3")))
	})

	t.Run("multiple_partitions", func(t *testing.T) {
		// Update different partitions and verify they don't interfere
		m.UpdateOffsetMetrics(10, 100, 150, 10)
		m.UpdateOffsetMetrics(11, 200, 250, 20)
		m.UpdateOffsetMetrics(12, 300, 350, 30)

		require.Equal(t, float64(100), testutil.ToFloat64(m.lastCommittedOffset.WithLabelValues("10")))
		require.Equal(t, float64(50), testutil.ToFloat64(m.offsetLag.WithLabelValues("10")))

		require.Equal(t, float64(200), testutil.ToFloat64(m.lastCommittedOffset.WithLabelValues("11")))
		require.Equal(t, float64(50), testutil.ToFloat64(m.offsetLag.WithLabelValues("11")))

		require.Equal(t, float64(300), testutil.ToFloat64(m.lastCommittedOffset.WithLabelValues("12")))
		require.Equal(t, float64(50), testutil.ToFloat64(m.offsetLag.WithLabelValues("12")))
	})
}

func TestMetrics_RecordOffsetCommit(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("successful_commit", func(t *testing.T) {
		m.RecordOffsetCommit(0, nil, 0.05)

		successCount := testutil.ToFloat64(m.offsetCommits.WithLabelValues("0", "success"))
		require.Equal(t, float64(1), successCount)

		errorCount := testutil.ToFloat64(m.offsetCommits.WithLabelValues("0", "error"))
		require.Equal(t, float64(0), errorCount)

		// Verify histogram recorded the duration
		metricFamilies, err := reg.Gather()
		require.NoError(t, err)

		var found bool
		for _, mf := range metricFamilies {
			if mf.GetName() == "indexer_kafka_offset_commit_duration_seconds" {
				found = true
				for _, metric := range mf.GetMetric() {
					// Find the metric for partition 0
					labelMap := make(map[string]string)
					for _, label := range metric.GetLabel() {
						labelMap[label.GetName()] = label.GetValue()
					}
					if labelMap["partition"] == "0" {
						histogram := metric.GetHistogram()
						require.Equal(t, uint64(1), histogram.GetSampleCount())
					}
				}
			}
		}
		require.True(t, found, "commit duration histogram not found")
	})

	t.Run("failed_commit", func(t *testing.T) {
		m.RecordOffsetCommit(1, errors.New("kafka unavailable"), 1.5)

		successCount := testutil.ToFloat64(m.offsetCommits.WithLabelValues("1", "success"))
		require.Equal(t, float64(0), successCount)

		errorCount := testutil.ToFloat64(m.offsetCommits.WithLabelValues("1", "error"))
		require.Equal(t, float64(1), errorCount)
	})

	t.Run("multiple_commits_same_partition", func(t *testing.T) {
		m.RecordOffsetCommit(2, nil, 0.01)
		m.RecordOffsetCommit(2, nil, 0.02)
		m.RecordOffsetCommit(2, errors.New("timeout"), 2.0)
		m.RecordOffsetCommit(2, nil, 0.03)

		successCount := testutil.ToFloat64(m.offsetCommits.WithLabelValues("2", "success"))
		require.Equal(t, float64(3), successCount)

		errorCount := testutil.ToFloat64(m.offsetCommits.WithLabelValues("2", "error"))
		require.Equal(t, float64(1), errorCount)
	})

	t.Run("multiple_partitions", func(t *testing.T) {
		m.RecordOffsetCommit(10, nil, 0.1)
		m.RecordOffsetCommit(11, errors.New("error"), 0.2)
		m.RecordOffsetCommit(12, nil, 0.3)

		require.Equal(t, float64(1), testutil.ToFloat64(m.offsetCommits.WithLabelValues("10", "success")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.offsetCommits.WithLabelValues("10", "error")))

		require.Equal(t, float64(0), testutil.ToFloat64(m.offsetCommits.WithLabelValues("11", "success")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.offsetCommits.WithLabelValues("11", "error")))

		require.Equal(t, float64(1), testutil.ToFloat64(m.offsetCommits.WithLabelValues("12", "success")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.offsetCommits.WithLabelValues("12", "error")))
	})

	t.Run("various_durations", func(t *testing.T) {
		// Record commits with various durations to test histogram buckets
		m.RecordOffsetCommit(20, nil, 0.001) // 1ms
		m.RecordOffsetCommit(20, nil, 0.01)  // 10ms
		m.RecordOffsetCommit(20, nil, 0.1)   // 100ms
		m.RecordOffsetCommit(20, nil, 1.0)   // 1s
		m.RecordOffsetCommit(20, nil, 5.0)   // 5s

		// Verify all were recorded
		successCount := testutil.ToFloat64(m.offsetCommits.WithLabelValues("20", "success"))
		require.Equal(t, float64(5), successCount)
	})
}

func TestMetrics_RecordOffsetInsert(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("single_insert", func(t *testing.T) {
		m.RecordOffsetInsert(0)

		count := testutil.ToFloat64(m.offsetInserts.WithLabelValues("0"))
		require.Equal(t, float64(1), count)
	})

	t.Run("multiple_inserts_same_partition", func(t *testing.T) {
		m.RecordOffsetInsert(1)
		m.RecordOffsetInsert(1)
		m.RecordOffsetInsert(1)
		m.RecordOffsetInsert(1)
		m.RecordOffsetInsert(1)

		count := testutil.ToFloat64(m.offsetInserts.WithLabelValues("1"))
		require.Equal(t, float64(5), count)
	})

	t.Run("multiple_partitions", func(t *testing.T) {
		m.RecordOffsetInsert(10)
		m.RecordOffsetInsert(10)
		m.RecordOffsetInsert(11)
		m.RecordOffsetInsert(11)
		m.RecordOffsetInsert(11)
		m.RecordOffsetInsert(12)

		require.Equal(t, float64(2), testutil.ToFloat64(m.offsetInserts.WithLabelValues("10")))
		require.Equal(t, float64(3), testutil.ToFloat64(m.offsetInserts.WithLabelValues("11")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.offsetInserts.WithLabelValues("12")))
	})

	t.Run("high_throughput_simulation", func(t *testing.T) {
		// Simulate high-throughput scenario with many inserts
		for i := 0; i < 1000; i++ {
			m.RecordOffsetInsert(100)
		}

		count := testutil.ToFloat64(m.offsetInserts.WithLabelValues("100"))
		require.Equal(t, float64(1000), count)
	})
}

func TestMetrics_OffsetMethods_NilReceiver(t *testing.T) {
	// All offset methods should handle nil receiver gracefully (no panic)
	var m *Metrics

	require.NotPanics(t, func() {
		m.UpdateOffsetMetrics(0, 100, 200, 50)
	})

	require.NotPanics(t, func() {
		m.RecordOffsetCommit(0, nil, 0.5)
	})

	require.NotPanics(t, func() {
		m.RecordOffsetCommit(0, errors.New("error"), 1.0)
	})

	require.NotPanics(t, func() {
		m.RecordOffsetInsert(0)
	})
}

func TestMetrics_RecordPartitionAssignment(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("single_partition_assignment", func(t *testing.T) {
		m.RecordPartitionAssignment([]int32{0})

		// Verify rebalance event recorded
		require.Equal(t, float64(1), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("assigned")))

		// Verify partition assignment recorded
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionAssignments.WithLabelValues("0")))

		// Verify assigned partitions gauge
		require.Equal(t, float64(1), testutil.ToFloat64(m.assignedPartitions))
	})

	t.Run("multiple_partitions_assignment", func(t *testing.T) {
		m.RecordPartitionAssignment([]int32{0, 1, 2})

		// Verify rebalance event counter incremented
		require.Equal(t, float64(2), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("assigned")))

		// Verify each partition assignment recorded
		require.Equal(t, float64(2), testutil.ToFloat64(m.partitionAssignments.WithLabelValues("0")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionAssignments.WithLabelValues("1")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionAssignments.WithLabelValues("2")))

		// Verify assigned partitions gauge updated to latest count
		require.Equal(t, float64(3), testutil.ToFloat64(m.assignedPartitions))
	})

	t.Run("repeated_assignments_same_partition", func(t *testing.T) {
		// Simulate a partition being assigned multiple times (e.g., across rebalances)
		m.RecordPartitionAssignment([]int32{5})
		m.RecordPartitionAssignment([]int32{5})
		m.RecordPartitionAssignment([]int32{5})

		// Verify rebalance events incremented
		require.Equal(t, float64(5), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("assigned")))

		// Verify partition assignment counter incremented
		require.Equal(t, float64(3), testutil.ToFloat64(m.partitionAssignments.WithLabelValues("5")))

		// Verify assigned partitions gauge shows latest
		require.Equal(t, float64(1), testutil.ToFloat64(m.assignedPartitions))
	})

	t.Run("empty_assignment", func(t *testing.T) {
		m.RecordPartitionAssignment([]int32{})

		// Rebalance event should still be recorded
		require.Equal(t, float64(6), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("assigned")))

		// Gauge should be 0
		require.Equal(t, float64(0), testutil.ToFloat64(m.assignedPartitions))
	})
}

func TestMetrics_RecordPartitionRevocation(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("single_partition_revocation", func(t *testing.T) {
		m.RecordPartitionRevocation([]int32{0})

		// Verify rebalance event recorded
		require.Equal(t, float64(1), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("revoked")))

		// Verify partition revocation recorded
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionRevocations.WithLabelValues("0")))

		// Verify assigned partitions gauge cleared
		require.Equal(t, float64(0), testutil.ToFloat64(m.assignedPartitions))
	})

	t.Run("multiple_partitions_revocation", func(t *testing.T) {
		m.RecordPartitionRevocation([]int32{0, 1, 2})

		// Verify rebalance event counter incremented
		require.Equal(t, float64(2), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("revoked")))

		// Verify each partition revocation recorded
		require.Equal(t, float64(2), testutil.ToFloat64(m.partitionRevocations.WithLabelValues("0")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionRevocations.WithLabelValues("1")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionRevocations.WithLabelValues("2")))

		// Gauge should be cleared
		require.Equal(t, float64(0), testutil.ToFloat64(m.assignedPartitions))
	})

	t.Run("repeated_revocations_same_partition", func(t *testing.T) {
		// Simulate a partition being revoked multiple times (e.g., across rebalances)
		m.RecordPartitionRevocation([]int32{5})
		m.RecordPartitionRevocation([]int32{5})
		m.RecordPartitionRevocation([]int32{5})

		// Verify rebalance events incremented
		require.Equal(t, float64(5), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("revoked")))

		// Verify partition revocation counter incremented
		require.Equal(t, float64(3), testutil.ToFloat64(m.partitionRevocations.WithLabelValues("5")))

		// Gauge should still be 0
		require.Equal(t, float64(0), testutil.ToFloat64(m.assignedPartitions))
	})

	t.Run("empty_revocation", func(t *testing.T) {
		m.RecordPartitionRevocation([]int32{})

		// Rebalance event should still be recorded
		require.Equal(t, float64(6), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("revoked")))

		// Gauge should remain 0
		require.Equal(t, float64(0), testutil.ToFloat64(m.assignedPartitions))
	})
}

func TestMetrics_RebalanceScenario(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	// Simulate a typical rebalance scenario
	t.Run("initial_assignment", func(t *testing.T) {
		m.RecordPartitionAssignment([]int32{0, 1, 2})

		require.Equal(t, float64(1), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("assigned")))
		require.Equal(t, float64(3), testutil.ToFloat64(m.assignedPartitions))
	})

	t.Run("rebalance_revoke_then_assign", func(t *testing.T) {
		// First revoke all
		m.RecordPartitionRevocation([]int32{0, 1, 2})
		require.Equal(t, float64(1), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("revoked")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.assignedPartitions))

		// Then assign different partitions
		m.RecordPartitionAssignment([]int32{1, 2, 3, 4})
		require.Equal(t, float64(2), testutil.ToFloat64(m.rebalanceEvents.WithLabelValues("assigned")))
		require.Equal(t, float64(4), testutil.ToFloat64(m.assignedPartitions))
	})

	t.Run("verify_partition_counts", func(t *testing.T) {
		// Partition 0: assigned once, revoked once
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionAssignments.WithLabelValues("0")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionRevocations.WithLabelValues("0")))

		// Partition 1: assigned twice, revoked once
		require.Equal(t, float64(2), testutil.ToFloat64(m.partitionAssignments.WithLabelValues("1")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionRevocations.WithLabelValues("1")))

		// Partition 3: assigned once, never revoked
		require.Equal(t, float64(1), testutil.ToFloat64(m.partitionAssignments.WithLabelValues("3")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.partitionRevocations.WithLabelValues("3")))
	})
}

func TestMetrics_RebalanceMethods_NilReceiver(t *testing.T) {
	// All rebalance methods should handle nil receiver gracefully (no panic)
	var m *Metrics

	require.NotPanics(t, func() {
		m.RecordPartitionAssignment([]int32{0, 1, 2})
	})

	require.NotPanics(t, func() {
		m.RecordPartitionRevocation([]int32{0, 1, 2})
	})

	require.NotPanics(t, func() {
		m.RecordPartitionAssignment([]int32{})
	})

	require.NotPanics(t, func() {
		m.RecordPartitionRevocation(nil)
	})
}

func TestMetrics_ReceiptFetchInFlight(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	require.Equal(t, float64(0), testutil.ToFloat64(m.receiptFetchesInFlight))

	m.IncReceiptFetchInFlight()
	m.IncReceiptFetchInFlight()
	require.Equal(t, float64(2), testutil.ToFloat64(m.receiptFetchesInFlight))

	m.DecReceiptFetchInFlight()
	require.Equal(t, float64(1), testutil.ToFloat64(m.receiptFetchesInFlight))

	m.DecReceiptFetchInFlight()
	require.Equal(t, float64(0), testutil.ToFloat64(m.receiptFetchesInFlight))
}

func TestMetrics_RecordReceiptFetch(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("successful_fetch_with_logs", func(t *testing.T) {
		m.RecordReceiptFetch(nil, 0.05, 3)

		require.Equal(t, float64(1), testutil.ToFloat64(m.receiptsFetched.WithLabelValues("success")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.receiptsFetched.WithLabelValues("error")))
		require.Equal(t, float64(3), testutil.ToFloat64(m.logsFetched))
	})

	t.Run("failed_fetch", func(t *testing.T) {
		m.RecordReceiptFetch(errors.New("rpc failure"), 1.0, 0)

		require.Equal(t, float64(1), testutil.ToFloat64(m.receiptsFetched.WithLabelValues("error")))
	})

	t.Run("successful_fetch_zero_logs", func(t *testing.T) {
		m.RecordReceiptFetch(nil, 0.02, 0)

		require.Equal(t, float64(2), testutil.ToFloat64(m.receiptsFetched.WithLabelValues("success")))
		// logsFetched should remain unchanged (still 3 from first call)
		require.Equal(t, float64(3), testutil.ToFloat64(m.logsFetched))
	})

	t.Run("fetch_with_many_logs", func(t *testing.T) {
		m.RecordReceiptFetch(nil, 0.1, 50)

		require.Equal(t, float64(3), testutil.ToFloat64(m.receiptsFetched.WithLabelValues("success")))
		require.Equal(t, float64(53), testutil.ToFloat64(m.logsFetched)) // 3 + 50
	})
}

func TestMetrics_AddLogsProcessed(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("add_positive_count", func(t *testing.T) {
		m.AddLogsProcessed(10)
		require.Equal(t, float64(10), testutil.ToFloat64(m.logsProcessed))
	})

	t.Run("add_more", func(t *testing.T) {
		m.AddLogsProcessed(5)
		require.Equal(t, float64(15), testutil.ToFloat64(m.logsProcessed))
	})

	t.Run("zero_count_ignored", func(t *testing.T) {
		m.AddLogsProcessed(0)
		require.Equal(t, float64(15), testutil.ToFloat64(m.logsProcessed))
	})

	t.Run("negative_count_ignored", func(t *testing.T) {
		m.AddLogsProcessed(-5)
		require.Equal(t, float64(15), testutil.ToFloat64(m.logsProcessed))
	})
}

func TestMetrics_RecordMessageReceived(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("single_message", func(t *testing.T) {
		m.RecordMessageReceived(0)
		require.Equal(t, float64(1), testutil.ToFloat64(m.messagesReceived.WithLabelValues("0")))
	})

	t.Run("multiple_messages_same_partition", func(t *testing.T) {
		m.RecordMessageReceived(0)
		m.RecordMessageReceived(0)
		require.Equal(t, float64(3), testutil.ToFloat64(m.messagesReceived.WithLabelValues("0")))
	})

	t.Run("multiple_partitions", func(t *testing.T) {
		m.RecordMessageReceived(1)
		m.RecordMessageReceived(2)
		m.RecordMessageReceived(2)

		require.Equal(t, float64(1), testutil.ToFloat64(m.messagesReceived.WithLabelValues("1")))
		require.Equal(t, float64(2), testutil.ToFloat64(m.messagesReceived.WithLabelValues("2")))
		// Partition 0 untouched since last subtest
		require.Equal(t, float64(3), testutil.ToFloat64(m.messagesReceived.WithLabelValues("0")))
	})
}

func TestMetrics_RecordMessageProcessed(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("successful_processing", func(t *testing.T) {
		m.RecordMessageProcessed(0, nil, 0.05)

		require.Equal(t, float64(1), testutil.ToFloat64(m.messagesProcessed.WithLabelValues("0", "success")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.messagesProcessed.WithLabelValues("0", "error")))
	})

	t.Run("failed_processing", func(t *testing.T) {
		m.RecordMessageProcessed(0, errors.New("unmarshal error"), 0.1)

		require.Equal(t, float64(1), testutil.ToFloat64(m.messagesProcessed.WithLabelValues("0", "success")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.messagesProcessed.WithLabelValues("0", "error")))
	})

	t.Run("multiple_partitions", func(t *testing.T) {
		m.RecordMessageProcessed(1, nil, 0.02)
		m.RecordMessageProcessed(1, nil, 0.03)
		m.RecordMessageProcessed(2, errors.New("db error"), 1.5)

		require.Equal(t, float64(2), testutil.ToFloat64(m.messagesProcessed.WithLabelValues("1", "success")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.messagesProcessed.WithLabelValues("1", "error")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.messagesProcessed.WithLabelValues("2", "success")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.messagesProcessed.WithLabelValues("2", "error")))
	})

	t.Run("histogram_records_duration", func(t *testing.T) {
		metricFamilies, err := reg.Gather()
		require.NoError(t, err)

		var found bool
		for _, mf := range metricFamilies {
			if mf.GetName() == "indexer_consumer_message_processing_duration_seconds" {
				found = true
				// Should have observations across multiple partitions
				require.NotEmpty(t, mf.GetMetric())
			}
		}
		require.True(t, found, "message processing duration histogram not found")
	})
}

func TestMetrics_MessagesInFlight(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	require.Equal(t, float64(0), testutil.ToFloat64(m.messagesInFlight))

	m.IncMessagesInFlight()
	m.IncMessagesInFlight()
	m.IncMessagesInFlight()
	require.Equal(t, float64(3), testutil.ToFloat64(m.messagesInFlight))

	m.DecMessagesInFlight()
	require.Equal(t, float64(2), testutil.ToFloat64(m.messagesInFlight))

	m.DecMessagesInFlight()
	m.DecMessagesInFlight()
	require.Equal(t, float64(0), testutil.ToFloat64(m.messagesInFlight))
}

func TestMetrics_RecordDLQProduction(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("successful_publish", func(t *testing.T) {
		m.RecordDLQProduction(nil, 0.02)

		require.Equal(t, float64(1), testutil.ToFloat64(m.dlqProduced.WithLabelValues("success")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.dlqProduced.WithLabelValues("error")))
	})

	t.Run("failed_publish", func(t *testing.T) {
		m.RecordDLQProduction(errors.New("kafka unavailable"), 2.0)

		require.Equal(t, float64(1), testutil.ToFloat64(m.dlqProduced.WithLabelValues("success")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.dlqProduced.WithLabelValues("error")))
	})

	t.Run("multiple_publishes", func(t *testing.T) {
		m.RecordDLQProduction(nil, 0.01)
		m.RecordDLQProduction(nil, 0.03)
		m.RecordDLQProduction(errors.New("timeout"), 5.0)

		require.Equal(t, float64(3), testutil.ToFloat64(m.dlqProduced.WithLabelValues("success")))
		require.Equal(t, float64(2), testutil.ToFloat64(m.dlqProduced.WithLabelValues("error")))
	})

	t.Run("histogram_records_duration", func(t *testing.T) {
		metricFamilies, err := reg.Gather()
		require.NoError(t, err)

		var found bool
		for _, mf := range metricFamilies {
			if mf.GetName() == "indexer_consumer_dlq_production_duration_seconds" {
				found = true
				metric := mf.GetMetric()[0]
				histogram := metric.GetHistogram()
				require.Equal(t, uint64(5), histogram.GetSampleCount())
			}
		}
		require.True(t, found, "DLQ production duration histogram not found")
	})
}

func TestMetrics_RecordKafkaError(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	t.Run("fatal_error", func(t *testing.T) {
		m.RecordKafkaError(true)

		require.Equal(t, float64(1), testutil.ToFloat64(m.kafkaErrors.WithLabelValues("fatal")))
		require.Equal(t, float64(0), testutil.ToFloat64(m.kafkaErrors.WithLabelValues("non_fatal")))
	})

	t.Run("non_fatal_error", func(t *testing.T) {
		m.RecordKafkaError(false)

		require.Equal(t, float64(1), testutil.ToFloat64(m.kafkaErrors.WithLabelValues("fatal")))
		require.Equal(t, float64(1), testutil.ToFloat64(m.kafkaErrors.WithLabelValues("non_fatal")))
	})

	t.Run("multiple_errors", func(t *testing.T) {
		m.RecordKafkaError(false)
		m.RecordKafkaError(false)
		m.RecordKafkaError(true)

		require.Equal(t, float64(2), testutil.ToFloat64(m.kafkaErrors.WithLabelValues("fatal")))
		require.Equal(t, float64(3), testutil.ToFloat64(m.kafkaErrors.WithLabelValues("non_fatal")))
	})
}

func TestMetrics_IncreaseUnknownEventCount(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg)
	require.NoError(t, err)

	require.Equal(t, float64(0), testutil.ToFloat64(m.unknownEvents))

	m.IncreaseUnknownEventCount()
	require.Equal(t, float64(1), testutil.ToFloat64(m.unknownEvents))

	m.IncreaseUnknownEventCount()
	m.IncreaseUnknownEventCount()
	require.Equal(t, float64(3), testutil.ToFloat64(m.unknownEvents))
}

func TestMetrics_ConsumerMethods_NilReceiver(t *testing.T) {
	var m *Metrics

	require.NotPanics(t, func() {
		m.RecordMessageReceived(0)
	})

	require.NotPanics(t, func() {
		m.RecordMessageProcessed(0, nil, 0.5)
	})

	require.NotPanics(t, func() {
		m.RecordMessageProcessed(0, errors.New("error"), 1.0)
	})

	require.NotPanics(t, func() {
		m.IncMessagesInFlight()
	})

	require.NotPanics(t, func() {
		m.DecMessagesInFlight()
	})

	require.NotPanics(t, func() {
		m.RecordDLQProduction(nil, 0.1)
	})

	require.NotPanics(t, func() {
		m.RecordDLQProduction(errors.New("error"), 0.5)
	})

	require.NotPanics(t, func() {
		m.RecordKafkaError(true)
	})

	require.NotPanics(t, func() {
		m.RecordKafkaError(false)
	})

	require.NotPanics(t, func() {
		m.IncreaseUnknownEventCount()
	})

	require.NotPanics(t, func() {
		m.IncReceiptFetchInFlight()
	})

	require.NotPanics(t, func() {
		m.DecReceiptFetchInFlight()
	})

	require.NotPanics(t, func() {
		m.RecordReceiptFetch(nil, 0.1, 5)
	})

	require.NotPanics(t, func() {
		m.AddLogsProcessed(10)
	})
}
