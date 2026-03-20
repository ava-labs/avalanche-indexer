package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	dto "github.com/prometheus/client_model/go"
)

var errProcessing = errors.New("processing failed")

type mockProcessor struct {
	mu      sync.Mutex
	calls   int
	results []error
}

func (m *mockProcessor) Process(_ context.Context, _ *ckafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.calls
	m.calls++
	if idx >= len(m.results) {
		return m.results[len(m.results)-1]
	}
	return m.results[idx]
}

func (m *mockProcessor) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func newTestConsumer(t *testing.T, proc *mockProcessor, policy RetryPolicy) (*Consumer, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	require.NoError(t, err)

	return &Consumer{
		processor: proc,
		metrics:   m,
		cfg:       ConsumerConfig{Retry: policy}.WithDefaults(),
		log:       zap.NewNop().Sugar(),
	}, reg
}

func testMessage() *ckafka.Message {
	topic := "test-topic"
	return &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    42,
		},
		Value: []byte("test-value"),
	}
}

// gatherCounter returns the value of a counter metric by its fully-qualified
// name (namespace_subsystem_name) from the registry, or 0 if not found.
func gatherCounter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				if m.GetCounter() != nil {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func assertRetryMetrics(t *testing.T, reg *prometheus.Registry, wantRetries, wantExhausted float64) {
	t.Helper()
	assert.Equal(t, wantRetries, gatherCounter(t, reg, "indexer_consumer_message_retries_total"),
		"message_retries_total")
	assert.Equal(t, wantExhausted, gatherCounter(t, reg, "indexer_consumer_message_retries_exhausted_total"),
		"message_retries_exhausted_total")
}

func TestProcessWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	proc := &mockProcessor{results: []error{nil}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{MaxRetries: 3})

	err := c.processWithRetry(t.Context(), testMessage())

	require.NoError(t, err)
	assert.Equal(t, 1, proc.CallCount())
	assertRetryMetrics(t, reg, 0, 0)
}

func TestProcessWithRetry_ContextCanceledOnFirstAttempt(t *testing.T) {
	proc := &mockProcessor{results: []error{context.Canceled}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{MaxRetries: 3})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, proc.CallCount())
	assertRetryMetrics(t, reg, 0, 0)
}

func TestProcessWithRetry_FailThenSucceed(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing, nil}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 3,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.NoError(t, err)
	assert.Equal(t, 2, proc.CallCount())
	assertRetryMetrics(t, reg, 1, 0)
}

func TestProcessWithRetry_FailMultipleThenSucceed(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing, errProcessing, errProcessing, nil}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 5,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.NoError(t, err)
	assert.Equal(t, 4, proc.CallCount())
	assertRetryMetrics(t, reg, 3, 0)
}

func TestProcessWithRetry_AllRetriesExhausted(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 2,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, errProcessing)
	assert.Equal(t, 3, proc.CallCount()) // 1 initial + 2 retries
	assertRetryMetrics(t, reg, 2, 1)
}

func TestProcessWithRetry_NoRetriesConfigured(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{MaxRetries: 0})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, errProcessing)
	assert.Equal(t, 1, proc.CallCount())
	assertRetryMetrics(t, reg, 0, 0)
}

func TestProcessWithRetry_ContextCancelledDuringBackoff(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: InfiniteRetries,
		BaseDelay:  10 * time.Second,
		MaxDelay:   10 * time.Second,
	})

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := c.processWithRetry(ctx, testMessage())

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, proc.CallCount())
	assertRetryMetrics(t, reg, 1, 0)
}

func TestProcessWithRetry_ContextCancelledDuringRetryProcessing(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing, context.Canceled}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 5,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 2, proc.CallCount())
	assertRetryMetrics(t, reg, 1, 0)
}

func TestProcessWithRetry_InfiniteRetries_EventualSuccess(t *testing.T) {
	failures := make([]error, 10)
	for i := range failures {
		failures[i] = errProcessing
	}
	failures = append(failures, nil)

	proc := &mockProcessor{results: failures}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: InfiniteRetries,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.NoError(t, err)
	assert.Equal(t, 11, proc.CallCount()) // 1 initial + 10 retries
	assertRetryMetrics(t, reg, 10, 0)
}

func TestProcessWithRetry_SingleRetrySuccess(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing, nil}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 1,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.NoError(t, err)
	assert.Equal(t, 2, proc.CallCount())
	assertRetryMetrics(t, reg, 1, 0)
}

func TestProcessWithRetry_SingleRetryExhausted(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 1,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, errProcessing)
	assert.Equal(t, 2, proc.CallCount()) // 1 initial + 1 retry
	assertRetryMetrics(t, reg, 1, 1)
}

func TestProcessWithRetry_WrappedContextCanceled(t *testing.T) {
	wrappedErr := fmt.Errorf("wrapped: %w", context.Canceled)
	proc := &mockProcessor{results: []error{wrappedErr}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{MaxRetries: 3})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, proc.CallCount())
	assertRetryMetrics(t, reg, 0, 0)
}

func TestProcessWithRetry_WrappedContextCanceledOnRetry(t *testing.T) {
	wrappedErr := fmt.Errorf("wrapped: %w", context.Canceled)
	proc := &mockProcessor{results: []error{errProcessing, wrappedErr}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 3,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 2, proc.CallCount())
	assertRetryMetrics(t, reg, 1, 0)
}

func TestProcessWithRetry_DeadlineExceededIsRetried(t *testing.T) {
	proc := &mockProcessor{results: []error{context.DeadlineExceeded, nil}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 3,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.NoError(t, err)
	assert.Equal(t, 2, proc.CallCount())
	assertRetryMetrics(t, reg, 1, 0)
}

func TestProcessWithRetry_AlwaysFails_ExactRetryCount(t *testing.T) {
	tests := []struct {
		name       string
		maxRetries int
		wantCalls  int
	}{
		{"0 retries", 0, 1},
		{"1 retry", 1, 2},
		{"3 retries", 3, 4},
		{"5 retries", 5, 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := &mockProcessor{results: []error{errProcessing}}
			c, _ := newTestConsumer(t, proc, RetryPolicy{
				MaxRetries: tt.maxRetries,
				BaseDelay:  1 * time.Millisecond,
				MaxDelay:   5 * time.Millisecond,
			})

			err := c.processWithRetry(t.Context(), testMessage())

			require.ErrorIs(t, err, errProcessing)
			assert.Equal(t, tt.wantCalls, proc.CallCount())
		})
	}
}

// gatherMetricNames is a test helper that collects all registered metric names
// to aid debugging if assertRetryMetrics can't find a metric.
func gatherMetricNames(t *testing.T, reg *prometheus.Registry) []string {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, mf := range families {
		names = append(names, mf.GetName())
	}
	return names
}

func TestProcessWithRetry_MetricsRegistered(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 1,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	_ = c.processWithRetry(t.Context(), testMessage())

	families, err := reg.Gather()
	require.NoError(t, err)

	foundRetries := false
	foundExhausted := false
	for _, mf := range families {
		switch mf.GetName() {
		case "indexer_consumer_message_retries_total":
			foundRetries = true
			require.Len(t, mf.GetMetric(), 1)
			assert.Equal(t, dto.MetricType_COUNTER, mf.GetType())
		case "indexer_consumer_message_retries_exhausted_total":
			foundExhausted = true
			require.Len(t, mf.GetMetric(), 1)
			assert.Equal(t, dto.MetricType_COUNTER, mf.GetType())
		}
	}
	assert.True(t, foundRetries, "expected indexer_consumer_message_retries_total in registry, found: %v", gatherMetricNames(t, reg))
	assert.True(t, foundExhausted, "expected indexer_consumer_message_retries_exhausted_total in registry, found: %v", gatherMetricNames(t, reg))
}

// --- Error classification tests ---

func TestProcessWithRetry_NonRetryableError_BypassesRetries(t *testing.T) {
	nonRetryableErr := processor.NonRetryable(errors.New("bad message"))
	proc := &mockProcessor{results: []error{nonRetryableErr}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 5,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, nonRetryableErr)
	assert.True(t, processor.IsNonRetryable(err))
	assert.Equal(t, 1, proc.CallCount(), "should not retry non-retryable errors")
	assertRetryMetrics(t, reg, 0, 0)
}

func TestProcessWithRetry_FatalError_BypassesRetries(t *testing.T) {
	fatalErr := processor.Fatal(errors.New("auth failure"))
	proc := &mockProcessor{results: []error{fatalErr}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 5,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, fatalErr)
	assert.True(t, processor.IsFatal(err))
	assert.Equal(t, 1, proc.CallCount(), "should not retry fatal errors")
	assertRetryMetrics(t, reg, 0, 0)
}

func TestProcessWithRetry_NonRetryableOnRetry_StopsImmediately(t *testing.T) {
	nonRetryableErr := processor.NonRetryable(errors.New("bad data discovered on retry"))
	proc := &mockProcessor{results: []error{errProcessing, nonRetryableErr}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 5,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, nonRetryableErr)
	assert.True(t, processor.IsNonRetryable(err))
	assert.Equal(t, 2, proc.CallCount(), "should stop on first non-retryable error during retry")
	assertRetryMetrics(t, reg, 1, 0)
}

func TestProcessWithRetry_FatalOnRetry_StopsImmediately(t *testing.T) {
	fatalErr := processor.Fatal(errors.New("schema mismatch discovered on retry"))
	proc := &mockProcessor{results: []error{errProcessing, errProcessing, fatalErr}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 5,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, fatalErr)
	assert.True(t, processor.IsFatal(err))
	assert.Equal(t, 3, proc.CallCount(), "should stop on first fatal error during retry")
	assertRetryMetrics(t, reg, 2, 0)
}

func TestProcessWithRetry_NonRetryableWithInfiniteRetries_StillBypasses(t *testing.T) {
	nonRetryableErr := processor.NonRetryable(errors.New("permanently invalid"))
	proc := &mockProcessor{results: []error{nonRetryableErr}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: InfiniteRetries,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, nonRetryableErr)
	assert.True(t, processor.IsNonRetryable(err))
	assert.Equal(t, 1, proc.CallCount(), "non-retryable should bypass even infinite retries")
	assertRetryMetrics(t, reg, 0, 0)
}

func TestProcessWithRetry_NonRetryablePreservesWrappedError(t *testing.T) {
	sentinel := errors.New("bad json")
	nonRetryableErr := processor.NonRetryable(fmt.Errorf("unmarshal: %w", sentinel))
	proc := &mockProcessor{results: []error{nonRetryableErr}}
	c, _ := newTestConsumer(t, proc, RetryPolicy{MaxRetries: 3})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, nonRetryableErr)
	assert.True(t, processor.IsNonRetryable(err))
	assert.ErrorIs(t, err, sentinel)
}

func TestProcessWithRetry_FatalPreservesWrappedError(t *testing.T) {
	sentinel := errors.New("auth denied")
	fatalErr := processor.Fatal(fmt.Errorf("clickhouse: %w", sentinel))
	proc := &mockProcessor{results: []error{fatalErr}}
	c, _ := newTestConsumer(t, proc, RetryPolicy{MaxRetries: 3})

	err := c.processWithRetry(t.Context(), testMessage())

	require.ErrorIs(t, err, fatalErr)
	assert.True(t, processor.IsFatal(err))
	assert.ErrorIs(t, err, sentinel)
}

func TestProcessWithRetry_RetryableError_StillRetries(t *testing.T) {
	proc := &mockProcessor{results: []error{errProcessing, errProcessing, nil}}
	c, reg := newTestConsumer(t, proc, RetryPolicy{
		MaxRetries: 3,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	})

	err := c.processWithRetry(t.Context(), testMessage())

	require.NoError(t, err)
	assert.Equal(t, 3, proc.CallCount(), "retryable errors should still be retried")
	assertRetryMetrics(t, reg, 2, 0)
}
