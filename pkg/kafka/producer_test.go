package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/testutils"
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// NewProducer Tests
// ============================================================================

func TestNewProducer_ValidConfig(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &cKafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := NewProducer(ctx, cfg, log)
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Clean up
	producer.Close(5 * time.Second)
}

// ============================================================================
// Producer Close Tests
// ============================================================================

func TestProducer_Close_Idempotent(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &cKafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := NewProducer(ctx, cfg, log)
	require.NoError(t, err)

	// First close
	producer.Close(5 * time.Second)

	// Second close should not panic or cause issues
	producer.Close(5 * time.Second)
}

func TestProducer_Close_WaitsForGoroutines(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &cKafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := NewProducer(ctx, cfg, log)
	require.NoError(t, err)

	// Give goroutines time to start
	time.Sleep(100 * time.Millisecond)

	// Close should wait for goroutines
	start := time.Now()
	producer.Close(10 * time.Second)
	elapsed := time.Since(start)

	// Should complete reasonably quickly (not timeout)
	assert.Less(t, elapsed, 10*time.Second)
}

// ============================================================================
// Producer Errors Channel Tests
// ============================================================================

func TestProducer_Errors_ChannelClosed(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &cKafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := NewProducer(ctx, cfg, log)
	require.NoError(t, err)

	// Get error channel
	errCh := producer.Errors()
	require.NotNil(t, errCh)

	// Close producer
	producer.Close(5 * time.Second)

	// Error channel should be closed
	_, ok := <-errCh
	assert.False(t, ok, "error channel should be closed after Close()")
}

func TestProducer_ErrorChannelBufferSize(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &cKafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := NewProducer(ctx, cfg, log)
	require.NoError(t, err)
	defer producer.Close(5 * time.Second)

	errCh := producer.Errors()

	// Channel should have some capacity
	assert.Greater(t, cap(errCh), 0)
}

// ============================================================================
// Producer Context Cancellation Tests
// ============================================================================

func TestProducer_ContextCancellation_StopsGoroutines(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())

	cfg := &cKafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := NewProducer(ctx, cfg, log)
	require.NoError(t, err)

	// Cancel context
	cancel()

	// Give goroutines time to stop
	time.Sleep(200 * time.Millisecond)

	// Clean up
	producer.Close(5 * time.Second)
}

// ============================================================================
// Producer Struct Tests
// ============================================================================

func TestProducer_StructFields(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &cKafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := NewProducer(ctx, cfg, log)
	require.NoError(t, err)
	defer producer.Close(5 * time.Second)

	// Verify internal fields are initialized
	assert.NotNil(t, producer.producer)
	assert.NotNil(t, producer.log)
	assert.NotNil(t, producer.errCh)
	assert.NotNil(t, producer.eventsDone)
	assert.NotNil(t, producer.logsDone)
}

// ============================================================================
// Constants Tests
// ============================================================================

func TestQueueFullErrorRetryDelay(t *testing.T) {
	// Verify retry delay is reasonable (should be positive and <= 1 second)
	assert.Greater(t, queueFullErrorRetryDelay, time.Duration(0))
	assert.LessOrEqual(t, queueFullErrorRetryDelay, 1*time.Second)
}
