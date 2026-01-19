package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/testutils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// ConsumerConfig Tests
// ============================================================================

func TestConsumerConfig_ZeroValues(t *testing.T) {
	cfg := ConsumerConfig{}

	assert.Empty(t, cfg.DLQTopic)
	assert.Empty(t, cfg.Topic)
	assert.Equal(t, int64(0), cfg.MaxConcurrency)
	assert.False(t, cfg.IsDLQConsumer)
	assert.Empty(t, cfg.BootstrapServers)
	assert.Empty(t, cfg.GroupID)
	assert.Empty(t, cfg.AutoOffsetReset)
	assert.False(t, cfg.EnableLogs)
	assert.Equal(t, time.Duration(0), cfg.OffsetManagerCommitInterval)
}

func TestConsumerConfig_AllFields(t *testing.T) {
	cfg := ConsumerConfig{
		DLQTopic:                    "my-dlq",
		Topic:                       "my-topic",
		MaxConcurrency:              25,
		IsDLQConsumer:               true,
		BootstrapServers:            "broker1:9092,broker2:9092",
		GroupID:                     "my-group",
		AutoOffsetReset:             "latest",
		EnableLogs:                  true,
		OffsetManagerCommitInterval: 10 * time.Second,
	}

	assert.Equal(t, "my-dlq", cfg.DLQTopic)
	assert.Equal(t, "my-topic", cfg.Topic)
	assert.Equal(t, int64(25), cfg.MaxConcurrency)
	assert.True(t, cfg.IsDLQConsumer)
	assert.Equal(t, "broker1:9092,broker2:9092", cfg.BootstrapServers)
	assert.Equal(t, "my-group", cfg.GroupID)
	assert.Equal(t, "latest", cfg.AutoOffsetReset)
	assert.True(t, cfg.EnableLogs)
	assert.Equal(t, 10*time.Second, cfg.OffsetManagerCommitInterval)
}

func TestConsumerConfig_Structure(t *testing.T) {
	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "test-group",
		Topic:                       "test-topic",
		DLQTopic:                    "test-dlq",
		MaxConcurrency:              10,
		IsDLQConsumer:               false,
		AutoOffsetReset:             "earliest",
		EnableLogs:                  true,
		OffsetManagerCommitInterval: 5 * time.Second,
	}

	require.NotEmpty(t, cfg.BootstrapServers)
	require.NotEmpty(t, cfg.GroupID)
	require.NotEmpty(t, cfg.Topic)
	require.Greater(t, cfg.MaxConcurrency, int64(0))
}

// ============================================================================
// NewConsumer Tests
// ============================================================================

func TestNewConsumer_MinimalConfig(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProc := &testutils.MockProcessor{}

	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "group",
		Topic:                       "topic",
		MaxConcurrency:              1,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 1 * time.Second,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	assert.Equal(t, "topic", consumer.cfg.Topic)
	assert.Equal(t, int64(1), consumer.cfg.MaxConcurrency)
	assert.Equal(t, "", consumer.cfg.DLQTopic)
	assert.False(t, consumer.cfg.IsDLQConsumer)
	assert.False(t, consumer.cfg.EnableLogs)
}

func TestNewConsumer_MaximalConfig(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		time.Sleep(200 * time.Millisecond)
	})
	mockProc := &testutils.MockProcessor{}

	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "production-consumer-group",
		Topic:                       "production-topic",
		DLQTopic:                    "production-dlq",
		MaxConcurrency:              100,
		IsDLQConsumer:               false,
		AutoOffsetReset:             "latest",
		EnableLogs:                  false, // Disabled to avoid race in tests
		OffsetManagerCommitInterval: 30 * time.Second,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	assert.Equal(t, "production-topic", consumer.cfg.Topic)
	assert.Equal(t, "production-dlq", consumer.cfg.DLQTopic)
	assert.Equal(t, int64(100), consumer.cfg.MaxConcurrency)
	assert.False(t, consumer.cfg.IsDLQConsumer)
	assert.Equal(t, "latest", consumer.cfg.AutoOffsetReset)
	assert.False(t, consumer.cfg.EnableLogs)
	assert.Equal(t, 30*time.Second, consumer.cfg.OffsetManagerCommitInterval)
}

func TestNewConsumer_VariousConfigurations(t *testing.T) {
	log := testutils.NewTestLogger(t)
	mockProc := &testutils.MockProcessor{}

	testCases := []struct {
		name string
		cfg  ConsumerConfig
	}{
		{
			name: "minimum_config",
			cfg: ConsumerConfig{
				BootstrapServers:            "localhost:9092",
				GroupID:                     "group",
				Topic:                       "topic",
				MaxConcurrency:              1,
				AutoOffsetReset:             "earliest",
				OffsetManagerCommitInterval: 1 * time.Second,
			},
		},
		{
			name: "with_DLQ",
			cfg: ConsumerConfig{
				BootstrapServers:            "localhost:9092",
				GroupID:                     "group",
				Topic:                       "topic",
				DLQTopic:                    "dlq",
				MaxConcurrency:              10,
				AutoOffsetReset:             "earliest",
				OffsetManagerCommitInterval: 5 * time.Second,
			},
		},
		{
			name: "DLQ_consumer",
			cfg: ConsumerConfig{
				BootstrapServers:            "localhost:9092",
				GroupID:                     "group",
				Topic:                       "dlq",
				IsDLQConsumer:               true,
				MaxConcurrency:              5,
				AutoOffsetReset:             "earliest",
				OffsetManagerCommitInterval: 5 * time.Second,
			},
		},
		{
			name: "high_concurrency",
			cfg: ConsumerConfig{
				BootstrapServers:            "localhost:9092",
				GroupID:                     "group",
				Topic:                       "topic",
				MaxConcurrency:              100,
				AutoOffsetReset:             "earliest",
				OffsetManagerCommitInterval: 10 * time.Second,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				time.Sleep(150 * time.Millisecond) // Wait for background goroutines
			})

			consumer, err := NewConsumer(ctx, log, tc.cfg, mockProc)
			require.NoError(t, err)
			require.NotNil(t, consumer)
			assert.Equal(t, tc.cfg.Topic, consumer.cfg.Topic)
			assert.Equal(t, tc.cfg.MaxConcurrency, consumer.cfg.MaxConcurrency)
		})
	}
}

func TestNewConsumer_StructFields(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		time.Sleep(150 * time.Millisecond)
	})
	mockProc := &testutils.MockProcessor{}

	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "test-group",
		Topic:                       "test-topic",
		MaxConcurrency:              10,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 5 * time.Second,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	// Verify internal fields
	assert.NotNil(t, consumer.consumer)
	assert.NotNil(t, consumer.offsetManager)
	assert.NotNil(t, consumer.processor)
	assert.NotNil(t, consumer.sem)
	assert.NotNil(t, consumer.rebalanceContexts)
	assert.NotNil(t, consumer.errCh)
	assert.NotNil(t, consumer.doneCh)
	assert.NotNil(t, consumer.logsDone)
}

// ============================================================================
// Consumer Internal State Tests
// ============================================================================

func TestConsumer_ChannelsInitialization(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProc := &testutils.MockProcessor{}

	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "test-group",
		Topic:                       "test-topic",
		MaxConcurrency:              10,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 5 * time.Second,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	assert.NotNil(t, consumer.logsDone)
	assert.NotNil(t, consumer.doneCh)
	assert.NotNil(t, consumer.errCh)

	// Verify channels are not closed initially
	select {
	case <-consumer.logsDone:
		// logsDone might be closed if logs are disabled
	case <-consumer.doneCh:
		t.Fatal("doneCh should not be closed initially")
	case <-consumer.errCh:
		t.Fatal("errCh should not be closed initially")
	default:
		// Success - channels are open
	}
}

func TestConsumer_ErrorChannelCapacity(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProc := &testutils.MockProcessor{}

	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "test-group",
		Topic:                       "test-topic",
		MaxConcurrency:              2,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 5 * time.Second,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	// Error channel capacity should match MaxConcurrency
	assert.Equal(t, cap(consumer.errCh), int(cfg.MaxConcurrency))

	// Fill the error channel
	consumer.errCh <- errors.New("error 1")
	consumer.errCh <- errors.New("error 2")
	assert.Equal(t, 2, len(consumer.errCh))

	// Drain
	<-consumer.errCh
	<-consumer.errCh
	assert.Equal(t, 0, len(consumer.errCh))
}

func TestConsumer_RebalanceContextsInitialization(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProc := &testutils.MockProcessor{}

	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "test-group",
		Topic:                       "test-topic",
		MaxConcurrency:              10,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 5 * time.Second,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	assert.NotNil(t, consumer.rebalanceContexts)
	assert.Equal(t, 0, len(consumer.rebalanceContexts))
}

func TestConsumer_WaitGroupTracking(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProc := &testutils.MockProcessor{}

	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "test-group",
		Topic:                       "test-topic",
		MaxConcurrency:              10,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 5 * time.Second,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	// Simulate balanced Add/Done
	count := 5
	for i := 0; i < count; i++ {
		consumer.wg.Add(1)
	}

	done := make(chan struct{})
	go func() {
		consumer.wg.Wait()
		close(done)
	}()

	// Should not complete yet
	select {
	case <-done:
		t.Fatal("WaitGroup should not complete before Done() calls")
	case <-time.After(100 * time.Millisecond):
		// Success
	}

	// Call Done() for all
	for i := 0; i < count; i++ {
		consumer.wg.Done()
	}

	// Should complete now
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("WaitGroup should complete after all Done() calls")
	}
}

func TestConsumer_SemaphoreInitialization(t *testing.T) {
	log := testutils.NewTestLogger(t)

	testCases := []struct {
		name           string
		maxConcurrency int64
	}{
		{"concurrency-1", 1},
		{"concurrency-5", 5},
		{"concurrency-10", 10},
		{"concurrency-50", 50},
		{"concurrency-100", 100},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				time.Sleep(150 * time.Millisecond)
			})
			mockProc := &testutils.MockProcessor{}

			cfg := ConsumerConfig{
				BootstrapServers:            "localhost:9092",
				GroupID:                     "test-group",
				Topic:                       "test-topic",
				MaxConcurrency:              tc.maxConcurrency,
				AutoOffsetReset:             "earliest",
				OffsetManagerCommitInterval: 5 * time.Second,
			}

			consumer, err := NewConsumer(ctx, log, cfg, mockProc)
			require.NoError(t, err)
			require.NotNil(t, consumer.sem)

			// Verify semaphore capacity by acquiring all permits
			for i := int64(0); i < tc.maxConcurrency; i++ {
				acquired := consumer.sem.TryAcquire(1)
				assert.True(t, acquired, "Should acquire permit %d/%d", i+1, tc.maxConcurrency)
			}

			// Next acquire should fail
			acquired := consumer.sem.TryAcquire(1)
			assert.False(t, acquired, "Should not acquire beyond capacity")

			// Release all
			consumer.sem.Release(tc.maxConcurrency)
		})
	}
}

// ============================================================================
// Consumer Configuration Tests
// ============================================================================

func TestConsumer_AutoOffsetResetStrategies(t *testing.T) {
	log := testutils.NewTestLogger(t)

	strategies := []string{"earliest", "latest"}

	for _, strategy := range strategies {
		t.Run(strategy, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				time.Sleep(150 * time.Millisecond)
			})
			mockProc := &testutils.MockProcessor{}

			cfg := ConsumerConfig{
				BootstrapServers:            "localhost:9092",
				GroupID:                     "test-group",
				Topic:                       "test-topic",
				MaxConcurrency:              10,
				AutoOffsetReset:             strategy,
				OffsetManagerCommitInterval: 5 * time.Second,
			}

			consumer, err := NewConsumer(ctx, log, cfg, mockProc)
			require.NoError(t, err)
			assert.Equal(t, strategy, consumer.cfg.AutoOffsetReset)
		})
	}
}

func TestConsumer_CommitIntervals(t *testing.T) {
	log := testutils.NewTestLogger(t)

	intervals := []time.Duration{
		1 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		1 * time.Minute,
	}

	for _, interval := range intervals {
		t.Run(interval.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				time.Sleep(150 * time.Millisecond)
			})
			mockProc := &testutils.MockProcessor{}

			cfg := ConsumerConfig{
				BootstrapServers:            "localhost:9092",
				GroupID:                     "test-group",
				Topic:                       "test-topic",
				MaxConcurrency:              10,
				AutoOffsetReset:             "earliest",
				OffsetManagerCommitInterval: interval,
			}

			consumer, err := NewConsumer(ctx, log, cfg, mockProc)
			require.NoError(t, err)
			assert.Equal(t, interval, consumer.cfg.OffsetManagerCommitInterval)
		})
	}
}

func TestConsumer_DifferentGroupIDs(t *testing.T) {
	log := testutils.NewTestLogger(t)

	groupIDs := []string{
		"simple-group",
		"group-with-dashes",
		"group_with_underscores",
		"group.with.dots",
		"GroupWithCaps",
		"group123",
	}

	for _, groupID := range groupIDs {
		t.Run(groupID, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				time.Sleep(150 * time.Millisecond)
			})
			mockProc := &testutils.MockProcessor{}

			cfg := ConsumerConfig{
				BootstrapServers:            "localhost:9092",
				GroupID:                     groupID,
				Topic:                       "test-topic",
				MaxConcurrency:              10,
				AutoOffsetReset:             "earliest",
				OffsetManagerCommitInterval: 5 * time.Second,
			}

			consumer, err := NewConsumer(ctx, log, cfg, mockProc)
			require.NoError(t, err)
			assert.Equal(t, groupID, consumer.cfg.GroupID)
		})
	}
}

func TestConsumer_EnableLogsConfiguration(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProc := &testutils.MockProcessor{}

	// Test logs disabled (logs enabled tested in integration tests due to race conditions)
	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "test-group",
		Topic:                       "test-topic",
		MaxConcurrency:              10,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 5 * time.Second,
		EnableLogs:                  false,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)
	assert.False(t, consumer.cfg.EnableLogs)
}

// ============================================================================
// publishToDLQ Tests
// ============================================================================

func TestPublishToDLQ_DLQTopicNotConfigured(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProc := &testutils.MockProcessor{}

	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "test-group",
		Topic:                       "test-topic",
		DLQTopic:                    "", // No DLQ configured
		MaxConcurrency:              10,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 5 * time.Second,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	msg := testutils.NewTestMessage("test-topic", 0, 100, []byte("key"), []byte("value"))

	err = consumer.publishToDLQ(ctx, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "DLQ topic not configured")
}

func TestPublishToDLQ_ContextCanceled_Unit(t *testing.T) {
	log := testutils.NewTestLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProc := &testutils.MockProcessor{}

	cfg := ConsumerConfig{
		BootstrapServers:            "localhost:9092",
		GroupID:                     "test-group",
		Topic:                       "test-topic",
		DLQTopic:                    "test-dlq",
		MaxConcurrency:              10,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 5 * time.Second,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	// Cancel context before calling publishToDLQ
	cancel()

	msg := testutils.NewTestMessage("test-topic", 0, 100, []byte("key"), []byte("value"))
	err = consumer.publishToDLQ(ctx, msg)

	// Should return context.Canceled error
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
}

// ============================================================================
// Context and Rebalance Tests
// ============================================================================

func TestRebalanceCtx_Structure(t *testing.T) {
	ctx := context.Background()
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rCtx := rebalanceCtx{
		ctx:    childCtx,
		cancel: cancel,
	}

	assert.NotNil(t, rCtx.ctx)
	assert.NotNil(t, rCtx.cancel)

	// Verify context is not cancelled initially
	select {
	case <-rCtx.ctx.Done():
		t.Fatal("context should not be cancelled")
	default:
		// Success
	}

	// Cancel and verify
	rCtx.cancel()
	select {
	case <-rCtx.ctx.Done():
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("context should be cancelled")
	}
}

func TestRebalanceCtx_CancellationPropagation(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	ctx, cancel := context.WithCancel(parentCtx)
	rCtx := rebalanceCtx{
		ctx:    ctx,
		cancel: cancel,
	}

	// Cancel parent context
	parentCancel()

	// Child context should be cancelled too
	select {
	case <-rCtx.ctx.Done():
		assert.Error(t, rCtx.ctx.Err())
	case <-time.After(1 * time.Second):
		t.Fatal("context should be cancelled via parent")
	}
}

func TestRebalanceCtx_IndependentCancellation(t *testing.T) {
	parentCtx := context.Background()

	ctx1, cancel1 := context.WithCancel(parentCtx)
	ctx2, cancel2 := context.WithCancel(parentCtx)
	defer cancel2()

	rCtx1 := rebalanceCtx{ctx: ctx1, cancel: cancel1}
	rCtx2 := rebalanceCtx{ctx: ctx2, cancel: cancel2}

	// Cancel first context
	rCtx1.cancel()

	// First should be cancelled
	select {
	case <-rCtx1.ctx.Done():
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("rCtx1 should be cancelled")
	}

	// Second should still be active
	select {
	case <-rCtx2.ctx.Done():
		t.Fatal("rCtx2 should not be cancelled")
	default:
		// Success
	}
}

// ============================================================================
// Constants and Defaults Tests
// ============================================================================

func TestConsumer_Constants(t *testing.T) {
	// Session timeout should be reasonable (4 minutes)
	assert.GreaterOrEqual(t, defaultSessionTimeout, 60_000) // At least 1 minute
	assert.LessOrEqual(t, defaultSessionTimeout, 600_000)   // At most 10 minutes

	// Max poll interval should be longer than session timeout
	assert.Greater(t, defaultMaxPollInterval, defaultSessionTimeout)

	// Flush timeout should be reasonable
	assert.GreaterOrEqual(t, defaultFlushTimeout, 5*time.Second)
	assert.LessOrEqual(t, defaultFlushTimeout, 60*time.Second)

	// Goroutine wait timeout should be reasonable
	assert.GreaterOrEqual(t, defaultGoroutineWaitTimeout, 10*time.Second)
	assert.LessOrEqual(t, defaultGoroutineWaitTimeout, 2*time.Minute)
}

// ============================================================================
// Mock Processor Tests
// ============================================================================

func TestMockProcessor_Behavior(t *testing.T) {
	mockProc := &testutils.MockProcessor{}
	ctx := context.Background()
	msg := testutils.NewTestMessage("topic", 0, 1, []byte("key"), []byte("value"))

	// Mock successful processing
	mockProc.On("Process", mock.Anything, mock.Anything).Return(nil).Once()

	err := mockProc.Process(ctx, msg)
	assert.NoError(t, err)

	// Mock error
	mockProc.On("Process", mock.Anything, mock.Anything).Return(errors.New("processing failed")).Once()

	err = mockProc.Process(ctx, msg)
	assert.Error(t, err)
	assert.Equal(t, "processing failed", err.Error())

	mockProc.AssertExpectations(t)
}

func TestMockProcessor_Concurrent(t *testing.T) {
	mockProc := &testutils.MockProcessor{}
	ctx := context.Background()

	// Mock with run function to track concurrent calls
	callCount := atomic.Int32{}
	mockProc.On("Process", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			callCount.Add(1)
			time.Sleep(10 * time.Millisecond) // Simulate work
		})

	// Call from multiple goroutines
	var wg sync.WaitGroup
	concurrency := 10
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			msg := testutils.NewTestMessage("topic", 0, int64(idx), []byte("key"), []byte("value"))
			err := mockProc.Process(ctx, msg)
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int32(concurrency), callCount.Load())
}

func TestMockProcessor_VariousErrors(t *testing.T) {
	mockProc := &testutils.MockProcessor{}
	ctx := context.Background()
	msg := testutils.NewTestMessage("topic", 0, 1, []byte("key"), []byte("value"))

	errorTypes := []error{
		errors.New("generic error"),
		context.Canceled,
		context.DeadlineExceeded,
		errors.New("database connection failed"),
		errors.New("timeout"),
	}

	for i, expectedErr := range errorTypes {
		mockProc.On("Process", mock.Anything, mock.Anything).Return(expectedErr).Once()

		err := mockProc.Process(ctx, msg)
		assert.Equal(t, expectedErr, err, "iteration %d", i)
	}

	mockProc.AssertExpectations(t)
}

func TestMockProcessor_CallCounting(t *testing.T) {
	mockProc := &testutils.MockProcessor{}
	ctx := context.Background()

	callCount := atomic.Int32{}
	mockProc.On("Process", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			callCount.Add(1)
		})

	expectedCalls := 10
	for i := 0; i < expectedCalls; i++ {
		msg := testutils.NewTestMessage("topic", 0, int64(i), []byte("key"), []byte("value"))
		err := mockProc.Process(ctx, msg)
		require.NoError(t, err)
	}

	assert.Equal(t, int32(expectedCalls), callCount.Load())
}

func TestMockProcessor_DifferentMessages(t *testing.T) {
	mockProc := &testutils.MockProcessor{}
	ctx := context.Background()

	processedValues := []string{}
	var mu sync.Mutex

	mockProc.On("Process", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			msg := args.Get(1).(*kafka.Message)
			mu.Lock()
			processedValues = append(processedValues, string(msg.Value))
			mu.Unlock()
		})

	values := []string{"message1", "message2", "message3"}
	for i, value := range values {
		msg := testutils.NewTestMessage("topic", 0, int64(i), []byte("key"), []byte(value))
		err := mockProc.Process(ctx, msg)
		require.NoError(t, err)
	}

	assert.ElementsMatch(t, values, processedValues)
}

// ============================================================================
// Msg Struct Tests
// ============================================================================

func TestMsg_Structure(t *testing.T) {
	msg := Msg{
		Topic: "test-topic",
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
		Headers: map[string]string{
			"header1": "value1",
			"header2": "value2",
		},
	}

	assert.Equal(t, "test-topic", msg.Topic)
	assert.Equal(t, []byte("test-key"), msg.Key)
	assert.Equal(t, []byte("test-value"), msg.Value)
	assert.Len(t, msg.Headers, 2)
	assert.Equal(t, "value1", msg.Headers["header1"])
	assert.Equal(t, "value2", msg.Headers["header2"])
}

func TestMsg_EmptyValues(t *testing.T) {
	msg := Msg{
		Topic:   "",
		Key:     nil,
		Value:   nil,
		Headers: nil,
	}

	assert.Empty(t, msg.Topic)
	assert.Nil(t, msg.Key)
	assert.Nil(t, msg.Value)
	assert.Nil(t, msg.Headers)
}

func TestMsg_LargeHeaders(t *testing.T) {
	headers := make(map[string]string)
	for i := 0; i < 100; i++ {
		headers[fmt.Sprintf("header-%d", i)] = fmt.Sprintf("value-%d", i)
	}

	msg := Msg{
		Topic:   "test-topic",
		Key:     []byte("key"),
		Value:   []byte("value"),
		Headers: headers,
	}

	assert.Equal(t, 100, len(msg.Headers))
	assert.Equal(t, "value-42", msg.Headers["header-42"])
}

// ============================================================================
// Test Helper Tests
// ============================================================================

func TestNewTestMessage(t *testing.T) {
	topic := "test-topic"
	partition := int32(2)
	offset := int64(100)
	key := []byte("test-key")
	value := []byte("test-value")

	msg := testutils.NewTestMessage(topic, partition, offset, key, value)

	require.NotNil(t, msg)
	assert.Equal(t, topic, *msg.TopicPartition.Topic)
	assert.Equal(t, partition, msg.TopicPartition.Partition)
	assert.Equal(t, kafka.Offset(offset), msg.TopicPartition.Offset)
	assert.Equal(t, key, msg.Key)
	assert.Equal(t, value, msg.Value)
}
