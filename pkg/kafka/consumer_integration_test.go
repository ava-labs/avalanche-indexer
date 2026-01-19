//go:build integration
// +build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/testutils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// setupKafka starts a Kafka container and returns the bootstrap servers
func setupKafka(t *testing.T, ctx context.Context) (string, func()) {
	kafkaContainer, err := testKafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		testKafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err)

	bootstrapServers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	cleanup := func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Logf("failed to terminate kafka container: %s", err)
		}
	}

	return bootstrapServers[0], cleanup
}

// createTopic creates a Kafka topic with the specified number of partitions
func createTopic(t *testing.T, bootstrapServers, topic string, partitions int) {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	require.NoError(t, err)
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Error.Code().String() != "NO_ERROR" && results[0].Error.Code() != kafka.ErrNoError, results[0].Error)
}

// produceTestMessages produces test messages to a topic
func produceTestMessages(t *testing.T, bootstrapServers, topic string, count int) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",
	}

	ctx := context.Background()
	log := testutils.NewTestLogger(t)
	producer, err := NewProducer(ctx, config, log)
	require.NoError(t, err)
	defer producer.Close(5 * time.Second)

	for i := 0; i < count; i++ {
		msg := Msg{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		err := producer.Produce(ctx, msg)
		require.NoError(t, err)
	}
}

func TestConsumerIntegration_E2E_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-e2e-topic"
	dlqTopic := "test-e2e-dlq"
	createTopic(t, bootstrapServers, topic, 1)
	createTopic(t, bootstrapServers, dlqTopic, 1)

	// Produce test messages
	messageCount := 10
	produceTestMessages(t, bootstrapServers, topic, messageCount)

	// Track processed messages
	var processedCount atomic.Int32
	mockProc := &testutils.MockProcessor{}
	mockProc.On("Process", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		processedCount.Add(1)
	})

	// Create and start consumer
	log := testutils.NewTestLogger(t)
	cfg := ConsumerConfig{
		BootstrapServers:            bootstrapServers,
		GroupID:                     "test-e2e-group",
		Topic:                       topic,
		DLQTopic:                    dlqTopic,
		MaxConcurrency:              5,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 1 * time.Second,
		EnableLogs:                  false,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	// Start consumer in background
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(consumerCtx)
	}()

	// Wait for all messages to be processed
	require.Eventually(t, func() bool {
		return processedCount.Load() == int32(messageCount)
	}, 30*time.Second, 100*time.Millisecond, "expected %d messages to be processed", messageCount)

	// Cancel consumer and wait for shutdown
	consumerCancel()
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not shut down within timeout")
	}

	// Verify mock expectations
	mockProc.AssertExpectations(t)
}

func TestConsumerIntegration_DLQ_ProcessingFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-dlq-topic"
	dlqTopic := "test-dlq-dlq"
	createTopic(t, bootstrapServers, topic, 1)
	createTopic(t, bootstrapServers, dlqTopic, 1)

	// Produce test messages
	messageCount := 5
	produceTestMessages(t, bootstrapServers, topic, messageCount)

	// Mock processor that always fails
	mockProc := &testutils.MockProcessor{}
	processingError := errors.New("simulated processing failure")
	mockProc.On("Process", mock.Anything, mock.Anything).Return(processingError)

	// Create and start consumer
	log := testutils.NewTestLogger(t)
	cfg := ConsumerConfig{
		BootstrapServers:            bootstrapServers,
		GroupID:                     "test-dlq-group",
		Topic:                       topic,
		DLQTopic:                    dlqTopic,
		MaxConcurrency:              5,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 1 * time.Second,
		EnableLogs:                  false,
		IsDLQConsumer:               false,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	// Start consumer in background
	consumerCtx, consumerCancel := context.WithTimeout(ctx, 10*time.Second)
	defer consumerCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(consumerCtx)
	}()

	// Wait for messages to be processed (and fail)
	time.Sleep(5 * time.Second)
	consumerCancel()
	<-errCh

	// Verify DLQ contains failed messages
	// Create a DLQ consumer to verify
	var dlqCount atomic.Int32
	dlqProc := &testutils.MockProcessor{}
	dlqProc.On("Process", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		dlqCount.Add(1)
	})

	dlqCfg := ConsumerConfig{
		BootstrapServers:            bootstrapServers,
		GroupID:                     "test-dlq-verify-group",
		Topic:                       dlqTopic,
		DLQTopic:                    "",
		MaxConcurrency:              5,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 1 * time.Second,
		EnableLogs:                  false,
		IsDLQConsumer:               true,
	}

	dlqConsumer, err := NewConsumer(ctx, log, dlqCfg, dlqProc)
	require.NoError(t, err)

	dlqCtx, dlqCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dlqCancel()

	dlqErrCh := make(chan error, 1)
	go func() {
		dlqErrCh <- dlqConsumer.Start(dlqCtx)
	}()

	// Wait for DLQ messages to be processed
	require.Eventually(t, func() bool {
		return dlqCount.Load() == int32(messageCount)
	}, 15*time.Second, 100*time.Millisecond, "expected %d messages in DLQ", messageCount)

	dlqCancel()
	<-dlqErrCh

	mockProc.AssertExpectations(t)
	dlqProc.AssertExpectations(t)
}

func TestConsumerIntegration_IsDLQConsumer_PreventsDLQPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	dlqTopic := "test-dlq-infinite-loop"
	createTopic(t, bootstrapServers, dlqTopic, 1)

	// Produce test message
	produceTestMessages(t, bootstrapServers, dlqTopic, 1)

	// Mock processor that always fails
	mockProc := &testutils.MockProcessor{}
	processingError := errors.New("simulated DLQ processing failure")
	mockProc.On("Process", mock.Anything, mock.Anything).Return(processingError)

	// Create DLQ consumer with IsDLQConsumer=true
	log := testutils.NewTestLogger(t)
	cfg := ConsumerConfig{
		BootstrapServers:            bootstrapServers,
		GroupID:                     "test-dlq-infinite-group",
		Topic:                       dlqTopic,
		DLQTopic:                    dlqTopic, // Same topic to detect infinite loop
		MaxConcurrency:              1,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 1 * time.Second,
		EnableLogs:                  false,
		IsDLQConsumer:               true, // Prevent re-sending to DLQ
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	// Start consumer in background
	consumerCtx, consumerCancel := context.WithTimeout(ctx, 5*time.Second)
	defer consumerCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(consumerCtx)
	}()

	// Consumer should shut down due to error (not infinite loop)
	select {
	case err := <-errCh:
		// Should get an error from the error channel, not hang
		assert.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not shut down within timeout - possible infinite loop")
	}

	mockProc.AssertExpectations(t)
}

func TestConsumerIntegration_GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-shutdown-topic"
	createTopic(t, bootstrapServers, topic, 1)

	// Produce messages
	messageCount := 20
	produceTestMessages(t, bootstrapServers, topic, messageCount)

	// Mock processor with slow processing
	var processedCount atomic.Int32
	mockProc := &testutils.MockProcessor{}
	mockProc.On("Process", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		time.Sleep(100 * time.Millisecond) // Slow processing
		processedCount.Add(1)
	})

	// Create and start consumer
	log := testutils.NewTestLogger(t)
	cfg := ConsumerConfig{
		BootstrapServers:            bootstrapServers,
		GroupID:                     "test-shutdown-group",
		Topic:                       topic,
		DLQTopic:                    "test-shutdown-dlq",
		MaxConcurrency:              5,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 1 * time.Second,
		EnableLogs:                  false,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	// Start consumer
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(consumerCtx)
	}()

	// Wait for some processing to start
	time.Sleep(1 * time.Second)

	// Cancel context to trigger shutdown
	shutdownStart := time.Now()
	consumerCancel()

	// Wait for consumer to shut down
	select {
	case err := <-errCh:
		shutdownDuration := time.Since(shutdownStart)
		assert.NoError(t, err)

		// Shutdown should complete within reasonable time (< 30s timeout + buffer)
		assert.Less(t, shutdownDuration, 35*time.Second, "shutdown took too long")

		t.Logf("Processed %d/%d messages before shutdown in %v",
			processedCount.Load(), messageCount, shutdownDuration)
	case <-time.After(45 * time.Second):
		t.Fatal("consumer did not shut down within expected timeout")
	}

	mockProc.AssertExpectations(t)
}

func TestConsumerIntegration_MultiplePartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-multipart-topic"
	createTopic(t, bootstrapServers, topic, 4) // 4 partitions

	// Produce messages
	messageCount := 40
	produceTestMessages(t, bootstrapServers, topic, messageCount)

	// Track processed messages per partition
	partitionCounts := make(map[int32]*atomic.Int32)
	for i := 0; i < 4; i++ {
		partitionCounts[int32(i)] = &atomic.Int32{}
	}

	mockProc := &testutils.MockProcessor{}
	mockProc.On("Process", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		msg := args.Get(1).(*kafka.Message)
		if counter, ok := partitionCounts[msg.TopicPartition.Partition]; ok {
			counter.Add(1)
		}
	})

	// Create and start consumer
	log := testutils.NewTestLogger(t)
	cfg := ConsumerConfig{
		BootstrapServers:            bootstrapServers,
		GroupID:                     "test-multipart-group",
		Topic:                       topic,
		DLQTopic:                    "test-multipart-dlq",
		MaxConcurrency:              10,
		AutoOffsetReset:             "earliest",
		OffsetManagerCommitInterval: 1 * time.Second,
		EnableLogs:                  false,
	}

	consumer, err := NewConsumer(ctx, log, cfg, mockProc)
	require.NoError(t, err)

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(consumerCtx)
	}()

	// Wait for all messages to be processed
	require.Eventually(t, func() bool {
		total := int32(0)
		for _, counter := range partitionCounts {
			total += counter.Load()
		}
		return total == int32(messageCount)
	}, 30*time.Second, 100*time.Millisecond, "expected all messages to be processed")

	consumerCancel()
	<-errCh

	// Verify all partitions were processed
	for partition, counter := range partitionCounts {
		count := counter.Load()
		t.Logf("Partition %d processed %d messages", partition, count)
		assert.Greater(t, count, int32(0), "partition %d should have processed messages", partition)
	}

	mockProc.AssertExpectations(t)
}
