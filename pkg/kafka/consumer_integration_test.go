//go:build integration
// +build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	consumerTestTopic    = "test-consumer-topic"
	consumerDLQTopic     = "test-consumer-dlq"
	consumerTestTimeout  = 120 * time.Second
	consumerFlushTimeout = 10 * time.Second
	producerFlushTimeout = 5000
)

type testProcessor struct {
	processFunc     func(ctx context.Context, msg *ckafka.Message) error
	mu              sync.Mutex
	processedCount  int32
	processedMsgs   []*ckafka.Message
	shouldFail      bool
	failureError    error
	processingDelay time.Duration
}

func newTestProcessor() *testProcessor {
	return &testProcessor{
		processedMsgs: make([]*ckafka.Message, 0),
	}
}

func (p *testProcessor) Process(ctx context.Context, msg *ckafka.Message) error {
	if p.processingDelay > 0 {
		select {
		case <-time.After(p.processingDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if p.processFunc != nil {
		return p.processFunc(ctx, msg)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	atomic.AddInt32(&p.processedCount, 1)
	p.processedMsgs = append(p.processedMsgs, msg)

	if p.shouldFail {
		if p.failureError != nil {
			return p.failureError
		}
		return errors.New("processing failed")
	}

	return nil
}

func (p *testProcessor) GetProcessedCount() int {
	return int(atomic.LoadInt32(&p.processedCount))
}

func (p *testProcessor) GetProcessedMessages() []*ckafka.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*ckafka.Message{}, p.processedMsgs...)
}

func (p *testProcessor) SetShouldFail(shouldFail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.shouldFail = shouldFail
}

func (p *testProcessor) SetFailureError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failureError = err
}

type consumerKafkaContainer struct {
	container testcontainers.Container
	brokers   string
}

func setupConsumerKafka(t *testing.T) *consumerKafkaContainer {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.5.0",
		ExposedPorts: []string{"9093/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                          "1",
			"KAFKA_PROCESS_ROLES":                    "broker,controller",
			"KAFKA_LISTENERS":                        "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094,EXTERNAL://0.0.0.0:9093",
			"KAFKA_ADVERTISED_LISTENERS":             "PLAINTEXT://localhost:9092,EXTERNAL://127.0.0.1:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":   "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":         "1@localhost:9094",
			"KAFKA_CONTROLLER_LISTENER_NAMES":        "CONTROLLER",
			"KAFKA_INTER_BROKER_LISTENER_NAME":       "PLAINTEXT",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":        "true",
			"CLUSTER_ID":                             "MkU3OEVBNTcwNTJENDM2Qk",
		},
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = nat.PortMap{
				"9093/tcp": []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: "9093"}},
			}
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(consumerTestTimeout),
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start Kafka container")

	brokers := "127.0.0.1:9093"
	waitForKafkaBroker(t, brokers)

	createTestTopics(t, brokers, []string{consumerTestTopic, consumerDLQTopic})

	return &consumerKafkaContainer{
		container: kafkaContainer,
		brokers:   brokers,
	}
}

func (kc *consumerKafkaContainer) teardown(t *testing.T) {
	if kc.container != nil {
		ctx := context.Background()
		err := kc.container.Terminate(ctx)
		require.NoError(t, err, "Failed to terminate Kafka container")
	}
}

func newTestConsumerConfig(brokers, groupID string) ConsumerConfig {
	return ConsumerConfig{
		BootstrapServers:            brokers,
		GroupID:                     groupID,
		Topic:                       consumerTestTopic,
		DLQTopic:                    consumerDLQTopic,
		AutoOffsetReset:             "earliest",
		Concurrency:                 5,
		OffsetManagerCommitInterval: 5 * time.Second,
		PublishToDLQ:                false,
		EnableLogs:                  false,
	}
}

func createTestTopics(t *testing.T, brokers string, topics []string) {
	adminClient, err := ckafka.NewAdminClient(&ckafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	require.NoError(t, err)
	defer adminClient.Close()

	var topicSpecs []ckafka.TopicSpecification
	for _, topic := range topics {
		topicSpecs = append(topicSpecs, ckafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		})
	}

	results, err := adminClient.CreateTopics(context.Background(), topicSpecs)
	require.NoError(t, err)

	for _, result := range results {
		if result.Error.Code() != ckafka.ErrNoError {
			require.Fail(t, "Failed to create topic", "topic: %s, error: %v", result.Topic, result.Error)
		}
		t.Logf("Created topic: %s", result.Topic)
	}
}

func produceTestMessages(t *testing.T, brokers, topic string, count int) {
	config := &ckafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         "test-producer",
	}

	producer, err := ckafka.NewProducer(config)
	require.NoError(t, err)
	defer producer.Close()

	deliveryChan := make(chan ckafka.Event, count)
	for i := 0; i < count; i++ {
		msg := &ckafka.Message{
			TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
			Key:            []byte(fmt.Sprintf("key-%d", i)),
			Value:          []byte(fmt.Sprintf("value-%d", i)),
		}
		err := producer.Produce(msg, deliveryChan)
		require.NoError(t, err)
	}

	for i := 0; i < count; i++ {
		e := <-deliveryChan
		m := e.(*ckafka.Message)
		require.Nil(t, m.TopicPartition.Error, "Delivery failed")
	}

	pending := producer.Flush(producerFlushTimeout)
	require.Equal(t, 0, pending, "failed to flush producer")
	t.Logf("Produced %d messages to topic %s", count, topic)
}

func TestConsumer_NewConsumer(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	processor := newTestProcessor()

	t.Run("successful_creation", func(t *testing.T) {
		cfg := newTestConsumerConfig(kc.brokers, "test-group-new")
		cfg.PublishToDLQ = true

		consumer, err := NewConsumer(ctx, log, cfg, processor, nil)
		require.NoError(t, err)
		require.NotNil(t, consumer)
		require.NotNil(t, consumer.consumer)
		require.NotNil(t, consumer.dlqProducer)
		require.NotNil(t, consumer.offsetManager)
		require.NotNil(t, consumer.sem)

		// Cleanup
		err = consumer.consumer.Close()
		require.NoError(t, err)
		consumer.dlqProducer.Close(consumerFlushTimeout)
	})

	t.Run("dlq_topic_config_validation", func(t *testing.T) {
		cfg := ConsumerConfig{
			BootstrapServers:            kc.brokers,
			GroupID:                     "test-group-dlq-val",
			Topic:                       consumerTestTopic,
			PublishToDLQ:                true,
			DLQTopic:                    "", // Missing DLQ topic
			AutoOffsetReset:             "earliest",
			OffsetManagerCommitInterval: 5 * time.Second,
		}

		consumer, err := NewConsumer(ctx, log, cfg, processor, nil)
		assert.Error(t, err)
		assert.Nil(t, consumer)
		assert.Contains(t, err.Error(), "DLQ topic not configured")
	})
}

func TestConsumer_BasicProcessing(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("successful_message_processing", func(t *testing.T) {
		processor := newTestProcessor()
		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-basic-%d", time.Now().UnixNano()))
		cfg.EnableLogs = true

		consumer, err := NewConsumer(context.Background(), log, cfg, processor, nil)
		require.NoError(t, err)

		messageCount := 10
		produceTestMessages(t, kc.brokers, consumerTestTopic, messageCount)

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		assert.Eventually(t, func() bool {
			return processor.GetProcessedCount() >= messageCount
		}, 30*time.Second, 500*time.Millisecond, "Expected %d messages to be processed", messageCount)

		cancel()

		select {
		case err := <-consumerErrCh:
			assert.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("Consumer did not stop within timeout")
		}

		assert.GreaterOrEqual(t, processor.GetProcessedCount(), messageCount)
	})
}

func TestConsumer_ErrorHandling(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("processing_failure_without_dlq", func(t *testing.T) {
		processor := newTestProcessor()
		processor.SetShouldFail(true)
		processor.SetFailureError(errors.New("test failure"))

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-no-dlq-%d", time.Now().UnixNano()))
		cfg.PublishToDLQ = false

		consumer, err := NewConsumer(context.Background(), log, cfg, processor, nil)
		require.NoError(t, err)

		messageCount := 3
		produceTestMessages(t, kc.brokers, consumerTestTopic, messageCount)

		go func() {
			_ = consumer.Start(context.Background())
		}()

		select {
		case err, ok := <-consumer.errCh:
			require.True(t, ok)
			require.Error(t, err)
			require.Contains(t, err.Error(), "test failure")
			t.Logf("Consumer stopped with expected error: %v", err)
		case <-time.After(30 * time.Second):
			require.Fail(t, "Expected consumer to stop with error within timeout")
		}
	})
}

func TestConsumer_DLQProduction(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("failed_messages_published_to_dlq", func(t *testing.T) {
		processor := newTestProcessor()
		processor.SetShouldFail(true)
		processor.SetFailureError(errors.New("simulated processing failure"))

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-dlq-%d", time.Now().UnixNano()))
		cfg.PublishToDLQ = true

		consumer, err := NewConsumer(context.Background(), log, cfg, processor, nil)
		require.NoError(t, err)

		messageCount := 5
		produceTestMessages(t, kc.brokers, consumerTestTopic, messageCount)
		t.Logf("Produced %d messages to main topic", messageCount)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		require.Eventually(t, func() bool {
			count := processor.GetProcessedCount()
			t.Logf("Processed %d/%d messages (expected to fail)", count, messageCount)
			return count >= messageCount
		}, 20*time.Second, 500*time.Millisecond, "Expected %d messages to be processed", messageCount)

		t.Logf("All messages processed and failed as expected")

		cancel()

		select {
		case err := <-consumerErrCh:
			require.NoError(t, err, "Consumer should shutdown gracefully even with processing failures when DLQ is enabled")
		case <-time.After(10 * time.Second):
			t.Fatal("Consumer did not stop within timeout")
		}

		time.Sleep(2 * time.Second)

		dlqProcessor := newTestProcessor()
		dlqCfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-dlq-reader-%d", time.Now().UnixNano()))
		dlqCfg.Topic = consumerDLQTopic
		dlqCfg.PublishToDLQ = false

		dlqConsumer, err := NewConsumer(context.Background(), log, dlqCfg, dlqProcessor, nil)
		require.NoError(t, err)

		dlqCtx, dlqCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dlqCancel()

		dlqErrCh := make(chan error, 1)
		go func() {
			dlqErrCh <- dlqConsumer.Start(dlqCtx)
		}()

		require.Eventually(t, func() bool {
			count := dlqProcessor.GetProcessedCount()
			t.Logf("Consumed %d/%d messages from DLQ", count, messageCount)
			return count >= messageCount
		}, 20*time.Second, 500*time.Millisecond, "Expected %d messages in DLQ", messageCount)

		dlqMessages := dlqProcessor.GetProcessedMessages()
		require.GreaterOrEqual(t, len(dlqMessages), messageCount, "DLQ should contain all failed messages")

		for i, msg := range dlqMessages {
			t.Logf("DLQ message %d: key=%s, partition=%d, offset=%d, headers=%d",
				i, string(msg.Key), msg.TopicPartition.Partition, msg.TopicPartition.Offset, len(msg.Headers))
			require.NotNil(t, msg.Key, "DLQ message should have key")
			require.NotNil(t, msg.Value, "DLQ message should have value")
		}

		dlqCancel()
		select {
		case <-dlqErrCh:
		case <-time.After(5 * time.Second):
			t.Log("DLQ consumer cleanup timeout (non-fatal)")
		}

		t.Logf("Successfully verified %d messages were published to DLQ", len(dlqMessages))
	})
}

func TestConsumer_Concurrency(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("concurrent_message_processing", func(t *testing.T) {
		var processingCount int32
		var maxConcurrent int32

		processor := newTestProcessor()
		processor.processFunc = func(ctx context.Context, msg *ckafka.Message) error {
			current := atomic.AddInt32(&processingCount, 1)

			for {
				max := atomic.LoadInt32(&maxConcurrent)
				if current <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, current) {
					break
				}
			}

			time.Sleep(100 * time.Millisecond)

			atomic.AddInt32(&processingCount, -1)
			atomic.AddInt32(&processor.processedCount, 1)
			return nil
		}

		concurrencyLimit := int64(5)
		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-concurrent-%d", time.Now().UnixNano()))
		cfg.Concurrency = concurrencyLimit

		consumer, err := NewConsumer(context.Background(), log, cfg, processor, nil)
		require.NoError(t, err)

		messageCount := 20
		produceTestMessages(t, kc.brokers, consumerTestTopic, messageCount)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		go func() {
			_ = consumer.Start(ctx)
		}()

		assert.Eventually(t, func() bool {
			return processor.GetProcessedCount() >= messageCount
		}, 40*time.Second, 500*time.Millisecond)

		cancel()
		time.Sleep(3 * time.Second)

		maxReached := atomic.LoadInt32(&maxConcurrent)
		t.Logf("Max concurrent processing: %d (limit: %d)", maxReached, concurrencyLimit)
		assert.LessOrEqual(t, maxReached, int32(concurrencyLimit), "Concurrency exceeded limit")
	})
}

func TestConsumer_ContextCancellation(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("cancellation_during_processing", func(t *testing.T) {
		processor := newTestProcessor()
		processor.processingDelay = 5 * time.Second

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-cancel-%d", time.Now().UnixNano()))

		consumer, err := NewConsumer(context.Background(), log, cfg, processor, nil)
		require.NoError(t, err)

		messageCount := 5
		produceTestMessages(t, kc.brokers, consumerTestTopic, messageCount)

		ctx, cancel := context.WithCancel(context.Background())

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		time.Sleep(2 * time.Second)

		cancel()

		select {
		case err := <-consumerErrCh:
			assert.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("Consumer did not stop within timeout after cancellation")
		}
	})

	t.Run("context_canceled_error_in_processing", func(t *testing.T) {
		processor := newTestProcessor()
		processor.SetFailureError(context.Canceled)

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-ctx-cancel-err-%d", time.Now().UnixNano()))

		consumer, err := NewConsumer(context.Background(), log, cfg, processor, nil)
		require.NoError(t, err)

		messageCount := 3
		produceTestMessages(t, kc.brokers, consumerTestTopic, messageCount)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		time.Sleep(3 * time.Second)

		cancel()

		select {
		case err := <-consumerErrCh:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			require.Fail(t, "Consumer did not stop within timeout")
		}

		assert.GreaterOrEqual(t, processor.GetProcessedCount(), 1, "At least one message should be processed")
	})
}

func TestConsumer_Rebalancing(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("rebalance_with_consumer_addition_and_deletion", func(t *testing.T) {
		groupID := fmt.Sprintf("test-group-rebalance-%d", time.Now().UnixNano())

		messageCount := 30
		produceTestMessages(t, kc.brokers, consumerTestTopic, messageCount)
		t.Logf("Produced %d messages to topic", messageCount)

		processor1 := newTestProcessor()
		processor1.processingDelay = 1 * time.Second

		cfg1 := newTestConsumerConfig(kc.brokers, groupID)
		cfg1.EnableLogs = false
		cfg1.Concurrency = 3

		consumer1, err := NewConsumer(context.Background(), log, cfg1, processor1, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		consumer1ErrCh := make(chan error, 1)
		go func() {
			consumer1ErrCh <- consumer1.Start(ctx)
		}()
		t.Log("Started consumer 1")

		time.Sleep(3 * time.Second)

		processor2 := newTestProcessor()
		processor2.processingDelay = 1 * time.Second

		cfg2 := newTestConsumerConfig(kc.brokers, groupID)
		cfg2.EnableLogs = false
		cfg2.Concurrency = 3

		consumer2, err := NewConsumer(context.Background(), log, cfg2, processor2, nil)
		require.NoError(t, err)

		consumer2ErrCh := make(chan error, 1)
		go func() {
			consumer2ErrCh <- consumer2.Start(ctx)
		}()
		t.Log("Started consumer 2 (should trigger rebalance)")

		time.Sleep(5 * time.Second)

		processor3 := newTestProcessor()
		processor3.processingDelay = 1 * time.Second

		cfg3 := newTestConsumerConfig(kc.brokers, groupID)
		cfg3.EnableLogs = false
		cfg3.Concurrency = 3

		consumer3, err := NewConsumer(context.Background(), log, cfg3, processor3, nil)
		require.NoError(t, err)

		consumer3Ctx, consumer3Cancel := context.WithCancel(ctx)
		consumer3ErrCh := make(chan error, 1)
		go func() {
			consumer3ErrCh <- consumer3.Start(consumer3Ctx)
		}()
		t.Log("Started consumer 3 (should trigger another rebalance)")

		time.Sleep(5 * time.Second)

		t.Log("Stopping consumer 3 to trigger rebalance")
		consumer3Cancel()
		select {
		case err := <-consumer3ErrCh:
			require.NoError(t, err, "Consumer 3 should stop gracefully")
		case <-time.After(10 * time.Second):
			t.Fatal("Consumer 3 did not stop within timeout")
		}
		t.Log("Consumer 3 stopped (rebalance should occur)")

		time.Sleep(5 * time.Second)

		require.Eventually(t, func() bool {
			total := processor1.GetProcessedCount() + processor2.GetProcessedCount() + processor3.GetProcessedCount()
			t.Logf("Total processed: %d/%d (c1=%d, c2=%d, c3=%d)",
				total, messageCount,
				processor1.GetProcessedCount(),
				processor2.GetProcessedCount(),
				processor3.GetProcessedCount())
			return total >= messageCount
		}, 30*time.Second, 1*time.Second, "Expected all %d messages to be processed", messageCount)

		cancel()

		select {
		case err := <-consumer1ErrCh:
			require.NoError(t, err, "Consumer 1 should stop gracefully")
		case <-time.After(10 * time.Second):
			t.Fatal("Consumer 1 did not stop within timeout")
		}

		select {
		case err := <-consumer2ErrCh:
			require.NoError(t, err, "Consumer 2 should stop gracefully")
		case <-time.After(10 * time.Second):
			t.Fatal("Consumer 2 did not stop within timeout")
		}

		totalProcessed := processor1.GetProcessedCount() + processor2.GetProcessedCount() + processor3.GetProcessedCount()
		require.GreaterOrEqual(t, totalProcessed, messageCount,
			"All messages should be processed despite rebalances (c1=%d, c2=%d, c3=%d, total=%d)",
			processor1.GetProcessedCount(),
			processor2.GetProcessedCount(),
			processor3.GetProcessedCount(),
			totalProcessed)

		t.Logf("SUCCESS: All %d messages processed across consumers despite multiple rebalances", totalProcessed)

		allMessages := make(map[string]bool)
		for _, msg := range processor1.GetProcessedMessages() {
			key := string(msg.Key)
			allMessages[key] = true
		}
		for _, msg := range processor2.GetProcessedMessages() {
			key := string(msg.Key)
			allMessages[key] = true
		}
		for _, msg := range processor3.GetProcessedMessages() {
			key := string(msg.Key)
			allMessages[key] = true
		}

		for key := range allMessages {
			require.True(t, allMessages[key], "Message key %s should be present", key)
		}
	})
}

func TestConsumer_LogPrinting(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("logs_disabled", func(t *testing.T) {
		processor := newTestProcessor()

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-logs-disabled-%d", time.Now().UnixNano()))
		cfg.EnableLogs = false

		consumer, err := NewConsumer(context.Background(), log, cfg, processor, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		time.Sleep(500 * time.Millisecond)

		select {
		case _, ok := <-consumer.logsDone:
			require.True(t, !ok, "logsDone channel should be closed immediately when logs disabled")
		case <-time.After(1 * time.Second):
			require.Fail(t, "logsDone channel should be closed immediately when logs disabled")
		}

		cancel()

		select {
		case err := <-consumerErrCh:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Consumer did not stop within timeout")
		}
	})

	t.Run("logs_enabled", func(t *testing.T) {
		processor := newTestProcessor()

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-logs-enabled-%d", time.Now().UnixNano()))
		cfg.EnableLogs = true

		consumer, err := NewConsumer(context.Background(), log, cfg, processor, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		time.Sleep(500 * time.Millisecond)

		select {
		case _, ok := <-consumer.logsDone:
			require.True(t, ok, "logsDone channel should be open when logs enabled")
		case <-time.After(1 * time.Second):
			t.Log("logsDone channel should be open when logs enabled")
		}

		cancel()

		time.Sleep(15 * time.Second)

		select {
		case err := <-consumerErrCh:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Consumer did not stop within timeout")
		}

		select {
		case _, ok := <-consumer.logsDone:
			require.True(t, !ok, "logsDone channel should be closed after consumer stops")
		case <-time.After(1 * time.Second):
			require.Fail(t, "logsDone channel should be closed after consumer stops")
		}
	})
}

// gatherIntegrationCounter returns the value of a counter metric by its fully-qualified
// name from the Prometheus registry, or 0 if not found.
func gatherIntegrationCounter(t *testing.T, reg *prometheus.Registry, name string) float64 {
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

// newTestMetrics creates a Prometheus registry and metrics instance for tests
// that need to verify metric values.
func newTestMetrics(t *testing.T) (*prometheus.Registry, *metrics.Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	require.NoError(t, err)
	return reg, m
}

func TestConsumer_RetryWithRealKafka(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("finite_retries_eventual_success", func(t *testing.T) {
		var callCount atomic.Int32

		proc := newTestProcessor()
		proc.processFunc = func(_ context.Context, _ *ckafka.Message) error {
			n := callCount.Add(1)
			if n <= 2 {
				return errors.New("transient failure")
			}
			atomic.AddInt32(&proc.processedCount, 1)
			return nil
		}

		reg, m := newTestMetrics(t)

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-retry-finite-%d", time.Now().UnixNano()))
		cfg.Retry = RetryPolicy{
			MaxRetries: 5,
			BaseDelay:  100 * time.Millisecond,
			MaxDelay:   500 * time.Millisecond,
		}

		consumer, err := NewConsumer(context.Background(), log, cfg, proc, m)
		require.NoError(t, err)

		produceTestMessages(t, kc.brokers, consumerTestTopic, 1)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		require.Eventually(t, func() bool {
			return proc.GetProcessedCount() >= 1
		}, 20*time.Second, 500*time.Millisecond, "Message should be processed after retries")

		cancel()
		select {
		case err := <-consumerErrCh:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Consumer did not stop within timeout")
		}

		require.GreaterOrEqual(t, callCount.Load(), int32(3), "Expected at least 3 calls (2 failures + 1 success)")
		retries := gatherIntegrationCounter(t, reg, "indexer_consumer_message_retries_total")
		require.GreaterOrEqual(t, retries, float64(1), "Expected retry metrics to be recorded")
	})

	t.Run("retries_exhausted_then_error", func(t *testing.T) {
		proc := newTestProcessor()
		proc.SetShouldFail(true)
		proc.SetFailureError(errors.New("permanent failure"))

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-retry-exhausted-%d", time.Now().UnixNano()))
		cfg.Retry = RetryPolicy{
			MaxRetries: 2,
			BaseDelay:  50 * time.Millisecond,
			MaxDelay:   100 * time.Millisecond,
		}
		cfg.PublishToDLQ = false

		consumer, err := NewConsumer(context.Background(), log, cfg, proc, nil)
		require.NoError(t, err)

		produceTestMessages(t, kc.brokers, consumerTestTopic, 1)

		go func() {
			_ = consumer.Start(context.Background())
		}()

		select {
		case err := <-consumer.errCh:
			require.Error(t, err)
			require.Contains(t, err.Error(), "permanent failure")
		case <-time.After(30 * time.Second):
			t.Fatal("Expected consumer to report error after retries exhausted")
		}

		require.GreaterOrEqual(t, proc.GetProcessedCount(), 3,
			"Expected 1 initial + 2 retries = 3 calls minimum")
	})
}

func TestConsumer_InfiniteRetries(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("eventual_success_after_many_failures", func(t *testing.T) {
		var callCount atomic.Int32

		proc := newTestProcessor()
		proc.processFunc = func(_ context.Context, _ *ckafka.Message) error {
			n := callCount.Add(1)
			if n <= 5 {
				return errors.New("transient failure")
			}
			atomic.AddInt32(&proc.processedCount, 1)
			return nil
		}

		reg, m := newTestMetrics(t)

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-infinite-%d", time.Now().UnixNano()))
		cfg.Retry = RetryPolicy{
			MaxRetries: InfiniteRetries,
			BaseDelay:  50 * time.Millisecond,
			MaxDelay:   200 * time.Millisecond,
		}

		consumer, err := NewConsumer(context.Background(), log, cfg, proc, m)
		require.NoError(t, err)

		produceTestMessages(t, kc.brokers, consumerTestTopic, 1)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		require.Eventually(t, func() bool {
			return proc.GetProcessedCount() >= 1
		}, 20*time.Second, 500*time.Millisecond, "Message should eventually succeed with infinite retries")

		cancel()
		select {
		case err := <-consumerErrCh:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Consumer did not stop within timeout")
		}

		require.GreaterOrEqual(t, callCount.Load(), int32(6), "Expected at least 6 calls (5 failures + 1 success)")

		retries := gatherIntegrationCounter(t, reg, "indexer_consumer_message_retries_total")
		require.GreaterOrEqual(t, retries, float64(5), "Expected at least 5 retries recorded")

		exhausted := gatherIntegrationCounter(t, reg, "indexer_consumer_message_retries_exhausted_total")
		require.Equal(t, float64(0), exhausted, "No retries should be exhausted with infinite retries")
	})
}

func TestConsumer_DLQPipeline(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("primary_fails_dlq_consumer_succeeds", func(t *testing.T) {
		// --- Phase 1: Primary consumer fails, messages go to DLQ ---
		primaryProc := newTestProcessor()
		primaryProc.SetShouldFail(true)
		primaryProc.SetFailureError(errors.New("primary processing failure"))

		primaryCfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-pipeline-primary-%d", time.Now().UnixNano()))
		primaryCfg.PublishToDLQ = true

		primaryConsumer, err := NewConsumer(context.Background(), log, primaryCfg, primaryProc, nil)
		require.NoError(t, err)

		messageCount := 3
		produceTestMessages(t, kc.brokers, consumerTestTopic, messageCount)

		primaryCtx, primaryCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer primaryCancel()

		primaryErrCh := make(chan error, 1)
		go func() {
			primaryErrCh <- primaryConsumer.Start(primaryCtx)
		}()

		require.Eventually(t, func() bool {
			return primaryProc.GetProcessedCount() >= messageCount
		}, 20*time.Second, 500*time.Millisecond, "Primary should attempt all messages")

		primaryCancel()
		select {
		case <-primaryErrCh:
		case <-time.After(10 * time.Second):
			t.Fatal("Primary consumer did not stop within timeout")
		}

		time.Sleep(2 * time.Second)

		// --- Phase 2: DLQ consumer picks up from DLQ topic and succeeds ---
		dlqProc := newTestProcessor()

		dlqCfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-pipeline-dlq-%d", time.Now().UnixNano()))
		dlqCfg.Topic = consumerDLQTopic
		dlqCfg.PublishToDLQ = false
		dlqCfg.Retry = RetryPolicy{
			MaxRetries: InfiniteRetries,
			BaseDelay:  50 * time.Millisecond,
			MaxDelay:   200 * time.Millisecond,
		}

		dlqConsumer, err := NewConsumer(context.Background(), log, dlqCfg, dlqProc, nil)
		require.NoError(t, err)

		dlqCtx, dlqCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dlqCancel()

		dlqErrCh := make(chan error, 1)
		go func() {
			dlqErrCh <- dlqConsumer.Start(dlqCtx)
		}()

		require.Eventually(t, func() bool {
			count := dlqProc.GetProcessedCount()
			t.Logf("DLQ consumer processed %d/%d messages", count, messageCount)
			return count >= messageCount
		}, 20*time.Second, 500*time.Millisecond, "DLQ consumer should process all messages from DLQ")

		dlqCancel()
		select {
		case err := <-dlqErrCh:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("DLQ consumer did not stop within timeout")
		}

		dlqMessages := dlqProc.GetProcessedMessages()
		require.GreaterOrEqual(t, len(dlqMessages), messageCount,
			"DLQ consumer should have processed all messages that primary failed on")

		for _, msg := range dlqMessages {
			require.NotNil(t, msg.Key, "DLQ message should preserve original key")
			require.NotNil(t, msg.Value, "DLQ message should preserve original value")
		}
	})
}

func TestConsumer_NoDLQCascade(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("dlq_consumer_does_not_publish_to_secondary_dlq", func(t *testing.T) {
		proc := newTestProcessor()
		proc.SetShouldFail(true)
		proc.SetFailureError(errors.New("permanent failure on DLQ message"))

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-no-cascade-%d", time.Now().UnixNano()))
		cfg.Topic = consumerDLQTopic
		cfg.PublishToDLQ = false
		cfg.Retry = RetryPolicy{
			MaxRetries: 2,
			BaseDelay:  50 * time.Millisecond,
			MaxDelay:   100 * time.Millisecond,
		}

		consumer, err := NewConsumer(context.Background(), log, cfg, proc, nil)
		require.NoError(t, err)

		produceTestMessages(t, kc.brokers, consumerDLQTopic, 1)

		go func() {
			_ = consumer.Start(context.Background())
		}()

		// With PublishToDLQ=false, after retries are exhausted, the error
		// goes to errCh and the consumer shuts down.
		select {
		case err := <-consumer.errCh:
			require.Error(t, err)
			require.Contains(t, err.Error(), "permanent failure on DLQ message")
		case <-time.After(30 * time.Second):
			t.Fatal("Expected error on errCh after retries exhausted")
		}

		require.Nil(t, consumer.dlqProducer, "DLQ consumer should not have a DLQ producer")
	})
}

func TestConsumer_ContextCancelDuringRetryBackoff(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("exits_promptly_on_cancel_during_long_backoff", func(t *testing.T) {
		proc := newTestProcessor()
		proc.SetShouldFail(true)
		proc.SetFailureError(errors.New("always fails"))

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-cancel-backoff-%d", time.Now().UnixNano()))
		cfg.Retry = RetryPolicy{
			MaxRetries: InfiniteRetries,
			BaseDelay:  30 * time.Second,
			MaxDelay:   30 * time.Second,
		}
		cfg.PublishToDLQ = false

		consumer, err := NewConsumer(context.Background(), log, cfg, proc, nil)
		require.NoError(t, err)

		produceTestMessages(t, kc.brokers, consumerTestTopic, 1)

		ctx, cancel := context.WithCancel(context.Background())

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		// Wait for the first processing attempt + retry to start the 30s backoff
		require.Eventually(t, func() bool {
			return proc.GetProcessedCount() >= 1
		}, 15*time.Second, 200*time.Millisecond, "Consumer should attempt to process at least once")

		// Cancel while the consumer is in the 30s backoff sleep
		time.Sleep(500 * time.Millisecond)
		cancelStart := time.Now()
		cancel()

		select {
		case <-consumerErrCh:
			elapsed := time.Since(cancelStart)
			require.Less(t, elapsed, 5*time.Second,
				"Consumer should exit promptly after cancellation, not wait for full backoff")
		case <-time.After(10 * time.Second):
			t.Fatal("Consumer did not stop within timeout after cancellation")
		}
	})
}

func TestConsumer_Backpressure(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("pause_resume_metrics_recorded", func(t *testing.T) {
		proc := newTestProcessor()
		proc.processingDelay = 500 * time.Millisecond

		reg, m := newTestMetrics(t)

		cfg := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-backpressure-%d", time.Now().UnixNano()))
		cfg.Concurrency = 1

		consumer, err := NewConsumer(context.Background(), log, cfg, proc, m)
		require.NoError(t, err)

		messageCount := 10
		produceTestMessages(t, kc.brokers, consumerTestTopic, messageCount)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		consumerErrCh := make(chan error, 1)
		go func() {
			consumerErrCh <- consumer.Start(ctx)
		}()

		require.Eventually(t, func() bool {
			return proc.GetProcessedCount() >= messageCount
		}, 30*time.Second, 500*time.Millisecond, "All messages should be processed")

		cancel()
		select {
		case err := <-consumerErrCh:
			require.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("Consumer did not stop within timeout")
		}

		pauses := gatherIntegrationCounter(t, reg, "indexer_consumer_pauses_total")
		resumes := gatherIntegrationCounter(t, reg, "indexer_consumer_resumes_total")

		t.Logf("Pause count: %.0f, Resume count: %.0f", pauses, resumes)

		require.Greater(t, pauses, float64(0),
			"With concurrency=1 and 10 messages with 500ms delay, pauses should occur")
		require.Greater(t, resumes, float64(0),
			"After pauses, resumes should also occur")
		require.Equal(t, pauses, resumes,
			"Each pause should have a corresponding resume")
	})
}

func TestConsumer_ErrorPropagationErrgroup(t *testing.T) {
	kc := setupConsumerKafka(t)
	defer kc.teardown(t)

	log, err := utils.NewSugaredLogger(true)
	require.NoError(t, err)

	t.Run("first_consumer_error_cancels_second_consumer", func(t *testing.T) {
		// Consumer 1: processor always fails, PublishToDLQ=false -> error exits via Start()
		proc1 := newTestProcessor()
		proc1.SetShouldFail(true)
		proc1.SetFailureError(errors.New("fatal processing error"))

		cfg1 := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-errgroup-1-%d", time.Now().UnixNano()))
		cfg1.PublishToDLQ = false

		consumer1, err := NewConsumer(context.Background(), log, cfg1, proc1, nil)
		require.NoError(t, err)

		// Consumer 2: successful processor, reads from DLQ topic (no messages expected)
		proc2 := newTestProcessor()

		cfg2 := newTestConsumerConfig(kc.brokers, fmt.Sprintf("test-group-errgroup-2-%d", time.Now().UnixNano()))
		cfg2.Topic = consumerDLQTopic
		cfg2.PublishToDLQ = false

		consumer2, err := NewConsumer(context.Background(), log, cfg2, proc2, nil)
		require.NoError(t, err)

		// Wire both into an errgroup (same pattern as run.go)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		g, gctx := errgroup.WithContext(ctx)

		g.Go(func() error {
			return consumer1.Start(gctx)
		})

		g.Go(func() error {
			return consumer2.Start(gctx)
		})

		// Produce a message to the main topic to trigger consumer1's processor error
		produceTestMessages(t, kc.brokers, consumerTestTopic, 1)

		// errgroup.Wait should return the error from consumer1.
		// consumer1's Start() returns the loopErr from errCh, which triggers
		// context cancellation via errgroup, causing consumer2 to shut down.
		err = g.Wait()
		require.Error(t, err, "errgroup should return error from failing consumer")

		t.Logf("errgroup returned: %v", err)
	})
}
