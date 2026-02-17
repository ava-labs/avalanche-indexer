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

	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
)

type testProcessor struct {
	processFunc     func(ctx context.Context, msg *kafka.Message) error
	mu              sync.Mutex
	processedCount  int32
	processedMsgs   []*kafka.Message
	shouldFail      bool
	failureError    error
	processingDelay time.Duration
}

func newTestProcessor() *testProcessor {
	return &testProcessor{
		processedMsgs: make([]*kafka.Message, 0),
	}
}

func (p *testProcessor) Process(ctx context.Context, msg *kafka.Message) error {
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

func (p *testProcessor) GetProcessedMessages() []*kafka.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*kafka.Message{}, p.processedMsgs...)
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
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	require.NoError(t, err)
	defer adminClient.Close()

	var topicSpecs []kafka.TopicSpecification
	for _, topic := range topics {
		topicSpecs = append(topicSpecs, kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		})
	}

	results, err := adminClient.CreateTopics(context.Background(), topicSpecs)
	require.NoError(t, err)

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			require.Fail(t, "Failed to create topic", "topic: %s, error: %v", result.Topic, result.Error)
		}
		t.Logf("Created topic: %s", result.Topic)
	}
}

func produceTestMessages(t *testing.T, brokers, topic string, count int) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         "test-producer",
	}

	producer, err := kafka.NewProducer(config)
	require.NoError(t, err)
	defer producer.Close()

	deliveryChan := make(chan kafka.Event, count)
	for i := 0; i < count; i++ {
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(fmt.Sprintf("key-%d", i)),
			Value:          []byte(fmt.Sprintf("value-%d", i)),
		}
		err := producer.Produce(msg, deliveryChan)
		require.NoError(t, err)
	}

	for i := 0; i < count; i++ {
		e := <-deliveryChan
		m := e.(*kafka.Message)
		require.Nil(t, m.TopicPartition.Error, "Delivery failed")
	}

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
		processor.processFunc = func(ctx context.Context, msg *kafka.Message) error {
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
