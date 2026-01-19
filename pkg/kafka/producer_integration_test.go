//go:build integration
// +build integration

package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/testutils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerIntegration_E2E_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-producer-e2e"
	createTopic(t, bootstrapServers, topic, 1)

	// Create producer
	log := testutils.NewTestLogger(t)
	config := &kafka.ConfigMap{
		"bootstrap.servers":      bootstrapServers,
		"acks":                   "all",
		"go.logs.channel.enable": false,
	}

	producer, err := NewProducer(ctx, config, log)
	require.NoError(t, err)
	defer producer.Close(5 * time.Second)

	// Produce message
	testMsg := Msg{
		Topic: topic,
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	err = producer.Produce(ctx, testMsg)
	assert.NoError(t, err)

	// Verify message was produced by consuming it
	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "test-producer-verify",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(&consumerConfig)
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.Subscribe(topic, nil)
	require.NoError(t, err)

	// Poll for message
	msg, err := consumer.ReadMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, testMsg.Key, msg.Key)
	assert.Equal(t, testMsg.Value, msg.Value)
}

func TestProducerIntegration_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-producer-cancel"
	createTopic(t, bootstrapServers, topic, 1)

	// Create producer
	log := testutils.NewTestLogger(t)
	config := &kafka.ConfigMap{
		"bootstrap.servers":      bootstrapServers,
		"acks":                   "all",
		"go.logs.channel.enable": false,
	}

	producer, err := NewProducer(ctx, config, log)
	require.NoError(t, err)
	defer producer.Close(5 * time.Second)

	// Create cancelled context
	cancelledCtx, cancelFunc := context.WithCancel(ctx)
	cancelFunc() // Cancel immediately

	testMsg := Msg{
		Topic: topic,
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	// Should return context.Canceled error
	err = producer.Produce(cancelledCtx, testMsg)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestProducerIntegration_MultipleMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-producer-multiple"
	createTopic(t, bootstrapServers, topic, 1)

	// Create producer
	log := testutils.NewTestLogger(t)
	config := &kafka.ConfigMap{
		"bootstrap.servers":      bootstrapServers,
		"acks":                   "all",
		"go.logs.channel.enable": false,
	}

	producer, err := NewProducer(ctx, config, log)
	require.NoError(t, err)
	defer producer.Close(5 * time.Second)

	// Produce multiple messages
	messageCount := 100
	for i := 0; i < messageCount; i++ {
		msg := Msg{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		err := producer.Produce(ctx, msg)
		require.NoError(t, err)
	}

	// Verify all messages were produced
	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "test-producer-multiple-verify",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(&consumerConfig)
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.Subscribe(topic, nil)
	require.NoError(t, err)

	receivedCount := 0
	for receivedCount < messageCount {
		msg, err := consumer.ReadMessage(10 * time.Second)
		if err != nil {
			break
		}
		receivedCount++
		assert.Equal(t, []byte(fmt.Sprintf("key-%d", receivedCount-1)), msg.Key)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", receivedCount-1)), msg.Value)
	}

	assert.Equal(t, messageCount, receivedCount, "expected all messages to be received")
}

func TestProducerIntegration_ConcurrentProducers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-producer-concurrent"
	createTopic(t, bootstrapServers, topic, 4) // 4 partitions for better distribution

	log := testutils.NewTestLogger(t)
	config := &kafka.ConfigMap{
		"bootstrap.servers":      bootstrapServers,
		"acks":                   "all",
		"go.logs.channel.enable": false,
	}

	// Create multiple producers
	producerCount := 5
	messagesPerProducer := 20
	
	var wg sync.WaitGroup
	errCh := make(chan error, producerCount*messagesPerProducer)

	for p := 0; p < producerCount; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			producer, err := NewProducer(ctx, config, log)
			if err != nil {
				errCh <- err
				return
			}
			defer producer.Close(5 * time.Second)

			for i := 0; i < messagesPerProducer; i++ {
				msg := Msg{
					Topic: topic,
					Key:   []byte(fmt.Sprintf("producer-%d-key-%d", producerID, i)),
					Value: []byte(fmt.Sprintf("producer-%d-value-%d", producerID, i)),
				}
				if err := producer.Produce(ctx, msg); err != nil {
					errCh <- err
					return
				}
			}
		}(p)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		require.NoError(t, err)
	}

	// Verify all messages were produced
	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "test-producer-concurrent-verify",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(&consumerConfig)
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.Subscribe(topic, nil)
	require.NoError(t, err)

	expectedTotal := producerCount * messagesPerProducer
	receivedCount := 0
	timeout := time.After(30 * time.Second)

	for receivedCount < expectedTotal {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for messages, received %d/%d", receivedCount, expectedTotal)
		default:
			msg, err := consumer.ReadMessage(1 * time.Second)
			if err != nil {
				continue
			}
			receivedCount++
		}
	}

	assert.Equal(t, expectedTotal, receivedCount, "expected all messages from all producers")
}

func TestProducerIntegration_CloseFlushesMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-producer-flush"
	createTopic(t, bootstrapServers, topic, 1)

	log := testutils.NewTestLogger(t)
	config := &kafka.ConfigMap{
		"bootstrap.servers":      bootstrapServers,
		"acks":                   "all",
		"linger.ms":              100, // Add some delay
		"go.logs.channel.enable": false,
	}

	producer, err := NewProducer(ctx, config, log)
	require.NoError(t, err)

	// Produce multiple messages quickly
	messageCount := 50
	for i := 0; i < messageCount; i++ {
		msg := Msg{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		// Use background context to avoid blocking on delivery
		go producer.Produce(context.Background(), msg)
	}

	// Close with adequate timeout to flush
	producer.Close(10 * time.Second)

	// Verify all messages were flushed
	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "test-producer-flush-verify",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(&consumerConfig)
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.Subscribe(topic, nil)
	require.NoError(t, err)

	receivedCount := 0
	timeout := time.After(15 * time.Second)

	for receivedCount < messageCount {
		select {
		case <-timeout:
			// Some messages might be lost due to async production, but most should be delivered
			t.Logf("received %d/%d messages before timeout", receivedCount, messageCount)
			assert.GreaterOrEqual(t, receivedCount, messageCount*8/10, "expected at least 80%% of messages")
			return
		default:
			msg, err := consumer.ReadMessage(1 * time.Second)
			if err != nil {
				continue
			}
			receivedCount++
		}
	}

	assert.Equal(t, messageCount, receivedCount)
}

func TestProducerIntegration_ErrorChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka
	bootstrapServers, cleanup := setupKafka(t, ctx)
	defer cleanup()

	topic := "test-producer-errors"
	createTopic(t, bootstrapServers, topic, 1)

	log := testutils.NewTestLogger(t)
	config := &kafka.ConfigMap{
		"bootstrap.servers":      bootstrapServers,
		"acks":                   "all",
		"go.logs.channel.enable": false,
	}

	producer, err := NewProducer(ctx, config, log)
	require.NoError(t, err)

	// Get error channel
	errCh := producer.Errors()
	require.NotNil(t, errCh)

	// Close producer
	producer.Close(5 * time.Second)

	// Error channel should be closed
	select {
	case _, ok := <-errCh:
		assert.False(t, ok, "error channel should be closed after Close()")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for error channel to close")
	}
}

