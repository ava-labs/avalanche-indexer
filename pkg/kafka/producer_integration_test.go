//go:build integration
// +build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap/zaptest"
)

const (
	kafkaImage   = "confluentinc/cp-kafka:7.5.0"
	testTopic    = "test-topic"
	testTimeout  = 30 * time.Second
	flushTimeout = 10 * time.Second
)

type kafkaContainer struct {
	container testcontainers.Container
	brokers   string
}

func setupKafka(t *testing.T) *kafkaContainer {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        kafkaImage,
		ExposedPorts: []string{"9093/tcp"},
		HostConfigModifier: func(hc *container.HostConfig) {
			// Bind container port 9093 to host port 9093 to match advertised listeners
			hc.PortBindings = map[nat.Port][]nat.PortBinding{
				"9093/tcp": {{HostIP: "127.0.0.1", HostPort: "9093"}},
			}
		},
		Env: map[string]string{
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://127.0.0.1:9093,BROKER://127.0.0.1:9092", // Use IPv4 explicitly to avoid IPv6 connection attempts
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@127.0.0.1:9094",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_LOG_FLUSH_INTERVAL_MESSAGES":              "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qk",
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(testTimeout),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	// Since we bound port 9093 to host, we can connect directly
	// Use IPv4 explicitly to avoid IPv6 connection attempts
	brokers := "127.0.0.1:9093"

	// Wait for Kafka broker to be fully ready to accept connections
	waitForKafkaBroker(t, brokers)

	// Additional stabilization time for broker to be fully ready
	time.Sleep(2 * time.Second)

	createTestTopic(t, brokers)

	return &kafkaContainer{
		container: container,
		brokers:   brokers,
	}
}

// waitForKafkaBroker polls Kafka until the broker is ready to accept connections.
// This ensures the broker is fully initialized before running tests.
func waitForKafkaBroker(t *testing.T, brokers string) {
	maxRetries := 30
	retryDelay := time.Second

	for i := 0; i < maxRetries; i++ {
		adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
			"bootstrap.servers":                  brokers,
			"socket.connection.setup.timeout.ms": 10000, // 10s timeout for initial connection
			"socket.timeout.ms":                  10000, // 10s socket timeout
		})
		if err != nil {
			t.Logf("Attempt %d/%d: Failed to create admin client: %v", i+1, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		// Try to get metadata - this verifies the broker is responsive
		metadata, err := adminClient.GetMetadata(nil, false, 5000)
		adminClient.Close()

		if err != nil {
			t.Logf("Attempt %d/%d: Failed to get metadata: %v", i+1, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		// Check if we have at least one broker
		if len(metadata.Brokers) > 0 {
			t.Logf("Kafka broker is ready! Found %d broker(s)", len(metadata.Brokers))
			return
		}

		t.Logf("Attempt %d/%d: No brokers found in metadata", i+1, maxRetries)
		time.Sleep(retryDelay)
	}

	require.FailNow(t, "Kafka broker did not become ready within timeout")
}

func createTestTopic(t *testing.T, brokers string) {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	require.NoError(t, err)
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topics := []kafka.TopicSpecification{
		{
			Topic:             testTopic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	results, err := adminClient.CreateTopics(ctx, topics)
	require.NoError(t, err)

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			t.Fatalf("failed to create topic %s: %v", result.Topic, result.Error)
		}
	}

	time.Sleep(1 * time.Second)
}

func (kc *kafkaContainer) teardown(t *testing.T) {
	ctx := context.Background()
	if kc.container != nil {
		err := kc.container.Terminate(ctx)
		if err != nil {
			t.Logf("failed to terminate Kafka container: %v", err)
		}
	}
}

// TestHandleDeliveryEvent_Success tests successful delivery event handling
func TestHandleDeliveryEvent_Success(t *testing.T) {
	log := zaptest.NewLogger(t).Sugar()

	topic := "test-topic"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
		},
	}

	event := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    kafka.Offset(123),
			Error:     nil,
		},
	}

	err := handleDeliveryEvent(log, msg, event)
	assert.NoError(t, err)
}

// TestHandleDeliveryEvent_Error tests delivery event with error
func TestHandleDeliveryEvent_Error(t *testing.T) {
	log := zaptest.NewLogger(t).Sugar()

	topic := "test-topic"
	deliveryErr := errors.New("delivery failed")

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
	}

	event := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Error:     deliveryErr,
		},
	}

	err := handleDeliveryEvent(log, msg, event)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "delivery failed")
}

// TestHandleDeliveryEvent_UnexpectedEventType tests handling of unexpected event types
func TestHandleDeliveryEvent_UnexpectedEventType(t *testing.T) {
	log := zaptest.NewLogger(t).Sugar()

	topic := "test-topic"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
	}

	event := kafka.Error{}

	err := handleDeliveryEvent(log, msg, event)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unexpected delivery event")
}

// TestProducer_NewProducer tests producer creation
func TestProducer_NewProducer(t *testing.T) {
	kc := setupKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()

	t.Run("successful creation", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers": kc.brokers,
			"client.id":         "test-producer",
		}

		producer, err := NewProducer(ctx, conf, log)
		require.NoError(t, err)
		require.NotNil(t, producer)
		assert.NotNil(t, producer.Errors())

		// Give the producer time to start the background goroutines
		time.Sleep(5 * time.Second)

		select {
		case _, ok := <-producer.logsDone:
			require.True(t, !ok, "logsDone should be closed when logs are disabled or not configured")
		case <-time.After(100 * time.Millisecond):
			require.FailNow(t, "logsDone should be closed when logs are disabled or not configured")
		}

		select {
		case <-producer.eventsDone:
			// Monitor exited - check for error
			select {
			case fatalErr := <-producer.Errors():
				t.Logf("Monitor goroutine exited due to error: %v", fatalErr)
				// This is expected when Kafka broker is unavailable
			default:
				t.Fatal("Monitor exited but no error was sent")
			}
		case <-time.After(100 * time.Millisecond):
			require.True(t, true, "eventsDone should be open after producer creation, indicating that the monitor goroutine is running")
		}

		producer.Close(flushTimeout)
		time.Sleep(flushTimeout)

		select {
		case _, ok := <-producer.eventsDone:
			require.True(t, !ok, "eventsDone should be closed when producer is closed")
		case <-time.After(100 * time.Millisecond):
			require.FailNow(t, "eventsDone should be closed when producer is closed")

		}
	})

	t.Run("creation with invalid config", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"invalid.config": "value",
		}

		producer, err := NewProducer(ctx, conf, log)
		assert.Error(t, err)
		assert.Nil(t, producer)
	})

	t.Run("creation with logs value", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"go.logs.channel.enable": "invalid",
			"bootstrap.servers":      kc.brokers,
			"client.id":              "test-producer",
		}

		producer, err := NewProducer(ctx, conf, log)
		assert.EqualError(
			t,
			err,
			"failed to get go.logs.channel.enable: go.logs.channel.enable expects type bool, not string",
		)
		assert.Nil(t, producer)
	})

	t.Run("creation with logs enabled", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers":      kc.brokers,
			"go.logs.channel.enable": true,
		}

		producer, err := NewProducer(ctx, conf, log)
		require.NoError(t, err)
		require.NotNil(t, producer)

		select {
		case <-producer.logsDone:
			require.FailNow(t, "logsDone should not be closed before Close() is called")
		case <-time.After(100 * time.Millisecond):
			require.True(t, true, "logsDone channel should not be closed before Close() is called when logs are enabled")
		}

		producer.Close(flushTimeout)
		time.Sleep(flushTimeout)
		select {
		case _, ok := <-producer.logsDone:
			require.True(t, !ok, "logsDone channel closed successfully after Close() when logs are enabled")
		case <-time.After(1 * time.Second):
			require.FailNow(t, "logsDone should be closed after Close() is called when logs are enabled")
		}
	})
}

// TestProducer_Produce tests message production
func TestProducer_Produce(t *testing.T) {
	kc := setupKafka(t)
	defer kc.teardown(t)

	bgCtx := context.Background()
	log := zaptest.NewLogger(t).Sugar()

	t.Run("successful produce", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers": kc.brokers,
			"client.id":         "test-producer",
			"acks":              "all",
		}

		producer, err := NewProducer(bgCtx, conf, log)
		require.NoError(t, err)
		defer producer.Close(flushTimeout)

		msg := Msg{
			Topic: testTopic,
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err = producer.Produce(bgCtx, msg)
		assert.NoError(t, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers": kc.brokers,
			"client.id":         "test-producer",
		}

		producer, err := NewProducer(bgCtx, conf, log)
		require.NoError(t, err)
		defer producer.Close(flushTimeout)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		msg := Msg{
			Topic: testTopic,
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err = producer.Produce(ctx, msg)
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("context timeout", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers": kc.brokers,
			"client.id":         "test-producer",
		}

		producer, err := NewProducer(bgCtx, conf, log)
		require.NoError(t, err)
		defer producer.Close(flushTimeout)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		time.Sleep(1 * time.Millisecond)

		msg := Msg{
			Topic: testTopic,
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err = producer.Produce(ctx, msg)
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("concurrent production", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers": kc.brokers,
			"client.id":         "test-producer",
			"acks":              "all",
		}

		producer, err := NewProducer(bgCtx, conf, log)
		require.NoError(t, err)
		defer producer.Close(flushTimeout)

		numMessages := 50
		var wg sync.WaitGroup
		errCh := make(chan error, numMessages)

		for i := 0; i < numMessages; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				msg := Msg{
					Topic: testTopic,
					Key:   []byte(fmt.Sprintf("key-%d", idx)),
					Value: []byte(fmt.Sprintf("value-%d", idx)),
				}
				if err := producer.Produce(bgCtx, msg); err != nil {
					errCh <- err
				}
			}(i)
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			t.Errorf("concurrent produce failed: %v", err)
		}
	})
}

// TestProducer_Close tests producer close behavior
func TestProducer_Close(t *testing.T) {
	kc := setupKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()

	t.Run("close after produce", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers": kc.brokers,
			"client.id":         "test-producer",
		}

		producer, err := NewProducer(ctx, conf, log)
		require.NoError(t, err)

		msg := Msg{
			Topic: testTopic,
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		err = producer.Produce(ctx, msg)
		require.NoError(t, err)

		producer.Close(flushTimeout)

		time.Sleep(flushTimeout)

		select {
		case _, ok := <-producer.eventsDone:
			require.True(t, !ok, "eventsDone should be closed when producer is closed")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("eventsDone should be closed when producer is closed")
		}
		select {
		case _, ok := <-producer.logsDone:
			require.True(t, !ok, "logsDone should be closed when producer is closed")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("logsDone should be closed when producer is closed")
		}

		// Error channel may send one error before closing, or close immediately
		select {
		case err, ok := <-producer.Errors():
			if ok {
				// Received an error, verify channel closes after
				t.Logf("Received error from Errors() channel: %v", err)
				select {
				case _, ok2 := <-producer.Errors():
					require.False(t, ok2, "Errors() channel should be closed after sending error")
				case <-time.After(100 * time.Millisecond):
					t.Fatal("Errors() channel should close after sending error")
				}
			}
			// If ok=false, channel closed directly (expected)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Errors() channel should be readable or closed")
		}

		require.True(t, producer.producer.IsClosed(), "producer should be closed")
	})

	t.Run("close is idempotent", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers": kc.brokers,
			"client.id":         "test-producer",
		}

		producer, err := NewProducer(ctx, conf, log)
		require.NoError(t, err)

		producer.Close(flushTimeout)
		producer.Close(flushTimeout)
		producer.Close(flushTimeout)
	})
}

// TestProducer_Errors tests error channel behavior
func TestProducer_Errors(t *testing.T) {
	kc := setupKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()

	conf := &kafka.ConfigMap{
		"bootstrap.servers": kc.brokers,
		"client.id":         "test-producer",
	}

	producer, err := NewProducer(ctx, conf, log)
	require.NoError(t, err)

	errCh := producer.Errors()
	require.NotNil(t, errCh)

	producer.Close(flushTimeout)
	time.Sleep(flushTimeout)
	select {
	case _, ok := <-errCh:
		assert.False(t, ok, "error channel should be closed")
	case <-time.After(flushTimeout + 2*time.Second):
		t.Fatal("error channel should close after producer close")
	}
}

// TestProducer_StatsEvents tests that stats events are received and handled
func TestProducer_StatsEvents(t *testing.T) {
	kc := setupKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()

	conf := &kafka.ConfigMap{
		"bootstrap.servers":      kc.brokers,
		"client.id":              "test-producer",
		"statistics.interval.ms": 1000, // Emit stats every 1 second
	}

	producer, err := NewProducer(ctx, conf, log)
	require.NoError(t, err)
	defer producer.Close(flushTimeout)

	// Produce a message to ensure connection is active
	msg := Msg{
		Topic: testTopic,
		Key:   []byte("stats-test"),
		Value: []byte("test"),
	}

	err = producer.Produce(ctx, msg)
	require.NoError(t, err)

	// Wait for stats event (should come within ~1 second after first activity)
	// The stats event will be logged by monitorProducerEvents as "kafka stats event received"
	time.Sleep(2 * time.Second)

	// If we get here without the producer crashing, stats events were handled successfully
	// We can't directly assert the log output, but the test would fail if stats caused a panic
	t.Log("Stats events handled successfully (check logs for 'kafka stats event received')")
}

// TestProducer_ProduceErrors tests error handling in produceWithRetry
func TestProducer_ProduceErrors(t *testing.T) {
	kc := setupKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()

	t.Run("invalid message size", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers": kc.brokers,
			"client.id":         "test-producer",
			"message.max.bytes": 1000, // Kafka minimum is 1000 bytes
		}

		producer, err := NewProducer(ctx, conf, log)
		require.NoError(t, err)
		defer producer.Close(flushTimeout)

		// Try to produce a message larger than max.bytes
		largeMsg := Msg{
			Topic: testTopic,
			Key:   []byte("key"),
			Value: make([]byte, 2000), // Larger than message.max.bytes
		}

		err = producer.Produce(ctx, largeMsg)
		require.Error(t, err)
		// Kafka returns "Message size too large" for ErrMsgSizeTooLarge
		assert.ErrorContains(t, err, "Message size too large")
	})

	t.Run("context cancellation", func(t *testing.T) {
		conf := &kafka.ConfigMap{
			"bootstrap.servers": kc.brokers,
			"client.id":         "test-producer",
		}

		producer, err := NewProducer(ctx, conf, log)
		require.NoError(t, err)
		defer producer.Close(flushTimeout)

		// Create a context that's already cancelled
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		msg := Msg{
			Topic: testTopic,
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		err = producer.Produce(cancelledCtx, msg)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestProducer_BackgroundGoroutines tests goroutine lifecycle
func TestProducer_BackgroundGoroutines(t *testing.T) {
	kc := setupKafka(t)
	defer kc.teardown(t)

	t.Run("goroutines stop on Close", func(t *testing.T) {
		ctx := context.Background()
		log := zaptest.NewLogger(t).Sugar()

		conf := &kafka.ConfigMap{
			"bootstrap.servers":      kc.brokers,
			"client.id":              "test-producer",
			"go.logs.channel.enable": true,
		}

		producer, err := NewProducer(ctx, conf, log)
		require.NoError(t, err)

		done := make(chan bool)
		go func() {
			producer.Close(flushTimeout)
			done <- true
		}()

		select {
		case <-done:
			require.True(t, true, "Close should complete within flush timeout + buffer")
		case <-time.After(flushTimeout + 5*time.Second):
			t.Fatal("Close should complete within flush timeout + buffer")
		}
	})
}
