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
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://localhost:9093,BROKER://localhost:9092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9094",
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
	brokers := "localhost:9093"

	// Give Kafka extra time to fully start and stabilize
	time.Sleep(10 * time.Second)

	createTestTopic(t, brokers)

	return &kafkaContainer{
		container: container,
		brokers:   brokers,
	}
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
		time.Sleep(100 * time.Millisecond)

		select {
		case _, ok := <-producer.logsDone:
			require.True(t, !ok, "logsDone should be closed when logs are disabled or not configured")
		case <-time.After(100 * time.Millisecond):
			require.FailNow(t, "logsDone should be closed when logs are disabled or not configured")
		}

		select {
		case _, ok := <-producer.eventsDone:
			require.True(t, ok,
				"eventsDone should not be closed after producer creation; monitor goroutine should be running")
		case <-time.After(100 * time.Millisecond):
			require.True(t, true, "eventsDone  should be open after producer creation, indicating that the monitor goroutine is running")
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

		select {
		case _, ok := <-producer.Errors():
			require.True(t, !ok, "Errors() should be closed when producer is closed")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Errors() should be closed when producer is closed")
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

	select {
	case _, ok := <-errCh:
		assert.False(t, ok, "error channel should be closed")
	case <-time.After(flushTimeout + 2*time.Second):
		t.Fatal("error channel should close after producer close")
	}
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
