//go:build integration
// +build integration

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap/zaptest"
)

const (
	adminTestTimeout = 30 * time.Second
)

type adminKafkaContainer struct {
	container testcontainers.Container
	brokers   string
}

func setupAdminKafka(t *testing.T) *adminKafkaContainer {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.5.0",
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
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false", // Disable auto topic creation
			"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qk",
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(adminTestTimeout),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	// Since we bound port 9093 to host, we can connect directly
	brokers := "localhost:9093"

	// Give Kafka extra time to fully start and stabilize
	time.Sleep(3 * time.Second)

	return &adminKafkaContainer{
		container: container,
		brokers:   brokers,
	}
}

func (kc *adminKafkaContainer) teardown(t *testing.T) {
	ctx := context.Background()
	if kc.container != nil {
		err := kc.container.Terminate(ctx)
		if err != nil {
			t.Logf("failed to terminate Kafka container: %v", err)
		}
	}
}

func createAdminClient(t *testing.T, brokers string) *cKafka.AdminClient {
	admin, err := cKafka.NewAdminClient(&cKafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	require.NoError(t, err)
	require.NotNil(t, admin)
	return admin
}

func TestTopicConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  TopicConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: TopicConfig{
				Name:              "test-topic",
				NumPartitions:     3,
				ReplicationFactor: 1,
			},
			wantErr: false,
		},
		{
			name: "empty topic name",
			config: TopicConfig{
				Name:              "",
				NumPartitions:     3,
				ReplicationFactor: 1,
			},
			wantErr: true,
			errMsg:  "topic name cannot be empty",
		},
		{
			name: "zero partitions",
			config: TopicConfig{
				Name:              "test-topic",
				NumPartitions:     0,
				ReplicationFactor: 1,
			},
			wantErr: true,
			errMsg:  "number of partitions must be > 0",
		},
		{
			name: "negative partitions",
			config: TopicConfig{
				Name:              "test-topic",
				NumPartitions:     -1,
				ReplicationFactor: 1,
			},
			wantErr: true,
			errMsg:  "number of partitions must be > 0",
		},
		{
			name: "zero replication factor",
			config: TopicConfig{
				Name:              "test-topic",
				NumPartitions:     3,
				ReplicationFactor: 0,
			},
			wantErr: true,
			errMsg:  "replication factor must be > 0",
		},
		{
			name: "negative replication factor",
			config: TopicConfig{
				Name:              "test-topic",
				NumPartitions:     3,
				ReplicationFactor: -1,
			},
			wantErr: true,
			errMsg:  "replication factor must be > 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTopicMetadata(t *testing.T) {
	kc := setupAdminKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()
	admin := createAdminClient(t, kc.brokers)
	defer admin.Close()

	t.Run("non-existent topic returns nil metadata", func(t *testing.T) {
		metadata, err := TopicMetadata(admin, "non-existent-topic")
		require.NoError(t, err)
		assert.Nil(t, metadata)
	})

	t.Run("existing topic returns metadata", func(t *testing.T) {
		config := TopicConfig{
			Name:              "test-exists",
			NumPartitions:     2,
			ReplicationFactor: 1,
		}

		// Create the topic first
		err := CreateTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Give Kafka time to create the topic
		time.Sleep(500 * time.Millisecond)

		// Check it exists
		metadata, err := TopicMetadata(admin, "test-exists")
		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Equal(t, "test-exists", metadata.Topic)
		assert.Len(t, metadata.Partitions, 2)
	})
}

func TestCreateTopic(t *testing.T) {
	kc := setupAdminKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()
	admin := createAdminClient(t, kc.brokers)
	defer admin.Close()

	t.Run("creates topic successfully", func(t *testing.T) {
		config := TopicConfig{
			Name:              "test-create-new",
			NumPartitions:     3,
			ReplicationFactor: 1,
		}

		err := CreateTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Verify topic was created
		metadata, err := TopicMetadata(admin, "test-create-new")
		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Len(t, metadata.Partitions, 3)
	})

	t.Run("creating existing topic returns ErrTopicAlreadyExists", func(t *testing.T) {
		config := TopicConfig{
			Name:              "test-create-existing",
			NumPartitions:     1,
			ReplicationFactor: 1,
		}

		// Create first time
		err := CreateTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Create again - should return ErrTopicAlreadyExists
		err = CreateTopic(ctx, admin, config, log)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTopicAlreadyExists, "should return ErrTopicAlreadyExists sentinel error")
	})

	t.Run("invalid config returns validation error", func(t *testing.T) {
		config := TopicConfig{
			Name:              "",
			NumPartitions:     1,
			ReplicationFactor: 1,
		}

		err := CreateTopic(ctx, admin, config, log)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid topic config")
	})
}

func TestEnsureTopic(t *testing.T) {
	kc := setupAdminKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()
	admin := createAdminClient(t, kc.brokers)
	defer admin.Close()

	t.Run("creates new topic", func(t *testing.T) {
		config := TopicConfig{
			Name:              "test-ensure-new",
			NumPartitions:     2,
			ReplicationFactor: 1,
		}

		err := EnsureTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Verify topic exists
		metadata, err := TopicMetadata(admin, "test-ensure-new")
		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Len(t, metadata.Partitions, 2)
	})

	t.Run("is idempotent for existing topic with same config", func(t *testing.T) {
		config := TopicConfig{
			Name:              "test-ensure-same",
			NumPartitions:     1,
			ReplicationFactor: 1,
		}

		// Create first time
		err := EnsureTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Call again with same config
		err = EnsureTopic(ctx, admin, config, log)
		assert.NoError(t, err)
	})

	t.Run("increases partitions when requested", func(t *testing.T) {
		// Create topic with 2 partitions
		config := TopicConfig{
			Name:              "test-increase-partitions",
			NumPartitions:     2,
			ReplicationFactor: 1,
		}

		err := EnsureTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Verify initial partition count
		metadata, err := TopicMetadata(admin, "test-increase-partitions")
		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Len(t, metadata.Partitions, 2)

		// Request more partitions
		config.NumPartitions = 5
		err = EnsureTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Give Kafka time to increase partitions
		time.Sleep(500 * time.Millisecond)

		// Verify partition count increased
		metadata, err = TopicMetadata(admin, "test-increase-partitions")
		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Len(t, metadata.Partitions, 5)
	})

	t.Run("returns ErrCannotDecreasePartitions when topic has more partitions", func(t *testing.T) {
		// Create topic with 5 partitions
		config := TopicConfig{
			Name:              "test-more-partitions",
			NumPartitions:     5,
			ReplicationFactor: 1,
		}

		err := EnsureTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Request fewer partitions - should return error
		config.NumPartitions = 2
		err = EnsureTopic(ctx, admin, config, log)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCannotDecreasePartitions, "should return ErrCannotDecreasePartitions sentinel error")

		// Verify partition count unchanged (cannot decrease)
		metadata, err := TopicMetadata(admin, "test-more-partitions")
		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Len(t, metadata.Partitions, 5)
	})

	t.Run("invalid config returns validation error", func(t *testing.T) {
		config := TopicConfig{
			Name:              "test-topic",
			NumPartitions:     0,
			ReplicationFactor: 1,
		}

		err := EnsureTopic(ctx, admin, config, log)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid topic config")
	})
}

func TestIncreasePartitions(t *testing.T) {
	kc := setupAdminKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()
	admin := createAdminClient(t, kc.brokers)
	defer admin.Close()

	t.Run("increases partitions successfully", func(t *testing.T) {
		// Create topic with 1 partition
		config := TopicConfig{
			Name:              "test-increase",
			NumPartitions:     1,
			ReplicationFactor: 1,
		}

		err := CreateTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Increase to 3 partitions
		err = increasePartitions(ctx, admin, "test-increase", 3, log)
		require.NoError(t, err)

		// Give Kafka time to process
		time.Sleep(500 * time.Millisecond)

		// Verify partition count
		metadata, err := TopicMetadata(admin, "test-increase")
		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Len(t, metadata.Partitions, 3)
	})

	t.Run("fails for non-existent topic", func(t *testing.T) {
		err := increasePartitions(ctx, admin, "non-existent-topic", 5, log)
		assert.Error(t, err)
	})
}

func TestGetReplicationFactor(t *testing.T) {
	kc := setupAdminKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()
	admin := createAdminClient(t, kc.brokers)
	defer admin.Close()

	t.Run("returns correct replication factor", func(t *testing.T) {
		config := TopicConfig{
			Name:              "test-rf",
			NumPartitions:     2,
			ReplicationFactor: 1,
		}

		err := CreateTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Give Kafka time to create
		time.Sleep(500 * time.Millisecond)

		metadata, err := TopicMetadata(admin, "test-rf")
		require.NoError(t, err)
		require.NotNil(t, metadata)

		rf := getReplicationFactor(metadata)
		assert.Equal(t, 1, rf)
	})

	t.Run("returns 0 for empty partitions", func(t *testing.T) {
		metadata := &cKafka.TopicMetadata{
			Topic:      "empty",
			Partitions: []cKafka.PartitionMetadata{},
		}

		rf := getReplicationFactor(metadata)
		assert.Equal(t, 0, rf)
	})
}

func TestConcurrentTopicOperations(t *testing.T) {
	kc := setupAdminKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()
	admin := createAdminClient(t, kc.brokers)
	defer admin.Close()

	t.Run("concurrent topic creation", func(t *testing.T) {
		const numTopics = 10

		// Create topics concurrently
		errChan := make(chan error, numTopics)
		for i := 0; i < numTopics; i++ {
			go func(idx int) {
				config := TopicConfig{
					Name:              fmt.Sprintf("test-concurrent-%d", idx),
					NumPartitions:     1,
					ReplicationFactor: 1,
				}
				errChan <- CreateTopic(ctx, admin, config, log)
			}(i)
		}

		// Collect errors
		for i := 0; i < numTopics; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}

		// Verify all topics exist
		for i := 0; i < numTopics; i++ {
			topicName := fmt.Sprintf("test-concurrent-%d", i)
			metadata, err := TopicMetadata(admin, topicName)
			require.NoError(t, err)
			assert.NotNil(t, metadata, "topic %s should exist", topicName)
		}
	})
}

func TestReplicationFactorMismatch(t *testing.T) {
	kc := setupAdminKafka(t)
	defer kc.teardown(t)

	ctx := context.Background()
	log := zaptest.NewLogger(t).Sugar()
	admin := createAdminClient(t, kc.brokers)
	defer admin.Close()

	t.Run("returns ErrReplicationFactorMismatch when RF differs", func(t *testing.T) {
		// Create topic with RF=1
		config := TopicConfig{
			Name:              "test-rf-diff",
			NumPartitions:     1,
			ReplicationFactor: 1,
		}

		err := EnsureTopic(ctx, admin, config, log)
		require.NoError(t, err)

		// Try to "change" RF to 3 - should return error
		config.ReplicationFactor = 3
		err = EnsureTopic(ctx, admin, config, log)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrReplicationFactorMismatch, "should return ErrReplicationFactorMismatch sentinel error")

		// Verify RF is still 1 (unchanged)
		metadata, err := TopicMetadata(admin, "test-rf-diff")
		require.NoError(t, err)
		require.NotNil(t, metadata)
		rf := getReplicationFactor(metadata)
		assert.Equal(t, 1, rf, "replication factor should remain unchanged")
	})
}
