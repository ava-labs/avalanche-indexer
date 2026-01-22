package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

const (
	// metadataTimeout is the timeout for Kafka metadata operations.
	metadataTimeout = 10 * time.Second
)

// TopicConfig holds Kafka topic configuration options for creation or validation.
type TopicConfig struct {
	Name              string // Required: topic name
	NumPartitions     int    // Required: number of partitions (must be > 0)
	ReplicationFactor int    // Required: replication factor (must be > 0)
}

// Validate checks if the TopicConfig is valid for topic creation.
func (tc TopicConfig) Validate() error {
	if tc.Name == "" {
		return errors.New("topic name cannot be empty")
	}
	if tc.NumPartitions <= 0 {
		return fmt.Errorf("number of partitions must be > 0, got %d", tc.NumPartitions)
	}
	if tc.ReplicationFactor <= 0 {
		return fmt.Errorf("replication factor must be > 0, got %d", tc.ReplicationFactor)
	}
	return nil
}

// TopicExists checks if a Kafka topic exists and returns its metadata if found.
//
// Returns:
//   - metadata: Topic metadata if the topic exists, nil if it doesn't exist
//   - error: Non-nil if there was an error checking topic existence (network, permission, etc.)
func TopicExists(admin *kafka.AdminClient, topicName string) (*kafka.TopicMetadata, error) {
	metadata, err := admin.GetMetadata(&topicName, false, int(metadataTimeout.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for topic %q: %w", topicName, err)
	}

	topicMetadata, exists := metadata.Topics[topicName]
	if !exists || topicMetadata.Error.Code() == kafka.ErrUnknownTopicOrPart {
		// Topic doesn't exist - this is not an error condition
		return nil, nil
	}

	if topicMetadata.Error.Code() != kafka.ErrNoError {
		return nil, fmt.Errorf("topic %q has error: %w", topicName, topicMetadata.Error)
	}

	return &topicMetadata, nil
}

// CreateTopic creates a new Kafka topic with the given configuration.
//
// This function does NOT check if the topic already exists - use CreateTopicIfNotExists
// if you need idempotent behavior. Returns ErrTopicAlreadyExists error if the topic exists.
//
// The configuration is validated before topic creation. Returns an error if validation fails.
func CreateTopic(
	ctx context.Context,
	admin *kafka.AdminClient,
	config TopicConfig,
	log *zap.SugaredLogger,
) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid topic config: %w", err)
	}

	spec := kafka.TopicSpecification{
		Topic:             config.Name,
		NumPartitions:     config.NumPartitions,
		ReplicationFactor: config.ReplicationFactor,
	}

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{spec})
	if err != nil {
		return fmt.Errorf("failed to create topic %q: %w", config.Name, err)
	}

	// Check result - should only have one result since we created one topic
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("failed to create topic %q: %w", result.Topic, result.Error)
		}

		if result.Error.Code() == kafka.ErrTopicAlreadyExists {
			log.Infow("topic already exists",
				"topic", result.Topic,
				"partitions", config.NumPartitions,
				"replicationFactor", config.ReplicationFactor)
		} else {
			log.Infow("created topic",
				"topic", result.Topic,
				"partitions", config.NumPartitions,
				"replicationFactor", config.ReplicationFactor)
		}
	}

	return nil
}

// EnsureTopic ensures a Kafka topic exists with the desired configuration.
//
// Behavior:
//   - If topic doesn't exist: Creates it with the specified configuration
//   - If topic exists with fewer partitions: Increases partition count (Kafka allows this)
//   - If topic exists with more partitions: Logs warning but continues (cannot decrease)
//   - If replication factor differs: Logs warning (cannot be changed automatically)
//
// This is the recommended function for production use as it handles common scenarios
// gracefully. For strict validation, check the topic metadata after this call.
//
// Note: Kafka does not support decreasing partitions or changing replication factor
// through the admin API. These require manual intervention.
func EnsureTopic(
	ctx context.Context,
	admin *kafka.AdminClient,
	config TopicConfig,
	log *zap.SugaredLogger,
) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid topic config: %w", err)
	}

	topicMetadata, err := TopicExists(admin, config.Name)
	if err != nil {
		return fmt.Errorf("failed to check topic existence: %w", err)
	}

	if topicMetadata == nil {
		return CreateTopic(ctx, admin, config, log)
	}

	currentPartitions := len(topicMetadata.Partitions)
	currentRF := getReplicationFactor(topicMetadata)

	log.Infow("topic exists",
		"topic", config.Name,
		"currentPartitions", currentPartitions,
		"currentReplicationFactor", currentRF)

	if currentRF != config.ReplicationFactor {
		log.Warnw("topic replication factor differs from config",
			"topic", config.Name,
			"current", currentRF,
			"desired", config.ReplicationFactor,
			"note", "replication factor cannot be changed automatically - requires manual partition reassignment")
	}

	switch {
	case currentPartitions < config.NumPartitions:
		log.Infow("increasing topic partitions",
			"topic", config.Name,
			"from", currentPartitions,
			"to", config.NumPartitions)
		return increasePartitions(ctx, admin, config.Name, config.NumPartitions, log)

	case currentPartitions > config.NumPartitions:
		log.Warnw("topic has more partitions than configured",
			"topic", config.Name,
			"current", currentPartitions,
			"desired", config.NumPartitions,
			"note", "Kafka does not support decreasing partition count - current count will be retained")
		return errors.New("topic has more partitions than configured")

	default:
		return nil
	}
}

// increasePartitions increases the partition count for an existing topic.
// This is an internal helper function. Partitions can only be increased, never decreased.
func increasePartitions(
	ctx context.Context,
	admin *kafka.AdminClient,
	topicName string,
	newPartitionCount int,
	log *zap.SugaredLogger,
) error {
	partitionSpec := []kafka.PartitionsSpecification{
		{
			Topic:      topicName,
			IncreaseTo: newPartitionCount,
		},
	}

	results, err := admin.CreatePartitions(ctx, partitionSpec)
	if err != nil {
		return fmt.Errorf("failed to increase partitions for topic %q: %w", topicName, err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("failed to increase partitions for topic %q: %w", result.Topic, result.Error)
		}
		log.Infow("increased partitions",
			"topic", result.Topic,
			"newPartitionCount", newPartitionCount)
	}

	return nil
}

// getReplicationFactor extracts the replication factor from topic metadata.
// Returns 0 if the topic has no partitions (shouldn't happen in practice).
func getReplicationFactor(metadata *kafka.TopicMetadata) int {
	if len(metadata.Partitions) == 0 {
		return 0
	}
	return len(metadata.Partitions[0].Replicas)
}
