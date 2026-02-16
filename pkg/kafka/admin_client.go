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

var (
	ErrTopicAlreadyExists        = errors.New("topic already exists")
	ErrCannotDecreasePartitions  = errors.New("cannot decrease partitions count")
	ErrReplicationFactorMismatch = errors.New("replication factor mismatch")
)

// TopicConfig holds Kafka topic configuration options for creation or validation.
type TopicConfig struct {
	Name              string            // Required: topic name
	NumPartitions     int               // Required: number of partitions (must be > 0)
	ReplicationFactor int               // Required: replication factor (must be > 0)
	Config            map[string]string // Optional: topic configuration
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

// TopicMetadata checks if a Kafka topic exists and returns its metadata if found.
//
// This function uses ListTopics + GetMetadata to avoid triggering auto-creation.
// When auto.create.topics.enable=false, GetMetadata alone would fail for non-existent topics.
//
// Returns:
//   - metadata: Topic metadata if the topic exists, nil if it doesn't exist
//   - error: Non-nil if there was an error checking topic existence (network, permission, etc.)
func TopicMetadata(admin *kafka.AdminClient, topicName string) (*kafka.TopicMetadata, error) {
	// First, list all topics to check existence without triggering auto-creation
	metadata, err := admin.GetMetadata(nil, false, int(metadataTimeout.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	// Check if topic exists in the list
	topicMetadata, exists := metadata.Topics[topicName]
	if !exists || topicMetadata.Error.Code() == kafka.ErrUnknownTopicOrPart {
		// Topic doesn't exist - this is not an error condition
		return nil, nil
	}

	if topicMetadata.Error.Code() != kafka.ErrNoError {
		return nil, fmt.Errorf("topic metadata for topic %q has error: %w", topicName, topicMetadata.Error)
	}

	return &topicMetadata, nil
}

// CreateTopic creates a new Kafka topic with the given configuration.
//
// If the topic already exists, returns ErrTopicAlreadyExists. Use EnsureTopic if you need
// idempotent behavior that handles existing topics gracefully.
//
// The configuration is validated before topic creation. Returns an error if validation fails.
// If TopicConfig.Config is provided, those configurations will be applied to the new topic.
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
		Config:            config.Config,
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
			log.Errorw("topic already exists",
				"topic", result.Topic,
				"partitions", config.NumPartitions,
				"replicationFactor", config.ReplicationFactor)
			return ErrTopicAlreadyExists
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
// This is the recommended function for production use. It provides idempotent topic
// management with automatic partition scaling, config updates, and strict validation.
//
// Behavior:
//   - Topic doesn't exist: Creates it with the specified configuration
//   - Topic has fewer partitions: Increases partition count automatically
//   - Topic has more partitions: Returns ErrCannotDecreasePartitions
//   - Replication factor differs: Returns ErrReplicationFactorMismatch
//   - Topic configs differ: Updates configs automatically (if TopicConfig.Config is provided)
//   - Configuration matches: Returns nil (idempotent)
//
// Error Handling:
//   - Returns ErrCannotDecreasePartitions if existing partitions > desired (Kafka limitation)
//   - Returns ErrReplicationFactorMismatch if replication factor differs (cannot be changed)
//   - Returns wrapped error for validation failures or Kafka admin API errors
//
// Important:
//   - TopicMetadata uses GetMetadata(nil, ...) to list all topics, avoiding auto-creation.
//   - This allows auto.create.topics.enable=true for internal topics (__consumer_offsets)
//     while preventing accidental auto-creation of user topics with default settings.
//   - Decreasing partitions requires manual intervention (delete and recreate topic).
//   - Changing replication factor requires manual partition reassignment.
//   - Topic configs can be updated dynamically if they differ from desired state.
func EnsureTopic(
	ctx context.Context,
	admin *kafka.AdminClient,
	config TopicConfig,
	log *zap.SugaredLogger,
) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid topic config: %w", err)
	}

	topicMetadata, err := TopicMetadata(admin, config.Name)
	if err != nil {
		return fmt.Errorf("failed to check topic existence: %w", err)
	}

	if topicMetadata == nil {
		return CreateTopic(ctx, admin, config, log)
	}

	return ensureTopicConfiguration(ctx, admin, topicMetadata, log, config)
}

// ensureTopicConfiguration validates and adjusts topic configuration.
//
// Behavior:
//   - If topic has fewer partitions: Increases partition count automatically
//   - If topic has more partitions: Returns ErrCannotDecreasePartitions
//   - If replication factor differs: Returns ErrReplicationFactorMismatch
//   - If topic configs differ: Updates configs automatically
//
// This is an internal helper function used by EnsureTopic.
func ensureTopicConfiguration(ctx context.Context, admin *kafka.AdminClient, topicMetadata *kafka.TopicMetadata, log *zap.SugaredLogger, config TopicConfig) error {
	currentPartitions := len(topicMetadata.Partitions)
	currentRF := getReplicationFactor(topicMetadata)

	log.Infow("topic exists",
		"topic", config.Name,
		"currentPartitions", currentPartitions,
		"currentReplicationFactor", currentRF)

	if currentRF != config.ReplicationFactor {
		log.Errorw("topic replication factor differs from config",
			"topic", config.Name,
			"current", currentRF,
			"desired", config.ReplicationFactor,
			"note", "replication factor cannot be changed automatically")
		return ErrReplicationFactorMismatch
	}

	// Handle partition count changes
	var partitionErr error
	switch {
	case currentPartitions < config.NumPartitions:
		log.Infow("increasing topic partitions",
			"topic", config.Name,
			"from", currentPartitions,
			"to", config.NumPartitions)
		partitionErr = increasePartitions(ctx, admin, config.Name, config.NumPartitions, log)

	case currentPartitions > config.NumPartitions:
		log.Errorw("topic has more partitions than configured",
			"topic", config.Name,
			"current", currentPartitions,
			"desired", config.NumPartitions,
			"note", "Kafka does not support decreasing partition count - current count will be retained")
		partitionErr = ErrCannotDecreasePartitions
	}

	if partitionErr != nil {
		return partitionErr
	}

	// Handle topic config changes (if any configs are specified)
	if len(config.Config) > 0 {
		if err := ensureTopicConfig(ctx, admin, config.Name, config.Config, log); err != nil {
			return fmt.Errorf("failed to ensure topic config: %w", err)
		}
	}

	return nil
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

// ensureTopicConfig ensures topic configuration matches desired config.
// Updates any configs that differ from the desired state.
func ensureTopicConfig(
	ctx context.Context,
	admin *kafka.AdminClient,
	topicName string,
	desiredConfig map[string]string,
	log *zap.SugaredLogger,
) error {
	resource := kafka.ConfigResource{
		Type: kafka.ResourceTopic,
		Name: topicName,
	}

	results, err := admin.DescribeConfigs(ctx, []kafka.ConfigResource{resource})
	if err != nil {
		return fmt.Errorf("failed to describe configs for topic %q: %w", topicName, err)
	}

	if len(results) == 0 {
		return fmt.Errorf("no config results returned for topic %q", topicName)
	}

	result := results[0]
	if result.Error.Code() != kafka.ErrNoError {
		return fmt.Errorf("failed to describe configs for topic %q: %w", topicName, result.Error)
	}

	configsToUpdate := make(map[string]kafka.ConfigEntry)
	for key, desiredValue := range desiredConfig {
		// Find current value
		var currentValue string
		var found bool
		for _, entry := range result.Config {
			if entry.Name == key {
				currentValue = entry.Value
				found = true
				break
			}
		}

		if !found || currentValue != desiredValue {
			log.Infow("topic config differs",
				"topic", topicName,
				"config", key,
				"current", currentValue,
				"desired", desiredValue)

			configsToUpdate[key] = kafka.ConfigEntry{
				Name:  key,
				Value: desiredValue,
			}
		}
	}

	if len(configsToUpdate) == 0 {
		log.Debugw("topic configs match desired state", "topic", topicName)
		return nil
	}

	configEntries := make([]kafka.ConfigEntry, 0, len(configsToUpdate))
	for _, entry := range configsToUpdate {
		configEntries = append(configEntries, entry)
	}

	alterResource := kafka.ConfigResource{
		Type:   kafka.ResourceTopic,
		Name:   topicName,
		Config: configEntries,
	}

	alterResults, err := admin.IncrementalAlterConfigs(ctx, []kafka.ConfigResource{alterResource})
	if err != nil {
		return fmt.Errorf("failed to alter configs for topic %q: %w", topicName, err)
	}

	for _, alterResult := range alterResults {
		if alterResult.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("failed to alter configs for topic %q: %w", topicName, alterResult.Error)
		}
		log.Infow("updated topic configs",
			"topic", alterResult.Name,
			"updatedConfigs", len(configsToUpdate))
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
