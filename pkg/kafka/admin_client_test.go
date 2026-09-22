package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func topicMetadataWithReplicas(replicas ...int32) *ckafka.TopicMetadata {
	return &ckafka.TopicMetadata{
		Partitions: []ckafka.PartitionMetadata{{Replicas: replicas}},
	}
}

func TestAwaitSettledMetadata(t *testing.T) {
	t.Run("returns immediately when replicas are already assigned", func(t *testing.T) {
		md := topicMetadataWithReplicas(1)

		// A nil admin client is safe here only because settled metadata must not
		// trigger a re-read; if it ever does, this test panics rather than passes.
		got, err := awaitSettledMetadata(t.Context(), nil, "settled", md)

		require.NoError(t, err)
		assert.Same(t, md, got)
	})

	t.Run("honours a cancelled context while waiting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		got, err := awaitSettledMetadata(ctx, nil, "unsettled", topicMetadataWithReplicas())

		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got)
	})
}

// TestGetReplicationFactorUnsettled covers the pure function against metadata
// that a broker can report mid-creation. The integration suite exercises it
// against a live broker; the case that matters here is a partition that exists
// but has no replicas assigned yet, which reads as replication factor zero.
func TestGetReplicationFactorUnsettled(t *testing.T) {
	tests := []struct {
		name string
		md   *ckafka.TopicMetadata
		want int
	}{
		{"no partitions yet", &ckafka.TopicMetadata{}, 0},
		{"partition without replicas", topicMetadataWithReplicas(), 0},
		{"single replica", topicMetadataWithReplicas(1), 1},
		{"three replicas", topicMetadataWithReplicas(1, 2, 3), 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, getReplicationFactor(tt.md))
		})
	}
}

func TestErrTopicMetadataNotSettledIsDistinct(t *testing.T) {
	assert.NotErrorIs(t, ErrTopicMetadataNotSettled, ErrReplicationFactorMismatch)
}
