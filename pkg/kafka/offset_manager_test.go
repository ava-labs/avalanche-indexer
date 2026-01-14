package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func createLogger(t *testing.T) *zap.SugaredLogger {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	return logger.Sugar()
}

// Test out of order InsertOffset() calls and ensure all offsets are committed
// even with a non-zero initial lastCommitted and an offset of 0 exists.
func TestUnorderedOffsetsWithNonZeroInit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	partition := int32(0)
	initOffset := kafka.Offset(0)
	assignment := []kafka.TopicPartition{{Partition: partition, Offset: initOffset}}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t))
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 20})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 3})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 1})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 0})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 2})
	time.Sleep(30 * time.Millisecond)

	require.Equal(t, kafka.Offset(3), om.partitionStates[partition].lastCommitted)
	require.Equal(t, 1, len(om.partitionStates[partition].window))
	require.Equal(t, kafka.Offset(20), om.partitionStates[partition].window[0].Offset)
}

// Test ordered InsertOffset() calls with a time break.
func TestOrderedOffsets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	partition := int32(1)
	initOffset := kafka.Offset(3)
	assignment := []kafka.TopicPartition{{Partition: partition, Offset: initOffset}}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t))
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	// if used correctly, offsets 0 and 2 should not exist
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 0})
	// skip 1
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 2})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 3})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 4})

	time.Sleep(30 * time.Millisecond)

	require.Equal(t, kafka.Offset(4), om.partitionStates[partition].lastCommitted)
	require.Equal(t, 0, len(om.partitionStates[partition].window))

	time.Sleep(30 * time.Millisecond)

	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 5})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 6})

	time.Sleep(30 * time.Millisecond)
	require.Equal(t, kafka.Offset(6), om.partitionStates[partition].lastCommitted)
	require.Equal(t, 0, len(om.partitionStates[partition].window))
}

func TestGapBetweenLastCommittedAndWindow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	partition := int32(2)
	initOffset := kafka.Offset(0)
	assignment := []kafka.TopicPartition{{Partition: partition, Offset: initOffset}}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t))
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 3})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 4})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 5})
	time.Sleep(30 * time.Millisecond)

	require.Equal(t, kafka.Offset(0), om.partitionStates[partition].lastCommitted)
	require.Equal(t, 3, len(om.partitionStates[partition].window))

	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 2})
	time.Sleep(30 * time.Millisecond)
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 1})
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, kafka.Offset(5), om.partitionStates[partition].lastCommitted)
	require.Equal(t, 0, len(om.partitionStates[partition].window))
}

func TestMultiplePartitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p1 := int32(0)
	initOffset1 := kafka.Offset(0)

	p2 := int32(3)
	initOffset2 := kafka.Offset(5)

	assignment := []kafka.TopicPartition{
		{Partition: p1, Offset: initOffset1},
		{Partition: p2, Offset: initOffset2},
	}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t))
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 0})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 1})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 2})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 3})

	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 3})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 4})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 5})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 6})

	time.Sleep(30 * time.Millisecond)

	require.Equal(t, kafka.Offset(3), om.partitionStates[p1].lastCommitted)
	require.Equal(t, 0, len(om.partitionStates[p1].window))

	require.Equal(t, kafka.Offset(6), om.partitionStates[p2].lastCommitted)
	require.Equal(t, 0, len(om.partitionStates[p2].window))
}

// Test rebalance events where a second partition assignment is added followed
// by all partitions being revoked.
func TestRebalanceEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p1 := int32(0)
	initOffset1 := kafka.Offset(0)
	assignment := []kafka.TopicPartition{{Partition: p1, Offset: initOffset1}}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t))
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 0})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 1})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 2})

	time.Sleep(30 * time.Millisecond)

	// simulate rebalance adding partition assignment
	p2 := int32(3)
	initOffset2 := kafka.Offset(5)
	om.RebalanceCb(nil, kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: p2, Offset: initOffset2},
		},
	})

	// p1 state should not be affected
	require.Equal(t, kafka.Offset(2), om.partitionStates[p1].lastCommitted)
	require.Equal(t, kafka.Offset(initOffset2), om.partitionStates[p2].lastCommitted)

	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 5})
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 6})

	om.RebalanceCb(nil, kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: p1},
		},
	})

	time.Sleep(30 * time.Millisecond)
	require.Equal(t, kafka.Offset(6), om.partitionStates[p2].lastCommitted)

	// should do nothing and throw warning messages
	om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 8})

	require.Nil(t, om.partitionStates[p1])
	require.Equal(t, 1, len(om.partitionStates))

	// revoke the last partition, consumer would be completely unassigned
	om.RebalanceCb(nil, kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: p2},
		},
	})
	require.Nil(t, om.partitionStates[p2])
	require.Equal(t, 0, len(om.partitionStates))
	time.Sleep(30 * time.Millisecond) // managerLoop should do nothing
}
