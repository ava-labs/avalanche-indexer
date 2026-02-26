package kafka

import (
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
	ctx := t.Context()
	partition := int32(0)
	initOffset := kafka.Offset(0)
	assignment := []kafka.TopicPartition{{Partition: partition, Offset: initOffset}}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t), nil)
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 20}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 3}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 1}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 0}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 2}))
	time.Sleep(30 * time.Millisecond)

	state := om.getPartitionState(partition)
	require.NotNil(t, state)
	require.Equal(t, kafka.Offset(3), state.lastCommitted)
	require.Len(t, state.window, 1)
	require.Equal(t, kafka.Offset(20), state.window[0].Offset)
}

// Test ordered InsertOffset() calls with a time break.
func TestOrderedOffsets(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	partition := int32(1)
	initOffset := kafka.Offset(3)
	assignment := []kafka.TopicPartition{{Partition: partition, Offset: initOffset}}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t), nil)
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	// if used correctly, offsets 0 and 2 should not exist
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 0}))
	// skip 1
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 2}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 3}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 4}))

	time.Sleep(30 * time.Millisecond)

	state := om.getPartitionState(partition)
	require.NotNil(t, state)
	require.Equal(t, kafka.Offset(4), state.lastCommitted)
	require.Empty(t, state.window)

	time.Sleep(30 * time.Millisecond)

	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 5}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 6}))

	time.Sleep(30 * time.Millisecond)
	state = om.getPartitionState(partition)
	require.NotNil(t, state)
	require.Equal(t, kafka.Offset(6), state.lastCommitted)
	require.Empty(t, state.window)
}

func TestGapBetweenLastCommittedAndWindow(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	partition := int32(2)
	initOffset := kafka.Offset(0)
	assignment := []kafka.TopicPartition{{Partition: partition, Offset: initOffset}}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t), nil)
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 3}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 4}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 5}))
	time.Sleep(30 * time.Millisecond)

	state := om.getPartitionState(partition)
	require.NotNil(t, state)
	require.Equal(t, kafka.Offset(0), state.lastCommitted)
	require.Len(t, state.window, 3)

	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 2}))
	time.Sleep(30 * time.Millisecond)
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: partition, Offset: 1}))
	time.Sleep(30 * time.Millisecond)
	state = om.getPartitionState(partition)
	require.NotNil(t, state)
	require.Equal(t, kafka.Offset(5), state.lastCommitted)
	require.Empty(t, state.window)
}

func TestMultiplePartitions(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	p1 := int32(0)
	initOffset1 := kafka.Offset(0)

	p2 := int32(3)
	initOffset2 := kafka.Offset(5)

	assignment := []kafka.TopicPartition{
		{Partition: p1, Offset: initOffset1},
		{Partition: p2, Offset: initOffset2},
	}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t), nil)
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 0}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 1}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 2}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 3}))

	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 3}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 4}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 5}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 6}))

	time.Sleep(30 * time.Millisecond)

	state1 := om.getPartitionState(p1)
	require.NotNil(t, state1)
	require.Equal(t, kafka.Offset(3), state1.lastCommitted)
	require.Empty(t, state1.window)

	state2 := om.getPartitionState(p2)
	require.NotNil(t, state2)
	require.Equal(t, kafka.Offset(6), state2.lastCommitted)
	require.Empty(t, state2.window)
}

// Test rebalance events where a second partition assignment is added followed
// by all partitions being revoked.
func TestRebalanceEvent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	p1 := int32(0)
	initOffset1 := kafka.Offset(0)
	assignment := []kafka.TopicPartition{{Partition: p1, Offset: initOffset1}}

	om := NewOffsetManager(ctx, nil, 10*time.Millisecond, "latest", true, createLogger(t), nil)
	err := om.RebalanceCb(nil, kafka.AssignedPartitions{Partitions: assignment})
	require.NoError(t, err)

	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 0}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 1}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 2}))

	time.Sleep(30 * time.Millisecond)

	// simulate rebalance adding partition assignment
	p2 := int32(3)
	initOffset2 := kafka.Offset(5)
	require.NoError(t, om.RebalanceCb(nil, kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: p2, Offset: initOffset2},
		},
	}))

	// p1 state should not be affected
	state1 := om.getPartitionState(p1)
	require.NotNil(t, state1)
	require.Equal(t, kafka.Offset(2), state1.lastCommitted)

	state2 := om.getPartitionState(p2)
	require.NotNil(t, state2)
	require.Equal(t, initOffset2, state2.lastCommitted)

	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 5}))
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p2, Offset: 6}))

	require.NoError(t, om.RebalanceCb(nil, kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: p1},
		},
	}))

	time.Sleep(30 * time.Millisecond)
	state2 = om.getPartitionState(p2)
	require.NotNil(t, state2)
	require.Equal(t, kafka.Offset(6), state2.lastCommitted)

	// should do nothing and throw warning messages
	require.NoError(t, om.InsertOffset(ctx, kafka.TopicPartition{Partition: p1, Offset: 8}))

	require.Nil(t, om.getPartitionState(p1))
	require.Equal(t, 1, om.getPartitionCount())

	// revoke the last partition, consumer would be completely unassigned
	require.NoError(t, om.RebalanceCb(nil, kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: p2},
		},
	}))
	require.Nil(t, om.getPartitionState(p2))
	require.Equal(t, 0, om.getPartitionCount())
	time.Sleep(30 * time.Millisecond) // managerLoop should do nothing
}
