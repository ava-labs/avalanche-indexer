package kafka

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

const (
	// Default suggested Offset Manager parameters
	OffsetManagerCommitInterval  = 5 * time.Second
	OffsetManagerAutoOffsetReset = "latest"

	WindowLengthWarningThreshold = 10000
)

type offsetState struct {
	window        []kafka.TopicPartition
	lastCommitted kafka.Offset
}

/*
A a thread-safe, in-memory sliding window offset manager for Kafka consumers to
manage offsets for each assigned partition and ensure "at least once" message
processing. A single OffsetManager supports only one topic subscription at a
time.

The OffsetManager is configured with a commit interval and initial lastCommitted
offset. At every commit interval, the OffsetManager will analyze the
lastCommitted offset and those in the window to determine the highest offset to
commit to Kafka brokers.

When threads are done processing consumed messages, they should call
InsertCommit() to add the commits to the window.

The window length is unbounded, meaning code bugs can lead to no offsets
committed and a continually growing window size. If the offset window length
goes above WindowLengthWarningThreshold, warning logs will be printed to help
diagnose.
*/
type OffsetManager struct {
	consumer        *kafka.Consumer
	autoOffsetReset string                 // auto.offset.reset config: "earliest" or "latest"
	partitionStates map[int32]*offsetState // map of offset states for each assigned partition
	mutex           sync.Mutex
	dryRun          bool // skip interactions with Brokers for testing
	log             *zap.SugaredLogger
}

// Creates new OffsetManager. To begin the OffsetManager, Start() must be called.
func NewOffsetManager(
	ctx context.Context,
	consumer *kafka.Consumer,
	interval time.Duration,
	autoOffsetReset string,
	dryRun bool,
	log *zap.SugaredLogger,
) *OffsetManager {
	om := &OffsetManager{
		consumer:        consumer,
		autoOffsetReset: autoOffsetReset,
		partitionStates: make(map[int32]*offsetState),
		dryRun:          dryRun,
		log:             log,
	}
	go om.managerLoop(ctx, interval, dryRun)
	return om
}

func (om *OffsetManager) managerLoop(
	ctx context.Context,
	interval time.Duration,
	dryRun bool,
) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			om.commitLatestValidOffsets(dryRun)
		case <-ctx.Done():
			return
		}
	}
}

// For each assigned partition, scan for a contiguous set of offsets in the
// current window starting from lastCommitted to find the latest valid offset to
// commit to Kafka brokers. After successful commits, truncate the window
// accordingly.
func (om *OffsetManager) commitLatestValidOffsets(dryRun bool) {
	var err error
	om.mutex.Lock()
	defer om.mutex.Unlock()

	for partition, state := range om.partitionStates {
		window := state.window
		lastCommitted := state.lastCommitted
		if len(window) == 0 {
			om.log.Debug("no offsets to commit")
			continue
		}

		if window[0].Offset <= lastCommitted+1 {
			end := 0
			for i := 1; i < len(window); i++ {
				// Commit any dangling offsets. This will happen when a new
				// consumer group is formed with auto.offset.reset="latest" and
				// there is no strict consensus on what the "latest" offset is
				// the producer is actively writing to the topic. See
				// InsertOffset() for more details.
				if window[i].Offset <= lastCommitted {
					end = i
					continue
				}

				if window[i].Offset != window[i-1].Offset+1 {
					break
				}
				end = i
			}

			if !dryRun {
				_, err = om.consumer.CommitOffsets([]kafka.TopicPartition{window[end]})
			}
			if err != nil {
				om.log.Errorf("failed to commit offsets: %v", err)
				return
			}

			om.log.Infof("committed offset %d for partition %d", window[end].Offset, partition)
			if end == len(window)-1 {
				om.partitionStates[partition] = &offsetState{
					[]kafka.TopicPartition{},
					window[end].Offset,
				}
			} else {
				om.partitionStates[partition] = &offsetState{window[end+1:], window[end].Offset}
			}
		}

		if len(om.partitionStates[partition].window) > WindowLengthWarningThreshold {
			om.log.Warnf(
				"partition %d window length is high: %d\n",
				partition,
				len(om.partitionStates[partition].window),
			)
		}
	}
}

// Inserts an offset to commit into the sliding window for partition
// `offset.Partition`. The argument `offset.Offset`  should be one higher than
// the offset of the message processed. That is
// `message.TopicPartition.Offset+1`. This is an unclear semantic in Kafka
// client libraries. E.g.
// https://github.com/confluentinc/confluent-kafka-go/issues/350
//
// Topic, Partition, and Offset fields are required.
func (om *OffsetManager) InsertOffset(ctx context.Context, offset kafka.TopicPartition) error {
	om.mutex.Lock()
	defer om.mutex.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	state := om.partitionStates[offset.Partition]
	if state == nil {
		om.log.Warnf("partition %d not found in partition states, ignoring", offset.Partition)
		return nil
	}

	// If the lastCommitted offset is not initialized, we set it to the offset
	// of the actual message that has been processed to generate this offset
	// commit (offset.Offset-1). This first picked offset does not need to be
	// the absolute first one fetched from the broker.
	if state.lastCommitted < 0 {
		state.lastCommitted = offset.Offset - 1
		om.log.Infof("init partition %d lastCommitted to %d", offset.Partition, offset.Offset)
	}

	window := state.window
	i := sort.Search(
		len(window),
		func(j int) bool { return window[j].Offset >= offset.Offset },
	)
	if i < len(window) && window[i].Offset == offset.Offset {
		return nil // already inserted
	}
	om.partitionStates[offset.Partition].window = slices.Insert(window, i, offset)
	return nil
}

// Resets or initializes the OffsetManager's partitionStates. This rebalance
// callback function must be passed to kafka.Consumer.Subscribe(). When the
// consumer joins a group, this will then be called as a way to initialize the
// OffsetManager
func (om *OffsetManager) RebalanceCb(consumer *kafka.Consumer, event kafka.Event) error {
	om.mutex.Lock()
	defer om.mutex.Unlock()
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		// Rebalance events may provide offsets, but offsets seem to be
		// kafka.InvalidOffset (-1001) when a consumer is joining an idle, but
		// already existing, group. So we explicitly get the committed offsets
		// from the broker.
		var err error
		var committedOffsets []kafka.TopicPartition
		if om.dryRun {
			committedOffsets = ev.Partitions
		} else {
			committedOffsets, err = consumer.Committed(ev.Partitions, 5000)
		}

		if err != nil {
			return fmt.Errorf("failed to get committed offsets: %w", err)
		}

		logStr := make([]string, len(committedOffsets))
		for i, co := range committedOffsets {
			om.partitionStates[co.Partition] = &offsetState{
				window:        []kafka.TopicPartition{},
				lastCommitted: co.Offset,
			}

			// If the group's stored offset is lower than the earliest offset of
			// the topic due to its retention policy, librdkafka will
			// immediately start fetching based off auto.offset.reset. We
			// invalidate any stored offset here and will have the OffsetManager
			// pick any first offset to initialize the window.
			if !om.dryRun {
				low, high, err := om.consumer.QueryWatermarkOffsets(*(co.Topic), co.Partition, 5000)
				if err != nil {
					om.log.Errorf("GetWatermarkOffsets failed: %v", err)
					return fmt.Errorf("GetWatermarkOffsets failed: %w", err)
				}

				om.log.Infof(
					"QueryWatermarkOffsets for partition %d: (low: %d, high: %d), auto.offset.reset: %s",
					co.Partition,
					low,
					high,
					om.autoOffsetReset,
				)

				if co.Offset < 0 || co.Offset < kafka.Offset(low) {
					// Reset the offset to kafka.OffsetInvalid (-1001) to
					// indicate a stored offset does not exist or is now out of
					// range
					om.partitionStates[co.Partition].lastCommitted = kafka.OffsetInvalid
				}
			}

			logStr[i] = fmt.Sprintf("(partition: %d, lastCommitted: %d)", co.Partition, om.partitionStates[co.Partition].lastCommitted)
		}

		om.log.Infof("rebalance event, adding partition states: %s\n", strings.Join(logStr, ","))
	case kafka.RevokedPartitions:
		logStr := make([]string, len(ev.Partitions))
		for i, partition := range ev.Partitions {
			logStr[i] = strconv.Itoa(int(partition.Partition))
			delete(om.partitionStates, partition.Partition)
		}
		om.log.Infof("rebalance event, removing state for partitions: %s\n", strings.Join(logStr, ","))
	default:
		om.log.Warnf("unknown rebalance event: %v", event)
	}
	return nil
}

func (om *OffsetManager) InsertOffsetWithRetry(
	ctx context.Context,
	msg *kafka.Message,
) {
	for {
		err := om.InsertOffset(ctx, kafka.TopicPartition{
			Topic:     msg.TopicPartition.Topic,
			Partition: msg.TopicPartition.Partition,
			Offset:    msg.TopicPartition.Offset + 1,
		})
		if err == nil || ctx.Err() != nil {
			return
		}

		om.log.Error("retrying InsertOffset. err ", err)
		time.Sleep(200 * time.Millisecond)
	}
}
