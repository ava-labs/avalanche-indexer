package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	defaultPartitionAssignmentStrategy = "roundrobin"
)

// Consumer provides concurrent Kafka message consumption with at-least-once delivery semantics.
// It manages partition rebalancing, offset commits via a sliding window, and DLQ publishing for failures.
// Not safe for concurrent use of exported methods.
type Consumer struct {
	processor     processor.Processor
	consumer      *ckafka.Consumer
	dlqProducer   *Producer
	log           *zap.SugaredLogger
	sem           *semaphore.Weighted
	offsetManager *OffsetManager
	metrics       *metrics.Metrics

	rebalanceContexts map[int32]rebalanceCtx

	logsDone chan struct{}
	doneCh   chan struct{}
	errCh    chan error

	cfg ConsumerConfig

	rebalanceMutex sync.RWMutex

	wg sync.WaitGroup
}

type rebalanceCtx struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// NewConsumer creates a Consumer and initializes its Kafka consumer, DLQ producer, and offset manager.
// The provided context is used for initializing resources but not for the consumer lifecycle.
// Returns an error if Kafka client creation fails.
func NewConsumer(
	ctx context.Context,
	log *zap.SugaredLogger,
	cfg ConsumerConfig,
	proc processor.Processor,
	m *metrics.Metrics,
) (*Consumer, error) {
	// Apply defaults to config
	cfg = cfg.WithDefaults()

	consumerConfig := ckafka.ConfigMap{
		"bootstrap.servers":             cfg.BootstrapServers,
		"group.id":                      cfg.GroupID,
		"auto.offset.reset":             cfg.AutoOffsetReset,
		"enable.auto.commit":            false,
		"session.timeout.ms":            int(cfg.SessionTimeout.Milliseconds()),
		"max.poll.interval.ms":          int(cfg.MaxPollInterval.Milliseconds()),
		"partition.assignment.strategy": defaultPartitionAssignmentStrategy,
		"go.logs.channel.enable":        cfg.EnableLogs,
		"fetch.message.max.bytes":       cfg.MessageMaxBytes,
	}
	cfg.SASL.ApplyToConfigMap(&consumerConfig)
	consumer, err := ckafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	var dlqProducer *Producer
	if cfg.PublishToDLQ {
		dlqProducerConfig := ckafka.ConfigMap{
			"bootstrap.servers":      cfg.BootstrapServers,
			"acks":                   "all", // All brokers must acknowledge the message
			"linger.ms":              5,     // Batch messages for 5ms
			"batch.size":             16384, // 16KB batch size
			"compression.type":       "lz4", // Fast compression
			"message.max.bytes":      cfg.MessageMaxBytes,
			"enable.idempotence":     true,
			"go.logs.channel.enable": cfg.EnableLogs,
		}
		cfg.SASL.ApplyToConfigMap(&dlqProducerConfig)
		dlqProducer, err = NewProducer(ctx, &dlqProducerConfig, log)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka producer: %w", err)
		}
	}

	offsetManager := NewOffsetManager(
		ctx,
		consumer,
		cfg.OffsetManagerCommitInterval,
		cfg.AutoOffsetReset,
		false,
		log,
		m,
	)

	if cfg.PublishToDLQ && cfg.DLQTopic == "" {
		return nil, errors.New("DLQ topic not configured")
	}

	return &Consumer{
		consumer:          consumer,
		dlqProducer:       dlqProducer,
		log:               log,
		cfg:               cfg,
		sem:               semaphore.NewWeighted(cfg.Concurrency),
		rebalanceContexts: make(map[int32]rebalanceCtx),
		offsetManager:     offsetManager,
		logsDone:          make(chan struct{}),
		errCh:             make(chan error, cfg.Concurrency),
		doneCh:            make(chan struct{}),
		processor:         proc,
		metrics:           m,
	}, nil
}

// Start begins consuming messages from the configured topic and blocks until ctx is cancelled,
// a fatal error occurs, or a processing error is sent to the error channel.
// On shutdown, waits up to 30s for in-flight messages to complete processing.
// Returns an error if subscription fails or if consumer/producer close fails.
func (c *Consumer) Start(ctx context.Context) error {
	c.log.Infow("starting consumer for topic", "topic", c.cfg.Topic)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	if !c.cfg.PublishToDLQ {
		c.log.Warnw("consumer is set to not publish to DLQ on failure",
			"topic", c.cfg.Topic,
		)
	}

	if c.cfg.EnableLogs {
		go c.printKafkaLogs(ctxWithCancel)
	} else {
		close(c.logsDone)
	}

	c.log.Infow("subscribing to topic", "topic", c.cfg.Topic)

	if err := c.consumer.SubscribeTopics([]string{c.cfg.Topic}, c.getRebalanceCallback(ctxWithCancel)); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	var dlqProducerErrs <-chan error
	if c.dlqProducer != nil {
		dlqProducerErrs = c.dlqProducer.Errors()
	}

	c.log.Info("consumer subscribed to topic, starting to poll for messages...")
	var loopErr error
	run := true
	for run {
		select {
		case <-ctx.Done():
			c.log.Info("context done, shutting down consumer...")
			run = false
			continue
		case dlqErr := <-dlqProducerErrs:
			c.log.Errorw("fatal error from DLQ producer, shutting down consumer", "error", dlqErr)
			loopErr = dlqErr
			run = false
			continue
		case procErr := <-c.errCh:
			c.log.Errorw("error from message processing, shutting down consumer", "error", procErr)
			loopErr = procErr
			run = false
			continue
		default:
			ev := c.consumer.Poll(int(c.cfg.PollInterval.Milliseconds()))
			if ev == nil {
				continue
			}

			switch msg := ev.(type) {
			case *ckafka.Message:
				c.metrics.RecordMessageReceived(msg.TopicPartition.Partition)
				c.rebalanceMutex.RLock()
				if _, ok := c.rebalanceContexts[msg.TopicPartition.Partition]; !ok {
					c.log.Errorw("partition not found in rebalance context", "partition", msg.TopicPartition.Partition)
					c.rebalanceMutex.RUnlock()
					continue
				}
				c.dispatch(c.rebalanceContexts[msg.TopicPartition.Partition].ctx, msg)
				c.rebalanceMutex.RUnlock()
			case ckafka.Error:
				if msg.IsFatal() {
					c.metrics.RecordKafkaError(true)
					c.log.Errorw("fatal kafka error", "error", msg)
					loopErr = msg
					run = false
					continue
				}
				c.metrics.RecordKafkaError(false)
				c.log.Warnw("kafka error (non-fatal)", "error", msg)
			default:
				c.metrics.IncUnknownEventCount()
				c.log.Debugw("ignoring kafka event", "event", msg)
			}
		}
	}

	c.log.Info("consumer shutting down...")
	closeErr := c.close()
	if closeErr != nil {
		c.log.Errorw("failed to close consumer", "error", closeErr)
	}

	c.log.Info("consumer shutdown complete")

	if loopErr != nil {
		return loopErr
	}
	return closeErr
}

// dispatch spawns a goroutine to process msg with concurrency control via semaphore.
// If all concurrency slots are occupied, the consumer partitions are paused to apply
// backpressure (stop pre-fetching from brokers), then a blocking acquire waits for a
// slot. Once acquired the partitions are resumed. This keeps librdkafka's internal
// buffers bounded while the background heartbeat thread maintains group membership.
func (c *Consumer) dispatch(ctx context.Context, msg *ckafka.Message) {
	if !c.sem.TryAcquire(1) {
		c.pauseConsumer()
		if err := c.sem.Acquire(ctx, 1); err != nil {
			c.resumeConsumer()
			c.log.Errorw("failed to acquire semaphore probably due to context cancellation or deadline exceeded, skipping message", "error", err)
			return
		}
		c.resumeConsumer()
	}

	c.wg.Add(1)
	c.metrics.IncMessagesInFlight()

	go func() {
		defer c.wg.Done()
		defer c.sem.Release(1)
		defer c.metrics.DecMessagesInFlight()

		start := time.Now()
		err := c.processWithRetry(ctx, msg)
		if err == nil {
			c.offsetManager.InsertOffsetWithRetry(ctx, msg)
			c.metrics.RecordMessageProcessed(msg.TopicPartition.Partition, nil, time.Since(start).Seconds())
			return
		}

		if errors.Is(err, context.Canceled) {
			c.log.Debugw("message processing cancelled", "error", err)
			c.metrics.RecordMessageProcessed(msg.TopicPartition.Partition, err, time.Since(start).Seconds())
			return
		}

		c.log.Errorw("message processing failed after retries",
			"error", err,
			"partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset,
		)

		if !c.cfg.PublishToDLQ {
			c.metrics.RecordMessageProcessed(msg.TopicPartition.Partition, err, time.Since(start).Seconds())
			select {
			case c.errCh <- err:
			default:
				c.log.Errorw("error channel full, dropping error", "error", err)
			}
			return
		}

		dlqPublishStart := time.Now()
		publishErr := c.publishToDLQ(ctx, msg)
		c.metrics.RecordDLQProduction(publishErr, time.Since(dlqPublishStart).Seconds())
		if publishErr != nil {
			if errors.Is(publishErr, context.Canceled) {
				c.metrics.RecordMessageProcessed(msg.TopicPartition.Partition, publishErr, time.Since(start).Seconds())
				return
			}
			c.log.Errorw("failed to publish to DLQ", "error", publishErr)
			c.metrics.RecordMessageProcessed(msg.TopicPartition.Partition, publishErr, time.Since(start).Seconds())
			select {
			case c.errCh <- publishErr:
			default:
				c.log.Errorw("error channel full, dropping error", "error", publishErr)
			}
			return
		}
		c.offsetManager.InsertOffsetWithRetry(ctx, msg)
		c.metrics.RecordMessageProcessed(msg.TopicPartition.Partition, err, time.Since(start).Seconds())
	}()
}

// processWithRetry attempts to process msg, retrying according to the
// configured RetryPolicy. Returns nil on success, context.Canceled if the
// context is cancelled, or the last processing error after retries are
// exhausted. With InfiniteRetries the loop only exits on success or
// context cancellation — the consumer stays stuck on the offset.
func (c *Consumer) processWithRetry(ctx context.Context, msg *ckafka.Message) error {
	err := c.processor.Process(ctx, msg)
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}

	policy := c.cfg.Retry
	for attempt := 0; policy.ShouldRetry(attempt); attempt++ {
		c.metrics.RecordMessageRetry()

		backoff := policy.Backoff(attempt)
		c.log.Errorw("retrying message processing",
			"error", err,
			"attempt", attempt+1,
			"backoff", backoff,
			"partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset,
		)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		err = c.processor.Process(ctx, msg)

		if err == nil || errors.Is(err, context.Canceled) {
			return err
		}
	}

	if c.cfg.Retry.MaxRetries != 0 {
		c.metrics.RecordMessageRetriesExhausted()
	}

	return err
}

// pauseConsumer pauses fetching on all currently assigned partitions and records
// a backpressure metric. It is safe to call when no partitions are assigned.
func (c *Consumer) pauseConsumer() {
	partitions, err := c.consumer.Assignment()
	if err != nil || len(partitions) == 0 {
		return
	}
	if err := c.consumer.Pause(partitions); err != nil {
		c.log.Warnw("failed to pause consumer partitions", "error", err)
		return
	}
	c.metrics.RecordConsumerPause()
	c.log.Debugw("consumer paused due to backpressure", "partitions", len(partitions))
}

// resumeConsumer resumes fetching on all currently assigned partitions.
func (c *Consumer) resumeConsumer() {
	partitions, err := c.consumer.Assignment()
	if err != nil || len(partitions) == 0 {
		return
	}
	if err := c.consumer.Resume(partitions); err != nil {
		c.log.Warnw("failed to resume consumer partitions", "error", err)
		return
	}
	c.metrics.RecordConsumerResume()
	c.log.Debugw("consumer resumed after backpressure cleared", "partitions", len(partitions))
}

// publishToDLQ publishes msg to the configured DLQ topic, preserving original key and value.
// Returns an error if DLQTopic is not configured or if production fails.
func (c *Consumer) publishToDLQ(ctx context.Context, msg *ckafka.Message) error {
	dlqMsg := Msg{
		Topic: c.cfg.DLQTopic,
		Key:   msg.Key,
		Value: msg.Value,
	}

	if err := c.dlqProducer.Produce(ctx, dlqMsg); err != nil {
		return fmt.Errorf("failed to produce to DLQ: %w", err)
	}

	c.log.Infow("published message to DLQ",
		"originalTopic", *msg.TopicPartition.Topic,
		"originalPartition", msg.TopicPartition.Partition,
		"originalOffset", msg.TopicPartition.Offset,
		"dlqTopic", c.cfg.DLQTopic,
	)

	return nil
}

// close gracefully shuts down the consumer by waiting for in-flight processing goroutines
// (with a configurable timeout), then closing the DLQ producer and Kafka consumer.
// Returns an error from the Kafka consumer close operation.
func (c *Consumer) close() error {
	c.log.Info("closing consumer...")
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.log.Info("all in-flight messages processed")
	case <-time.After(*c.cfg.GoroutineWaitTimeout):
		c.log.Warn("timeout waiting for in-flight messages, forcing shutdown")
	}

	close(c.doneCh)
	<-c.logsDone
	if c.dlqProducer != nil {
		c.dlqProducer.Close(*c.cfg.FlushTimeout)
	}
	return c.consumer.Close()
}

// getRebalanceCallback returns a thread-safe callback that manages partition contexts.
// On assignment, creates a cancellable context per partition. On revocation, cancels
// partition contexts to stop in-flight processing for revoked partitions.
func (c *Consumer) getRebalanceCallback(ctx context.Context) ckafka.RebalanceCb {
	return func(kc *ckafka.Consumer, event ckafka.Event) error {
		c.rebalanceMutex.Lock()
		defer c.rebalanceMutex.Unlock()

		switch ev := event.(type) {
		case ckafka.AssignedPartitions:
			c.log.Infow("partitions assigned",
				"protocol", kc.GetRebalanceProtocol(),
				"count", len(ev.Partitions),
				"partitions", ev.Partitions,
			)
			for _, partition := range ev.Partitions {
				rCtx := rebalanceCtx{}
				rCtx.ctx, rCtx.cancel = context.WithCancel(ctx)
				c.rebalanceContexts[partition.Partition] = rCtx
			}

		case ckafka.RevokedPartitions:
			c.log.Infow("partitions revoked",
				"protocol", kc.GetRebalanceProtocol(),
				"count", len(ev.Partitions),
				"partitions", ev.Partitions,
			)

			if kc.AssignmentLost() {
				c.log.Errorw("assignment lost involuntarily, commit may fail")
			}

			for _, partition := range ev.Partitions {
				c.rebalanceContexts[partition.Partition].cancel()
				c.log.Debugf("revoked partition %d. Context %+v canceled",
					partition.Partition,
					c.rebalanceContexts[partition.Partition],
				)
				delete(c.rebalanceContexts, partition.Partition)
			}
		default:
			c.log.Warnw("unexpected rebalance event", "event", event)
		}
		return c.offsetManager.RebalanceCb(kc, event)
	}
}

// printKafkaLogs drains the librdkafka logs channel and outputs to the logger.
// Closes logsDone channel on exit to signal log printing completion.
func (c *Consumer) printKafkaLogs(ctx context.Context) {
	defer close(c.logsDone)
	for {
		select {
		case <-ctx.Done():
			c.log.Info("stopping kafka logs printing for consumer")
			return
		case <-c.doneCh:
			c.log.Info("stopping kafka logs printing for consumer, done channel closed")
			return
		case log, ok := <-c.consumer.Logs():
			if !ok {
				c.log.Info("kafka logs printing for consumer, event channel closed")
				return
			}
			c.log.Debugf("consumer level: %d tag: %s message: %s ", log.Level, log.Tag, log.Message)
		}
	}
}
