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
	metrics "github.com/ava-labs/avalanche-indexer/pkg/metrics"
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	defaultPartitionAssignmentStrategy = "roundrobin"
)

// Consumer provides concurrent Kafka message consumption with at-least-once delivery semantics.
// It manages partition rebalancing, offset commits via a sliding window, and DLQ publishing for failures.
// Not safe for concurrent use of exported methods.
type Consumer struct {
	processor     processor.Processor
	consumer      *cKafka.Consumer
	dlqProducer   *Producer
	log           *zap.SugaredLogger
	sem           *semaphore.Weighted
	offsetManager *OffsetManager

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

	consumerConfig := cKafka.ConfigMap{
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
	consumer, err := cKafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	dlqProducerConfig := cKafka.ConfigMap{
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
	dlqProducer, err := NewProducer(ctx, &dlqProducerConfig, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
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

	c.log.Info("consumer subscribed to topic, starting to poll for messages...")
	run := true
	for run {
		select {
		case <-ctx.Done():
			c.log.Info("context done, shutting down consumer...")
			run = false
			continue
		case err := <-c.dlqProducer.Errors():
			c.log.Errorw("fatal error from DLQ producer, shutting down consumer", "error", err)
			run = false
			continue
		case err := <-c.errCh:
			c.log.Errorw("error from consumer, shutting down consumer", "error", err)
			run = false
			continue
		default:
			ev := c.consumer.Poll(int(c.cfg.PollInterval.Milliseconds()))
			if ev == nil {
				continue
			}

			switch msg := ev.(type) {
			case *cKafka.Message:
				c.rebalanceMutex.RLock()
				if _, ok := c.rebalanceContexts[msg.TopicPartition.Partition]; !ok {
					c.log.Errorw("partition not found in rebalance context", "partition", msg.TopicPartition.Partition)
					c.rebalanceMutex.RUnlock()
					continue
				}
				c.dispatch(c.rebalanceContexts[msg.TopicPartition.Partition].ctx, msg)
				c.rebalanceMutex.RUnlock()
			case cKafka.Error:
				if msg.IsFatal() {
					c.log.Errorw("fatal kafka error", "error", msg)
					run = false
					continue
				}
				c.log.Warnw("kafka error (non-fatal)", "error", msg)
			default:
				c.log.Debugw("ignoring kafka event", "event", msg)
			}
		}
	}

	c.log.Info("consumer shutting down...")
	err := c.close()
	if err != nil {
		c.log.Errorw("failed to close consumer", "error", err)
	}

	c.log.Info("consumer shutdown complete")
	return err
}

// dispatch spawns a goroutine to process msg with concurrency control via semaphore.
// Returns immediately after spawning (non-blocking). If context is cancelled during semaphore
// acquisition, the message is dropped (will be reprocessed after rebalance).
// On successful processing, commits offset. On failure, publishes to DLQ (if configured) before committing.
func (c *Consumer) dispatch(ctx context.Context, msg *cKafka.Message) {
	if err := c.sem.Acquire(ctx, 1); err != nil {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer c.sem.Release(1)
		err := c.processor.Process(ctx, msg)
		if err == nil {
			c.offsetManager.InsertOffsetWithRetry(ctx, msg)
			return
		}

		c.log.Errorw("error processing message", "error", err, "partition", msg.TopicPartition.Partition, "offset", msg.TopicPartition.Offset)

		if errors.Is(err, context.Canceled) {
			c.log.Debugw("processing cancelled due to context cancellation",
				"partition", msg.TopicPartition.Partition,
				"offset", msg.TopicPartition.Offset,
			)
			return
		}

		if !c.cfg.PublishToDLQ {
			select {
			case c.errCh <- err:
			default:
				c.log.Errorw("error channel full, dropping error", "error", err)
			}
			return
		}

		publishErr := c.publishToDLQ(ctx, msg)
		if publishErr != nil {
			if errors.Is(publishErr, context.Canceled) {
				return
			}
			c.log.Errorw("failed to publish to DLQ", "error", publishErr)
			select {
			case c.errCh <- publishErr:
			default:
				c.log.Errorw("error channel full, dropping error", "error", publishErr)
			}
			return
		}
		c.offsetManager.InsertOffsetWithRetry(ctx, msg)
	}()
}

// publishToDLQ publishes msg to the configured DLQ topic, preserving original key and value.
// Returns an error if DLQTopic is not configured or if production fails.
func (c *Consumer) publishToDLQ(ctx context.Context, msg *cKafka.Message) error {
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
	c.dlqProducer.Close(*c.cfg.FlushTimeout)
	return c.consumer.Close()
}

// getRebalanceCallback returns a thread-safe callback that manages partition contexts.
// On assignment, creates a cancellable context per partition. On revocation, cancels
// partition contexts to stop in-flight processing for revoked partitions.
func (c *Consumer) getRebalanceCallback(ctx context.Context) cKafka.RebalanceCb {
	return func(kc *cKafka.Consumer, event cKafka.Event) error {
		c.rebalanceMutex.Lock()
		defer c.rebalanceMutex.Unlock()

		switch ev := event.(type) {
		case cKafka.AssignedPartitions:
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

		case cKafka.RevokedPartitions:
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
