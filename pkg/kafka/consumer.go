package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/processor"
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

const (
	defaultSessionTimeout  = 240_000
	defaultMaxPollInterval = 3_400_000
)

// ConsumerConfig holds configuration for the Consumer.
type ConsumerConfig struct {
	DLQTopic                    string
	Topic                       string
	MaxConcurrency              int64
	IsDLQConsumer               bool
	BootstrapServers            string
	GroupID                     string
	AutoOffsetReset             string
	EnableLogs                  bool
	OffsetManagerCommitInterval time.Duration
}

// Consumer consumes Kafka messages, processes Coreth blocks, and handles failures via DLQ.
type Consumer struct {
	processor         processor.Processor
	consumer          *cKafka.Consumer
	dlqProducer       *Producer
	log               *zap.SugaredLogger
	rebalanceContexts map[int32]rebalanceCtx
	rebalanceMutex    sync.RWMutex
	sem               *semaphore.Weighted
	offsetManager     *OffsetManager
	logsDone          chan struct{}
	doneCh            chan struct{}
	errCh             chan error
	cfg               ConsumerConfig
}

type rebalanceCtx struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// NewConsumer creates a new Consumer.
func NewConsumer(
	ctx context.Context,
	log *zap.SugaredLogger,
	cfg ConsumerConfig,
	processor processor.Processor,
) (*Consumer, error) {
	consumerConfig := cKafka.ConfigMap{
		"bootstrap.servers":             cfg.BootstrapServers,
		"group.id":                      cfg.GroupID,
		"auto.offset.reset":             cfg.AutoOffsetReset,
		"enable.auto.commit":            false,
		"session.timeout.ms":            defaultSessionTimeout,
		"max.poll.interval.ms":          defaultMaxPollInterval,
		"partition.assignment.strategy": "roundrobin",
		"go.logs.channel.enable":        cfg.EnableLogs,
	}
	consumer, err := cKafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	dlqProducerConfig := cKafka.ConfigMap{
		"bootstrap.servers":      cfg.BootstrapServers,
		"acks":                   "all",
		"linger.ms":              5,
		"batch.size":             16384,
		"compression.type":       "lz4",
		"enable.idempotence":     true,
		"go.logs.channel.enable": cfg.EnableLogs,
	}
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
	)

	return &Consumer{
		consumer:          consumer,
		dlqProducer:       dlqProducer,
		log:               log,
		cfg:               cfg,
		sem:               semaphore.NewWeighted(cfg.MaxConcurrency),
		rebalanceContexts: make(map[int32]rebalanceCtx),
		offsetManager:     offsetManager,
		logsDone:          make(chan struct{}),
		errCh:             make(chan error, 1),
		doneCh:            make(chan struct{}),
		processor:         processor,
	}, nil
}

// Start begins consuming messages from the specified topic.
// It spawns a goroutine to handle results (commits and DLQ publishing).
func (c *Consumer) Start(ctx context.Context) error {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	if c.cfg.IsDLQConsumer {
		c.log.Warnw("consumer is subscribing to a DLQ topic - messages will NOT be re-sent to DLQ on failure",
			"topic", c.cfg.Topic,
		)
	}

	// start kafka logs printing if enabled
	if c.cfg.EnableLogs {
		go c.printKafkaLogs(ctxWithCancel)
	} else {
		close(c.logsDone)
	}

	if err := c.consumer.SubscribeTopics([]string{c.cfg.Topic}, c.getRebalanceCallback(ctxWithCancel)); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

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
			ev := c.consumer.Poll(100)
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
				// if the context is cancelled during dispatch, the offset commit will fail and
				// the msg will be reprocessed on restart
				c.dispatch(c.rebalanceContexts[msg.TopicPartition.Partition].ctx, msg)
				c.rebalanceMutex.RUnlock()
			case cKafka.Error:
				if msg.IsFatal() {
					c.log.Errorw("fatal kafka error", "error", msg)
					run = false
					continue
				} else {
					c.log.Warnw("kafka error (non-fatal)", "error", msg)
					continue
				}
			default:
				c.log.Debugw("ignoring kafka event", "event", msg)
			}
		}
	}

	// we can wait for all the goroutines started by dispatch but since our design
	// will be to process at least once, we can finish early and some msgs will be reprocessed.

	err := c.close(ctx)
	if err != nil {
		c.log.Errorw("failed to close consumer", "error", err)
	}

	c.log.Info("consumer shutdown complete")
	return err
}

// dispatch acquires a semaphore slot and processes the message in a goroutine.
func (c *Consumer) dispatch(ctx context.Context, msg *cKafka.Message) {
	// Acquire semaphore (blocks if max concurrency reached)
	if err := c.sem.Acquire(ctx, 1); err != nil {
		c.errCh <- fmt.Errorf("failed to acquire semaphore: %w", err)
		return
	}

	go func() {
		defer c.sem.Release(1)
		err := c.processor.Process(ctx, msg)
		if err != nil {
			if !c.cfg.IsDLQConsumer {
				publishErr := c.publishToDLQ(ctx, msg)
				if publishErr != nil {
					c.log.Errorw("failed to publish to DLQ", "error", publishErr)
					c.errCh <- publishErr
					return
				}
			} else {
				c.errCh <- err
				return
			}
		}
		c.offsetManager.InsertOffsetWithRetry(ctx, msg)
	}()
}

// publishToDLQ sends a failed message to the dead letter queue.
func (c *Consumer) publishToDLQ(ctx context.Context, msg *cKafka.Message) error {
	if c.cfg.DLQTopic == "" {
		return fmt.Errorf("DLQ topic not configured")
	}

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

// close shuts down the consumer and DLQ producer.
func (c *Consumer) close(ctx context.Context) error {
	close(c.doneCh)
	<-c.logsDone
	c.dlqProducer.Close(15000) // 15 second flush timeout
	return c.consumer.Close()
}

// rebalanceCallback handles partition assignment and revocation.
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
				c.log.Error("assignment lost involuntarily, commit may fail")
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

// printKafkaLogs prints kafka logs to the console.
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
