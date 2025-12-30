package queue

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

// KafkaPublisher is a synchronous Kafka producer implementation of QueuePublisher.
//
// Publish blocks until a delivery confirmation is received from Kafka.
// Background goroutines are used to process Kafka producer events and logs.
//
// Close MUST be called at least once to stop background goroutines and flush
// all in-flight messages.
type KafkaPublisher struct {
	producer   *kafka.Producer
	log        *zap.SugaredLogger
	errCh      chan error
	eventsDone chan struct{}
	logsDone   chan struct{}
	closedCh   chan struct{}
	once       sync.Once
}

const flushTimeoutMs = 10000

// NewKafkaPublisher creates a Kafka-backed QueuePublisher.
//
// The provided context controls the lifetime of background goroutines.
// Canceling the context signals the publisher to stop processing events.
//
// Callers must call Close to flush messages and release resources.
func NewKafkaPublisher(ctx context.Context, conf *kafka.ConfigMap, log *zap.SugaredLogger) (*KafkaPublisher, error) {
	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	logsChEnabled, err := conf.Get("go.logs.channel.enable", false)
	if err != nil {
		return nil, fmt.Errorf("failed to get go.logs.channel.enable: %w", err)
	}

	kq := KafkaPublisher{
		producer:   p,
		log:        log,
		eventsDone: make(chan struct{}),
		logsDone:   make(chan struct{}),
		errCh:      make(chan error, 1),
		closedCh:   make(chan struct{}),
		once:       sync.Once{},
	}

	if logsChEnabled.(bool) {
		go kq.printKafkaLogs(ctx)
	} else {
		close(kq.logsDone)
	}

	go kq.monitorProducerEvents(ctx)

	return &kq, nil
}

// Publish synchronously publishes a message to Kafka.
//
// Publish blocks until either a delivery receipt is received from Kafka
// or the provided context is canceled. If the producer queue is full,
// the message will be retried internally with a 1 second delay.
//
// Publish returns an error in the following cases:
//   - the broker is unavailable,
//   - the message is invalid or exceeds size limits,
//   - the topic or partition is unknown,
//   - authentication or authorization fails.
//
// If the context is canceled before delivery confirmation, Publish returns
// ctx.Err(). The message MAY still be delivered after Publish returns.
// Callers should design for possible duplicate delivery when retrying.
func (q *KafkaPublisher) Publish(ctx context.Context, msg Msg) error {
	deliveryCh := make(chan kafka.Event, 1)
	defer close(deliveryCh)

	kMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &msg.Topic,
			Partition: kafka.PartitionAny,
		},
		Value: msg.Value,
		Key:   msg.Key,
	}

	if err := q.produceWithRetry(ctx, kMsg, deliveryCh); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case e := <-deliveryCh:
		return handleDeliveryEvent(q.log, kMsg, e)
	}
}

// Close stops background goroutines and flushes all pending messages.
//
// Close blocks until all queued messages are delivered to Kafka.
// If the context is canceled, Close aborts the flush and closes the producer.
// Callers should be aware that canceling the context may result in message loss.
//
// Close must be called at least once. Calling Close multiple times does nothing.
func (q *KafkaPublisher) Close(ctx context.Context) {
	q.once.Do(func() {
		q.log.Info("closing kafka publisher")
		defer close(q.errCh)

		// Signal the monitor or logs goroutines to stop.
		close(q.closedCh)

		// Wait for the monitor or logs goroutines to stop.
		<-q.eventsDone
		<-q.logsDone

		// Flush the producer queue.
		for q.producer.Flush(flushTimeoutMs) > 0 {
			q.log.Warn("producer queue not flushed, retrying")
			select {
			case <-ctx.Done():
				q.log.Info("context done, stopping producer flush")
				q.producer.Close()
				return
			default:
				continue
			}
		}

		q.producer.Close()
		q.log.Info("kafka publisher closed")
	})
}

// Errors returns a channel that receives at most one fatal error.
// The channel is closed when the publisher shuts down.
// Non-fatal Kafka errors are logged and ignored.
//
// After receiving an error, the publisher is no longer usable.
// Call Close() and create a new publisher to recover.
func (q *KafkaPublisher) Errors() <-chan error {
	return q.errCh
}

func (q *KafkaPublisher) printKafkaLogs(ctx context.Context) {
	defer close(q.logsDone)
	for {
		select {
		case <-ctx.Done():
			q.log.Info("stopping kafka logs printing")
			return
		case <-q.closedCh:
			q.log.Info("stopping kafka logs printing, done channel closed")
			return
		case log, ok := <-q.producer.Logs():
			if !ok {
				q.log.Info("kafka logs printing, event channel closed")
				return
			}
			q.log.Debugf("level: %d tag: %s message: %s ", log.Level, log.Tag, log.Message)
		}
	}
}

// produceWithRetry produces a message to Kafka with retry logic.
//
// If the context is done, produceWithRetry returns the context error.
// If the producer queue is full, produceWithRetry sleeps for 1 second and retries.
// If the broker is not available, message size is invalid, the message is invalid,
// topic or partition is unknown, or the authentication fails, produceWithRetry returns an error.
func (q *KafkaPublisher) produceWithRetry(
	ctx context.Context,
	msg *kafka.Message,
	deliveryCh chan kafka.Event,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := q.producer.Produce(msg, deliveryCh)
		if err == nil {
			return nil
		}

		kafkaErr, ok := err.(kafka.Error)
		if !ok {
			return fmt.Errorf("failed to produce: %w", err)
		}

		switch kafkaErr.Code() {
		case kafka.ErrQueueFull:
			q.log.Warn("producer queue full, retrying")
			time.Sleep(time.Second)
			continue
		case kafka.ErrBrokerNotAvailable:
			return fmt.Errorf("broker not available: %w", err)
		case kafka.ErrInvalidMsgSize:
			return fmt.Errorf("invalid message size: %w", err)
		case kafka.ErrInvalidMsg:
			return fmt.Errorf("invalid message: %w", err)
		case kafka.ErrUnknownTopicOrPart:
			return fmt.Errorf("unknown topic or partition: %w", err)
		case kafka.ErrAuthentication:
			return fmt.Errorf("authentication error: %w", err)
		default:
			return fmt.Errorf("failed to produce: %w", err)
		}
	}
}

func (q *KafkaPublisher) monitorProducerEvents(ctx context.Context) {
	defer close(q.eventsDone)
	for {
		select {
		case <-ctx.Done():
			q.log.Info("stopping kafka producer events monitoring, context done")
			return
		case <-q.closedCh:
			q.log.Info("stopping kafka producer events monitoring, done channel closed")
			return
		case ev, ok := <-q.producer.Events():
			if !ok {
				err := fmt.Errorf("kafka producer events monitoring, event channel closed")
				select {
				case q.errCh <- err:
				default:
					q.log.Warnf("error channel is full, should not happen: %v", err)
				}
				return
			}

			switch e := ev.(type) {
			case *kafka.Message:
				q.log.Error("delivery receipts should be handled during publishing")
				if e.TopicPartition.Error != nil {
					q.log.Errorf("failed to deliver message: %v", e.TopicPartition)
				} else {
					q.log.Debugf("Successfully produced record to topic %s partition [%d] @ offset %v",
						*e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
				}
			case kafka.Error:
				if e.IsFatal() || e.Code() == kafka.ErrAllBrokersDown {
					err := fmt.Errorf("fatal err or ErrAllBrokersDown: %#x, %w", e.Code(), e)
					select {
					case q.errCh <- err:
					default:
						q.log.Warnf("error channel is full, should not happen: %v", err)
					}
					return
				} else {
					q.log.Warnf("ignoring unexpected kafka error: %#x, %v", e.Code(), e)
				}
			default:
				q.log.Warnf("Unknown event: %+v", e)
			}
		}
	}
}

func handleDeliveryEvent(log *zap.SugaredLogger, msg *kafka.Message, ev kafka.Event) error {
	switch e := ev.(type) {
	case *kafka.Message:
		if err := e.TopicPartition.Error; err != nil {
			return fmt.Errorf("delivery failed: %w", err)
		}

		if !slices.Equal(e.Value, msg.Value) {
			return fmt.Errorf("delivery receipt: %v did not match expected value: %v", e.Value, msg.Value)
		}

		log.Debugf(
			"delivered to topic [%s] partition [%d] at offset [%d]",
			*msg.TopicPartition.Topic,
			e.TopicPartition.Partition,
			e.TopicPartition.Offset,
		)
		return nil

	case kafka.Error:
		return fmt.Errorf(
			"kafka error: code=%d fatal=%t: %w",
			e.Code(),
			e.IsFatal(),
			e,
		)

	default:
		return fmt.Errorf("unexpected delivery event: %T", ev)
	}
}
