package queue

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

// KafkaPublisher is a synchronous Kafka producer implementation of QueuePublisher.
//
// Publish blocks until a delivery confirmation is received from Kafka.
// Background goroutines are used to process Kafka producer events and logs.
//
// Close MUST be called exactly once to stop background goroutines and flush
// all in-flight messages.
type KafkaPublisher struct {
	producer   *kafka.Producer
	log        *zap.SugaredLogger
	eventsDone chan struct{}
	logsDone   chan struct{}
	errCh      chan error
	cancel     context.CancelFunc
}

// NewKafkaPublisher creates a Kafka-backed QueuePublisher.
//
// The provided context controls the lifetime of background goroutines.
// Canceling the context signals the publisher to stop processing events.
//
// Callers must call Close to flush messages and release resources.
func NewKafkaPublisher(ctx context.Context, conf *kafka.ConfigMap, log *zap.SugaredLogger) (*KafkaPublisher, error) {
	chCtx, cancel := context.WithCancel(ctx)

	p, err := kafka.NewProducer(conf)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	logsChEnabled, err := conf.Get("go.logs.channel.enable", false)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to get go.logs.channel.enable: %w", err)
	}

	kq := KafkaPublisher{
		producer:   p,
		log:        log,
		eventsDone: make(chan struct{}),
		logsDone:   make(chan struct{}),
		errCh:      make(chan error, 1),
		cancel:     cancel,
	}

	if logsChEnabled.(bool) {
		go kq.printKafkaLogs(chCtx)
	} else {
		close(kq.logsDone)
	}

	go kq.monitorProducerEvents(chCtx)

	return &kq, nil
}

// Publish synchronously publishes a message to Kafka.
//
// Publish blocks until Kafka returns a delivery receipt or the context
// is canceled.
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
// Close must be called exactly once. Calling Close multiple times results
// in undefined behavior.
func (q *KafkaPublisher) Close(ctx context.Context) {
	q.log.Info("closing kafka publisher")
	defer close(q.errCh)
	q.cancel()
	<-q.eventsDone
	<-q.logsDone

	for q.producer.Flush(10000) > 0 {
		q.log.Warn("producer queue not flushed, retrying")
		time.Sleep(time.Second)

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
		case log, ok := <-q.producer.Logs():
			if !ok {
				q.log.Info("kafka logs printing, event channel closed")
				return
			}
			q.log.Debugf("level: %d tag: %s message: %s ", log.Level, log.Tag, log.Message)
		}
	}
}

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
