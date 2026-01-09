package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type Msg struct {
	Topic   string
	Value   []byte
	Key     []byte
	Headers map[string]string
}

// Producer is a synchronous Kafka producer implementation of QueueProducer.
//
// Produce blocks until a delivery confirmation is received from Kafka.
// Background goroutines are used to process Kafka producer events and logs.
//
// Close MUST be called at least once to stop background goroutines and flush
// all in-flight messages.
type Producer struct {
	producer   *kafka.Producer
	log        *zap.SugaredLogger
	errCh      chan error
	eventsDone chan struct{}
	logsDone   chan struct{}
	closedCh   chan struct{}
	once       sync.Once
}

const queueFullErrorRetryDelay = time.Second

// NewProducer creates a Kafka-backed QueueProducer.
//
// The provided context controls the lifetime of background goroutines.
// Canceling the context signals the producer to stop processing events.
//
// Callers must call Close to flush messages and release resources.
func NewProducer(ctx context.Context, conf *kafka.ConfigMap, log *zap.SugaredLogger) (*Producer, error) {
	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	logsChEnabled, err := conf.Get("go.logs.channel.enable", false)
	if err != nil {
		return nil, fmt.Errorf("failed to get go.logs.channel.enable: %w", err)
	}

	kq := Producer{
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

// Produce synchronously produces a message to Kafka.
//
// Produce blocks until either a delivery receipt is received from Kafka
// or the provided context is canceled. If the producer queue is full,
// the message will be retried internally with a 1 second delay.
//
// Produce returns an error in the following cases:
//   - the broker is unavailable,
//   - the message is invalid or exceeds size limits,
//   - the topic or partition is unknown,
//   - authentication or authorization fails.
//
// If the context is canceled before delivery confirmation, Produce returns
// ctx.Err(). The message MAY still be delivered after Produce returns.
// Callers should design for possible duplicate delivery when retrying.
func (q *Producer) Produce(ctx context.Context, msg Msg) error {
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
// If the timeout is reached, Close aborts the flush and closes the producer.
// Callers should be aware that reaching the timeout may result in message loss.
//
// Close must be called at least once. Calling Close multiple times does nothing.
func (q *Producer) Close(timeout time.Duration) {
	q.once.Do(func() {
		q.log.Info("closing kafka producer")
		defer close(q.errCh)

		// Signal the monitor or logs goroutines to stop.
		close(q.closedCh)

		// Wait for the monitor or logs goroutines to stop.
		<-q.eventsDone
		<-q.logsDone

		// Flush the producer queue.
		pending := q.producer.Flush(int(timeout.Milliseconds()))
		if pending > 0 {
			q.log.Warnf("flush incomplete, messages will be lost. pending: %d", pending)
		}

		q.producer.Close()
		q.log.Info("kafka producer closed")
	})
}

// Errors returns a channel that receives at most one fatal error.
// The channel is closed when the producer shuts down.
// Non-fatal Kafka errors are logged and ignored.
//
// After receiving an error, the producer is no longer usable.
// Call Close() and create a new producer to recover.
func (q *Producer) Errors() <-chan error {
	return q.errCh
}

func (q *Producer) printKafkaLogs(ctx context.Context) {
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
func (q *Producer) produceWithRetry(
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
			q.log.Warn("producer queue full, retrying in %s", queueFullErrorRetryDelay)
			time.Sleep(queueFullErrorRetryDelay)
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

func (q *Producer) monitorProducerEvents(ctx context.Context) {
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
				q.log.Error("delivery receipts should be handled during producing")
				if e.TopicPartition.Error != nil {
					q.log.Errorf("failed to deliver message: %v", e.TopicPartition)
				} else {
					q.log.Debugf("Successfully produced record to topic %s partition [%d] @ offset %v",
						*e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
				}
			case kafka.Stats:
				q.log.Infof("kafka stats event received %s", e.String())
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
	e, ok := ev.(*kafka.Message)
	if !ok {
		// Per-message delivery channels only receive *kafka.Message events,
		// but we keep this check as a defensive measure.
		return fmt.Errorf("unexpected delivery event: %T", ev)
	}

	if err := e.TopicPartition.Error; err != nil {
		return fmt.Errorf("delivery failed: %w", err)
	}

	log.Debugf(
		"delivered to topic [%s] partition [%d] at offset [%d]",
		*msg.TopicPartition.Topic,
		e.TopicPartition.Partition,
		e.TopicPartition.Offset,
	)
	return nil
}
