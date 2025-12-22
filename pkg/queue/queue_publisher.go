package queue

import "context"

// Msg represents a queue message.
//
// Topic identifies the destination topic.
// Key is used for partitioning when supported by the backend.
// Value contains the message payload.
// Headers contains additional metadata.
type Msg struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

type QueuePublisher interface {
	// Publish publishes a message to the underlying queue.
	//
	// Implementations may block until delivery is confirmed or fail early
	// depending on the underlying system.
	Publish(ctx context.Context, message Msg) error

	// Close stops the publisher and releases all resources.
	//
	// Close MUST be called exactly once. Implementations may block while
	// flushing in-flight messages. Canceling the context may result in
	// message loss depending on the implementation.
	Close(ctx context.Context)
}
