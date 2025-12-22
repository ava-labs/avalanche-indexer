// Package queue provides abstractions and implementations for publishing
// messages to durable queues.
//
// Implementations may use external systems such as Kafka. The package defines
// a common publisher interface with explicit lifecycle management.
//
// All QueuePublisher implementations require Close to be called exactly once
// to release resources and flush in-flight messages.
package queue
