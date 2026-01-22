package kafka

import (
	"time"
)

// Default timeout values for Kafka consumer
const (
	DefaultSessionTimeout       = 240 * time.Second
	DefaultMaxPollInterval      = 3400 * time.Second
	DefaultFlushTimeout         = 15 * time.Second
	DefaultGoroutineWaitTimeout = 30 * time.Second
	DefaultPollInterval         = 100 * time.Millisecond
)

// ConsumerConfig holds the configuration for a Kafka consumer
type ConsumerConfig struct {
	DLQTopic                    string         // Dead letter queue topic for failed messages
	Topic                       string         // Primary topic to consume from
	BootstrapServers            string         // Kafka broker addresses
	GroupID                     string         // Consumer group ID for offset management
	AutoOffsetReset             string         // Offset reset strategy: "earliest" or "latest"
	Concurrency                 int64          // Maximum concurrent message processors
	OffsetManagerCommitInterval time.Duration  // Interval for committing offsets
	SessionTimeout              *time.Duration // Session timeout for Kafka consumer
	MaxPollInterval             *time.Duration // Max poll interval for Kafka consumer
	FlushTimeout                *time.Duration // Flush timeout for Kafka consumer
	GoroutineWaitTimeout        *time.Duration // Goroutine wait timeout while Closing the Kafka consumer
	PollInterval                *time.Duration // Poll interval for Kafka consumer
	EnableLogs                  bool           // Enable librdkafka client logs
	PublishToDLQ                bool           // If true, failed messages are not re-sent to DLQ
}

// WithDefaults returns a copy of the config with default values filled in for any nil pointer fields.
// This method does not mutate the original config.
func (c ConsumerConfig) WithDefaults() ConsumerConfig {
	if c.SessionTimeout == nil {
		timeout := DefaultSessionTimeout
		c.SessionTimeout = &timeout
	}
	if c.MaxPollInterval == nil {
		interval := DefaultMaxPollInterval
		c.MaxPollInterval = &interval
	}
	if c.FlushTimeout == nil {
		timeout := DefaultFlushTimeout
		c.FlushTimeout = &timeout
	}
	if c.GoroutineWaitTimeout == nil {
		timeout := DefaultGoroutineWaitTimeout
		c.GoroutineWaitTimeout = &timeout
	}
	if c.PollInterval == nil {
		interval := DefaultPollInterval
		c.PollInterval = &interval
	}
	return c
}
