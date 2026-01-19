package kafka

import (
	"fmt"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"go.uber.org/zap"
)

// Default timeout values for Kafka consumer
const (
	DefaultSessionTimeout       = 240 * time.Second
	DefaultMaxPollInterval      = 3400 * time.Second
	DefaultFlushTimeout         = 15 * time.Second
	DefaultGoroutineWaitTimeout = 30 * time.Second
)

// ConsumerConfig holds the configuration for a Kafka consumer
type ConsumerConfig struct {
	DLQTopic                    string         `env:"KAFKA_DLQ_TOPIC"              envDefault:"blocks-dlq"`            // Dead letter queue topic for failed messages
	Topic                       string         `env:"KAFKA_TOPIC"                  envDefault:"blocks"`                // Primary topic to consume from
	BootstrapServers            string         `env:"KAFKA_BOOTSTRAP_SERVERS"      envDefault:"localhost:9092"`        // Kafka broker addresses
	GroupID                     string         `env:"KAFKA_GROUP_ID"               envDefault:"blocks-consumer-group"` // Consumer group ID for offset management
	AutoOffsetReset             string         `env:"KAFKA_AUTO_OFFSET_RESET"      envDefault:"earliest"`              // Offset reset strategy: "earliest" or "latest"
	Concurrency                 int64          `env:"KAFKA_CONCURRENCY"            envDefault:"10"`                    // Maximum concurrent message processors
	OffsetManagerCommitInterval time.Duration  `env:"KAFKA_OFFSET_COMMIT_INTERVAL" envDefault:"10s"`                   // Interval for committing offsets
	SessionTimeout              *time.Duration `env:"KAFKA_SESSION_TIMEOUT"        envDefault:"240s"`                  // Session timeout for Kafka consumer
	MaxPollInterval             *time.Duration `env:"KAFKA_MAX_POLL_INTERVAL"      envDefault:"3400s"`                 // Max poll interval for Kafka consumer
	FlushTimeout                *time.Duration `env:"KAFKA_FLUSH_TIMEOUT"          envDefault:"15s"`                   // Flush timeout for Kafka consumer
	GoroutineWaitTimeout        *time.Duration `env:"KAFKA_GOROUTINE_WAIT_TIMEOUT" envDefault:"30s"`                   // Goroutine wait timeout while Closing the Kafka consumer
	EnableLogs                  bool           `env:"KAFKA_ENABLE_LOGS"            envDefault:"false"`                 // Enable librdkafka client logs
	IsDLQConsumer               bool           `env:"KAFKA_IS_DLQ_CONSUMER"        envDefault:"false"`                 // If true, failed messages are not re-sent to DLQ
}

// LoadConsumerConfig loads Kafka configuration from environment variables
func LoadConsumerConfig() ConsumerConfig {
	var cfg ConsumerConfig
	if err := env.Parse(&cfg); err != nil {
		// Create a temporary logger for error reporting during config loading
		logger, logErr := zap.NewProduction()
		if logErr == nil {
			logger.Sugar().Errorw("failed to parse consumer config", "error", err)
		} else {
			// Fallback to fmt if logger creation fails
			fmt.Fprintf(os.Stderr, "failed to parse consumer config: %v\n", err)
		}
		os.Exit(1)
	}
	return cfg
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
	return c
}
