package checkpointer

import "time"

// Config holds the configuration for the checkpointer.
type Config struct {
	Interval     time.Duration // Interval between checkpoint writes
	WriteTimeout time.Duration // Timeout for each checkpoint write operation
	MaxRetries   int           // Maximum number of retry attempts for failed writes
	RetryBackoff time.Duration // Backoff duration between retry attempts
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Interval:     30 * time.Second,
		WriteTimeout: 1 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 300 * time.Millisecond,
	}
}
