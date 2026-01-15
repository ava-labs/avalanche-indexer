package processor

import (
	"context"

	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Processor defines the interface for processing Kafka messages.
// Implementations must be safe for concurrent use from multiple goroutines.
// Process should return context.Canceled if ctx is cancelled during execution.
type Processor interface {
	Process(ctx context.Context, msg *cKafka.Message) error
}
