package processor

import (
	"context"

	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Processor interface {
	Process(ctx context.Context, msg *cKafka.Message) error
}
