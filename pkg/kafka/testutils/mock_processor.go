package testutils

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/mock"
)

// MockProcessor is a mock implementation of processor.Processor for testing
type MockProcessor struct {
	mock.Mock
}

// Process mocks the Process method
func (m *MockProcessor) Process(ctx context.Context, msg *kafka.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

