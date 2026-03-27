package processor

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestCoreConsumerProcessor_NilMessage(t *testing.T) {
	proc := NewCoreConsumerProcessor(nil, nil, nil)

	err := proc.Process(t.Context(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilMessage)
	assert.True(t, IsNonRetryable(err))
}

func TestCoreConsumerProcessor_EmptyValue(t *testing.T) {
	proc := NewCoreConsumerProcessor(nil, nil, nil)

	msg := &ckafka.Message{Value: nil}
	err := proc.Process(t.Context(), msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilMessage)
}

func TestCoreConsumerProcessor_InvalidJSON(t *testing.T) {
	proc := NewCoreConsumerProcessor(nil, nil, nil)

	msg := &ckafka.Message{Value: []byte("not json")}
	err := proc.Process(t.Context(), msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnmarshalBlock)
	assert.True(t, IsNonRetryable(err))
}

func TestCoreConsumerProcessor_MissingBlockchainID(t *testing.T) {
	proc := NewCoreConsumerProcessor(nil, nil, nil)

	block := kafkamsg.EVMBlock{
		Number:     big.NewInt(1),
		Hash:       "0xhash",
		EVMChainID: big.NewInt(43114),
		// BlockchainID is nil
	}
	data, err := json.Marshal(block)
	require.NoError(t, err)

	msg := &ckafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.Error(t, err)
	assert.True(t, IsNonRetryable(err))
}
