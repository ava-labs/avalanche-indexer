package processor

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Mock implementation for internal transactions repository
type mockInternalTransactionsRepo struct {
	writeInternalTransactionFunc        func(ctx context.Context, tx *evmrepo.InternalTransactionRow) error
	batchInsertInternalTransactionsFunc func(ctx context.Context, txs []*evmrepo.InternalTransactionRow) error
	deleteInternalTransactionsFunc      func(ctx context.Context, chainID uint64) error
}

func (*mockInternalTransactionsRepo) CreateTableIfNotExists(context.Context) error { return nil }

func (m *mockInternalTransactionsRepo) WriteInternalTransaction(ctx context.Context, tx *evmrepo.InternalTransactionRow) error {
	if m.writeInternalTransactionFunc != nil {
		return m.writeInternalTransactionFunc(ctx, tx)
	}
	return nil
}

func (m *mockInternalTransactionsRepo) BatchInsertInternalTransactions(ctx context.Context, txs []*evmrepo.InternalTransactionRow) error {
	if m.batchInsertInternalTransactionsFunc != nil {
		return m.batchInsertInternalTransactionsFunc(ctx, txs)
	}
	return nil
}

func (m *mockInternalTransactionsRepo) DeleteInternalTransactions(ctx context.Context, chainID uint64) error {
	if m.deleteInternalTransactionsFunc != nil {
		return m.deleteInternalTransactionsFunc(ctx, chainID)
	}
	return nil
}

// ============================================================================
// CorethTracesProcessor Tests
// ============================================================================

func TestCorethTracesProcessor_Process_NilMessage(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethTracesProcessor(sugar, nil, nil, false, nil)

	err := proc.Process(t.Context(), nil)
	require.ErrorIs(t, err, ErrNilMessage)
	assert.True(t, IsNonRetryable(err), "nil message should be NonRetryable")
}

func TestCorethTracesProcessor_Process_NilMessageValue(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethTracesProcessor(sugar, nil, nil, false, nil)

	msg := &cKafka.Message{Value: nil}
	err := proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, ErrNilMessage)
	assert.True(t, IsNonRetryable(err), "nil value should be NonRetryable")
}

func TestCorethTracesProcessor_Process_InvalidJSON(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethTracesProcessor(sugar, nil, nil, false, nil)

	msg := &cKafka.Message{Value: []byte(`{invalid json}`)}
	err := proc.Process(t.Context(), msg)

	require.ErrorIs(t, err, ErrUnmarshalBlockTrace)
	assert.True(t, IsNonRetryable(err), "unmarshal failure should be NonRetryable")
}

func TestCorethTracesProcessor_Process_MissingBlockchainID(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethTracesProcessor(sugar, nil, nil, false, nil)

	blockTrace := &kafkamsg.EVMBlockTrace{
		EVMChainID:     big.NewInt(43113),
		BlockNumber:    1647,
		BlockTimestamp: 1640000000,
		BlockchainID:   nil,
		Traces:         []json.RawMessage{},
	}

	data, err := json.Marshal(blockTrace)
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, ErrMissingBlockchainID)
	assert.True(t, IsNonRetryable(err), "missing blockchainID should be NonRetryable")
}

func TestCorethTracesProcessor_Process_Success_NoRepo(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethTracesProcessor(sugar, nil, nil, false, nil)

	blockTrace := createTestBlockTrace()
	data, err := json.Marshal(blockTrace)
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.NoError(t, err)
}

func TestCorethTracesProcessor_Process_Success_WithRepo(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	var capturedTxs []*evmrepo.InternalTransactionRow
	repo := &mockInternalTransactionsRepo{
		batchInsertInternalTransactionsFunc: func(_ context.Context, txs []*evmrepo.InternalTransactionRow) error {
			capturedTxs = append(capturedTxs, txs...)
			return nil
		},
	}
	proc := NewCorethTracesProcessor(sugar, repo, nil, true, nil)

	blockTrace := createTestBlockTrace()
	data, err := json.Marshal(blockTrace)
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.NoError(t, err)

	// We should have 3 internal transactions (1 root + 2 children)
	require.Len(t, capturedTxs, 3)

	// Verify root call
	assert.Equal(t, "call_0", capturedTxs[0].CallIndex)
	assert.Equal(t, "CALL", capturedTxs[0].Type)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f7071", capturedTxs[0].TransactionHash)
	assert.Equal(t, uint64(1640000000000), capturedTxs[0].TimestampMs)

	// Verify first child
	assert.Equal(t, "call_0_0", capturedTxs[1].CallIndex)
	assert.Equal(t, "CALL", capturedTxs[1].Type)
	assert.Equal(t, uint64(1640000000000), capturedTxs[1].TimestampMs)

	// Verify second child
	assert.Equal(t, "call_0_1", capturedTxs[2].CallIndex)
	assert.Equal(t, "DELEGATECALL", capturedTxs[2].Type)
	assert.Equal(t, uint64(1640000000000), capturedTxs[2].TimestampMs)
}

func TestCorethTracesProcessor_Process_RepoError(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	expectedErr := errors.New("write failed")
	repo := &mockInternalTransactionsRepo{
		batchInsertInternalTransactionsFunc: func(_ context.Context, _ []*evmrepo.InternalTransactionRow) error {
			return expectedErr
		},
	}
	proc := NewCorethTracesProcessor(sugar, repo, nil, true, nil)

	blockTrace := createTestBlockTrace()
	data, err := json.Marshal(blockTrace)
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestCorethTracesProcessor_Process_RepoFatalError(t *testing.T) {
	t.Parallel()

	chErr := &chdriver.Exception{Code: clickhouseErrAccessDenied, Message: "access denied"}
	repo := &mockInternalTransactionsRepo{
		batchInsertInternalTransactionsFunc: func(_ context.Context, _ []*evmrepo.InternalTransactionRow) error {
			return chErr
		},
	}
	proc := NewCorethTracesProcessor(zap.NewNop().Sugar(), repo, nil, true, nil)

	blockTrace := createTestBlockTrace()
	data, err := json.Marshal(blockTrace)
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	assert.True(t, IsFatal(err), "ClickHouse access denied should be Fatal")
	assert.ErrorIs(t, err, chErr)
}

func TestCorethTracesProcessor_Process_RepoRetryableError(t *testing.T) {
	t.Parallel()

	transientErr := errors.New("connection reset")
	repo := &mockInternalTransactionsRepo{
		batchInsertInternalTransactionsFunc: func(_ context.Context, _ []*evmrepo.InternalTransactionRow) error {
			return transientErr
		},
	}
	proc := NewCorethTracesProcessor(zap.NewNop().Sugar(), repo, nil, true, nil)

	blockTrace := createTestBlockTrace()
	data, err := json.Marshal(blockTrace)
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	assert.False(t, IsFatal(err), "transient error should NOT be Fatal")
	assert.False(t, IsNonRetryable(err), "transient error should NOT be NonRetryable")
	assert.ErrorIs(t, err, transientErr)
}

func TestCorethTracesProcessor_Process_EmptyTraces(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	var capturedTxs []*evmrepo.InternalTransactionRow
	repo := &mockInternalTransactionsRepo{
		batchInsertInternalTransactionsFunc: func(_ context.Context, txs []*evmrepo.InternalTransactionRow) error {
			capturedTxs = append(capturedTxs, txs...)
			return nil
		},
	}
	proc := NewCorethTracesProcessor(sugar, repo, nil, false, nil)

	blockchainID := testBlockchainID
	blockTrace := &kafkamsg.EVMBlockTrace{
		EVMChainID:     big.NewInt(43113),
		BlockNumber:    1647,
		BlockTimestamp: 1640000000,
		BlockchainID:   &blockchainID,
		Traces:         []json.RawMessage{},
	}
	data, err := json.Marshal(blockTrace)
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.NoError(t, err)

	// No traces should be written
	assert.Empty(t, capturedTxs)
}

func TestCorethTracesProcessor_Process_MultipleTraces(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	var capturedTxs []*evmrepo.InternalTransactionRow
	repo := &mockInternalTransactionsRepo{
		batchInsertInternalTransactionsFunc: func(_ context.Context, txs []*evmrepo.InternalTransactionRow) error {
			capturedTxs = append(capturedTxs, txs...)
			return nil
		},
	}
	proc := NewCorethTracesProcessor(sugar, repo, nil, true, nil)

	blockTrace := createTestBlockTraceWithMultipleTransactions()
	data, err := json.Marshal(blockTrace)
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.NoError(t, err)

	// Two transactions, each with root call = 2 traces
	require.Len(t, capturedTxs, 2)
	assert.Equal(t, "0xaaa", capturedTxs[0].TransactionHash)
	assert.Equal(t, "0xbbb", capturedTxs[1].TransactionHash)
}

// ============================================================================
// Helper Functions
// ============================================================================

func createTestBlockTrace() *kafkamsg.EVMBlockTrace {
	blockchainID := testBlockchainID
	trace := map[string]interface{}{
		"txHash": "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f7071",
		"result": map[string]interface{}{
			"type":    "CALL",
			"from":    "0x4142434445464748494a4b4c4d4e4f5051525354",
			"to":      "0x55565758595a5b5c5d5e5f6061626364656667",
			"value":   "0xde0b6b3a7640000",
			"gas":     "0x5208",
			"gasUsed": "0x5208",
			"input":   "0x",
			"output":  "0x",
			"calls": []map[string]interface{}{
				{
					"type":    "CALL",
					"from":    "0x55565758595a5b5c5d5e5f6061626364656667",
					"to":      "0x6162636465666768696a6b6c6d6e6f7071727374",
					"value":   "0x0",
					"gas":     "0x1000",
					"gasUsed": "0x500",
					"input":   "0xabcd",
					"output":  "0x1234",
				},
				{
					"type":    "DELEGATECALL",
					"from":    "0x55565758595a5b5c5d5e5f6061626364656667",
					"to":      "0x7172737475767778797a7b7c7d7e7f8081828384",
					"value":   "0x0",
					"gas":     "0x2000",
					"gasUsed": "0x1000",
					"input":   "0xef",
					"output":  "0xfeed",
				},
			},
		},
	}

	traceBytes, _ := json.Marshal(trace)
	return &kafkamsg.EVMBlockTrace{
		EVMChainID:     big.NewInt(43113),
		BlockNumber:    1647,
		BlockTimestamp: 1640000000,
		TimestampMs:    1640000000000,
		BlockchainID:   &blockchainID,
		Traces:         []json.RawMessage{traceBytes},
	}
}

func createTestBlockTraceWithMultipleTransactions() *kafkamsg.EVMBlockTrace {
	blockchainID := testBlockchainID

	trace1 := map[string]interface{}{
		"txHash": "0xaaa",
		"result": map[string]interface{}{
			"type":    "CALL",
			"from":    "0x4142434445464748494a4b4c4d4e4f5051525354",
			"to":      "0x55565758595a5b5c5d5e5f6061626364656667",
			"value":   "0x0",
			"gas":     "0x5208",
			"gasUsed": "0x5208",
			"input":   "0x",
			"output":  "0x",
		},
	}

	trace2 := map[string]interface{}{
		"txHash": "0xbbb",
		"result": map[string]interface{}{
			"type":    "CREATE",
			"from":    "0x6162636465666768696a6b6c6d6e6f7071727374",
			"to":      "0x7172737475767778797a7b7c7d7e7f8081828384",
			"value":   "0x100",
			"gas":     "0x10000",
			"gasUsed": "0x8000",
			"input":   "0x600060",
			"output":  "0x",
		},
	}

	trace1Bytes, _ := json.Marshal(trace1)
	trace2Bytes, _ := json.Marshal(trace2)

	return &kafkamsg.EVMBlockTrace{
		EVMChainID:     big.NewInt(43113),
		BlockNumber:    1648,
		BlockTimestamp: 1640000001,
		TimestampMs:    1640000001000,
		BlockchainID:   &blockchainID,
		Traces:         []json.RawMessage{trace1Bytes, trace2Bytes},
	}
}
