package processor

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	testBlockHash    = "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	testBlockchainID = "11111111111111111111111111111111LpoYY"
)

// Mock implementations for testing
type mockBlocksRepo struct {
	writeBlockFunc func(ctx context.Context, block *evmrepo.BlockRow) error
}

func (*mockBlocksRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (m *mockBlocksRepo) WriteBlock(ctx context.Context, block *evmrepo.BlockRow) error {
	if m.writeBlockFunc != nil {
		return m.writeBlockFunc(ctx, block)
	}
	return nil
}

type mockTransactionsRepo struct {
	writeTransactionFunc func(ctx context.Context, tx *evmrepo.TransactionRow) error
}

func (*mockTransactionsRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (m *mockTransactionsRepo) WriteTransaction(ctx context.Context, tx *evmrepo.TransactionRow) error {
	if m.writeTransactionFunc != nil {
		return m.writeTransactionFunc(ctx, tx)
	}
	return nil
}

type mockLogsRepo struct {
	writeLogFunc func(ctx context.Context, log *evmrepo.LogRow) error
}

func (*mockLogsRepo) CreateTableIfNotExists(context.Context) error { return nil }
func (m *mockLogsRepo) WriteLog(ctx context.Context, log *evmrepo.LogRow) error {
	if m.writeLogFunc != nil {
		return m.writeLogFunc(ctx, log)
	}
	return nil
}

// Helper to compare *big.Int values (handles nil cases)
func assertBigIntEqual(t *testing.T, expected, actual *big.Int) {
	t.Helper()
	if expected == nil && actual == nil {
		return
	}
	if expected == nil {
		require.Nil(t, actual, "Expected nil, got %v", actual)
		return
	}
	if actual == nil {
		require.NotNil(t, actual, "Expected %v, got nil", expected)
		return
	}
	assert.Equal(t, 0, expected.Cmp(actual), "Expected %s, got %s", expected.String(), actual.String())
}

// Helper function to create a uint64 pointer
func uintPtr(u uint64) *uint64 {
	return &u
}

// ============================================================================
// CorethLogToLogRow Tests
// ============================================================================

func TestCorehtLogToLogRow_Success(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	log := createTestLog()

	logRow, err := CorethLogToLogRow(log, block)
	require.NoError(t, err)
	require.NotNil(t, logRow)

	assert.Equal(t, testBlockchainID, *logRow.BlockchainID)
	assertBigIntEqual(t, big.NewInt(43113), logRow.EVMChainID)
	assert.Equal(t, uint64(1647), logRow.BlockNumber)
	assert.Equal(t, log.BlockHash.Hex(), logRow.BlockHash)
	assert.Equal(t, time.Unix(1604768510, 0).UTC(), logRow.BlockTime)
	assert.Equal(t, log.TxHash.Hex(), logRow.TxHash)
	assert.Equal(t, uint32(0), logRow.TxIndex)
	assert.Equal(t, log.Address.Hex(), logRow.Address)
	assert.Equal(t, uint32(0), logRow.LogIndex)
	assert.False(t, logRow.Removed)
	assert.Equal(t, log.Data, logRow.Data)

	// Check topics
	assert.NotEmpty(t, logRow.Topic0)
	assert.Equal(t, log.Topics[0].Hex(), logRow.Topic0)
	require.NotNil(t, logRow.Topic1)
	assert.Equal(t, log.Topics[1].Hex(), *logRow.Topic1)
	require.NotNil(t, logRow.Topic2)
	assert.Equal(t, log.Topics[2].Hex(), *logRow.Topic2)
	assert.Nil(t, logRow.Topic3)
}

func TestCorethLogToLogRow_NilBlockchainID(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.BlockchainID = nil
	log := createTestLog()

	logRow, err := CorethLogToLogRow(log, block)
	require.ErrorIs(t, err, evmrepo.ErrBlockChainIDRequired)
	assert.Nil(t, logRow)
}

func TestCorethLogToLogRow_NilEVMChainID(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.EVMChainID = nil
	log := createTestLog()

	logRow, err := CorethLogToLogRow(log, block)
	require.ErrorIs(t, err, evmrepo.ErrEvmChainIDRequired)
	assert.Nil(t, logRow)
}

func TestCorethLogToLogRow_EmptyTopics(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	log := createTestLog()
	log.Topics = []common.Hash{}

	logRow, err := CorethLogToLogRow(log, block)
	require.NoError(t, err)
	require.NotNil(t, logRow)

	assert.Empty(t, logRow.Topic0)
	assert.Nil(t, logRow.Topic1)
	assert.Nil(t, logRow.Topic2)
	assert.Nil(t, logRow.Topic3)
}

func TestCorethLogToLogRow_AllFourTopics(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	log := createTestLog()
	log.Topics = []common.Hash{
		common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
		common.HexToHash("0x0000000000000000000000004142434445464748494a4b4c4d4e4f5051525354"),
		common.HexToHash("0x00000000000000000000000055565758595a5b5c5d5e5f6061626364656667"),
		common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000001234"),
	}

	logRow, err := CorethLogToLogRow(log, block)
	require.NoError(t, err)
	require.NotNil(t, logRow)

	assert.NotEmpty(t, logRow.Topic0)
	require.NotNil(t, logRow.Topic1)
	require.NotNil(t, logRow.Topic2)
	require.NotNil(t, logRow.Topic3)
	assert.Equal(t, log.Topics[3].Hex(), *logRow.Topic3)
}

func TestCorethLogToLogRow_RemovedFlag(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	log := createTestLog()
	log.Removed = true

	logRow, err := CorethLogToLogRow(log, block)
	require.NoError(t, err)
	require.NotNil(t, logRow)

	assert.True(t, logRow.Removed)
}

// ============================================================================
// CorethBlockToBlockRow Tests
// ============================================================================

func TestCorethBlockToBlockRow_Success(t *testing.T) {
	t.Parallel()

	block := createTestBlock()

	blockRow, err := CorethBlockToBlockRow(block)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	assert.Equal(t, testBlockchainID, *blockRow.BlockchainID)
	assertBigIntEqual(t, big.NewInt(43113), blockRow.EVMChainID)
	assertBigIntEqual(t, big.NewInt(1647), blockRow.BlockNumber)
	assert.Equal(t, testBlockHash, blockRow.Hash)
	assert.Equal(t, "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40", blockRow.ParentHash)
	assert.Equal(t, time.Unix(1604768510, 0).UTC(), blockRow.BlockTime)
	assert.Equal(t, uint64(1331), blockRow.Size)
	assert.Equal(t, uint64(20006296), blockRow.GasLimit)
	assert.Equal(t, uint64(183061), blockRow.GasUsed)
	assertBigIntEqual(t, big.NewInt(470000000000), blockRow.BaseFeePerGas)
	assert.Equal(t, "2a", blockRow.Nonce) // 42 in hex
}

func TestCorethBlockToBlockRow_NilBlockchainID(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.BlockchainID = nil

	blockRow, err := CorethBlockToBlockRow(block)
	require.ErrorIs(t, err, evmrepo.ErrBlockChainIDRequired)
	assert.Nil(t, blockRow)
}

func TestCorethBlockToBlockRow_NilNumber(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.Number = nil

	blockRow, err := CorethBlockToBlockRow(block)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	assertBigIntEqual(t, big.NewInt(0), blockRow.BlockNumber)
}

func TestCorethBlockToBlockRow_NilEVMChainID(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.EVMChainID = nil

	blockRow, err := CorethBlockToBlockRow(block)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	assertBigIntEqual(t, big.NewInt(0), blockRow.EVMChainID)
}

func TestCorethBlockToBlockRow_NilDifficulty(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.Difficulty = nil

	blockRow, err := CorethBlockToBlockRow(block)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	assertBigIntEqual(t, big.NewInt(0), blockRow.Difficulty)
	assertBigIntEqual(t, big.NewInt(0), blockRow.TotalDifficulty)
}

func TestCorethBlockToBlockRow_OptionalFields(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.BlobGasUsed = uintPtr(1000)
	block.ExcessBlobGas = uintPtr(2000)
	block.ParentBeaconBlockRoot = "0xbeaconroot1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	block.MinDelayExcess = 5000

	blockRow, err := CorethBlockToBlockRow(block)
	require.NoError(t, err)
	require.NotNil(t, blockRow)

	assert.Equal(t, uint64(1000), blockRow.BlobGasUsed)
	assert.Equal(t, uint64(2000), blockRow.ExcessBlobGas)
	assert.Equal(t, "0xbeaconroot1234567890abcdef1234567890abcdef1234567890abcdef1234567890", blockRow.ParentBeaconBlockRoot)
	assert.Equal(t, uint64(5000), blockRow.MinDelayExcess)
}

// ============================================================================
// CorethTransactionToTransactionRow Tests
// ============================================================================

func TestCorethTransactionToTransactionRow_Success(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	tx := createTestTransaction()
	txIndex := uint64(0)

	txRow, err := CorethTransactionToTransactionRow(tx, block, txIndex)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	assert.Equal(t, testBlockchainID, *txRow.BlockchainID)
	assertBigIntEqual(t, big.NewInt(43113), txRow.EVMChainID)
	assert.Equal(t, uint64(1647), txRow.BlockNumber)
	assert.Equal(t, testBlockHash, txRow.BlockHash)
	assert.Equal(t, time.Unix(1604768510, 0).UTC(), txRow.BlockTime)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273", txRow.Hash)
	assert.Equal(t, "0x4142434445464748494a4b4c4d4e4f5051525354", txRow.From)
	require.NotNil(t, txRow.To)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f6061626364656667", *txRow.To)
	assert.Equal(t, uint64(1), txRow.Nonce)
	assertBigIntEqual(t, big.NewInt(1000000000000000000), txRow.Value)
	assert.Equal(t, uint64(21000), txRow.Gas)
	assertBigIntEqual(t, big.NewInt(470000000000), txRow.GasPrice)
	assert.Equal(t, txIndex, txRow.TransactionIndex)
}

func TestCorethTransactionToTransactionRow_NilBlockchainID(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.BlockchainID = nil
	tx := createTestTransaction()

	txRow, err := CorethTransactionToTransactionRow(tx, block, 0)
	require.ErrorIs(t, err, evmrepo.ErrBlockChainIDRequiredForTx)
	assert.Nil(t, txRow)
}

func TestCorethTransactionToTransactionRow_NilTo(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	tx := createTestTransaction()
	tx.To = "" // Empty To (contract creation)

	txRow, err := CorethTransactionToTransactionRow(tx, block, 0)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	assert.Nil(t, txRow.To)
}

func TestCorethTransactionToTransactionRow_NilValue(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	tx := createTestTransaction()
	tx.Value = nil

	txRow, err := CorethTransactionToTransactionRow(tx, block, 0)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	assertBigIntEqual(t, big.NewInt(0), txRow.Value)
}

func TestCorethTransactionToTransactionRow_NilGasPrice(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	tx := createTestTransaction()
	tx.GasPrice = nil

	txRow, err := CorethTransactionToTransactionRow(tx, block, 0)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	assertBigIntEqual(t, big.NewInt(0), txRow.GasPrice)
}

func TestCorethTransactionToTransactionRow_MaxFeeFields(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	tx := createTestTransaction()
	tx.MaxFeePerGas = big.NewInt(1000000000)
	tx.MaxPriorityFee = big.NewInt(2000000000)

	txRow, err := CorethTransactionToTransactionRow(tx, block, 0)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	require.NotNil(t, txRow.MaxFeePerGas)
	require.NotNil(t, txRow.MaxPriorityFee)
	assertBigIntEqual(t, big.NewInt(1000000000), txRow.MaxFeePerGas)
	assertBigIntEqual(t, big.NewInt(2000000000), txRow.MaxPriorityFee)
}

func TestCorethTransactionToTransactionRow_NilBlockNumber(t *testing.T) {
	t.Parallel()

	block := createTestBlock()
	block.Number = nil
	tx := createTestTransaction()

	txRow, err := CorethTransactionToTransactionRow(tx, block, 0)
	require.NoError(t, err)
	require.NotNil(t, txRow)

	assert.Equal(t, uint64(0), txRow.BlockNumber)
}

// ============================================================================
// Process Tests
// ============================================================================

func TestProcess_NilMessage(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethProcessor(sugar, nil, nil, nil, nil)

	err := proc.Process(t.Context(), nil)
	require.ErrorIs(t, err, ErrNilMessage)
}

func TestProcess_NilMessageValue(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethProcessor(sugar, nil, nil, nil, nil)

	msg := &cKafka.Message{Value: nil}
	err := proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, ErrNilMessage)
}

func TestProcess_InvalidJSON(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethProcessor(sugar, nil, nil, nil, nil)

	msg := &cKafka.Message{Value: []byte(`{invalid json}`)}
	err := proc.Process(t.Context(), msg)

	var jsonErr *json.SyntaxError
	require.ErrorAs(t, err, &jsonErr)
	assert.Contains(t, err.Error(), "failed to unmarshal coreth block")
}

func TestProcess_MissingBlockchainID(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethProcessor(sugar, nil, nil, nil, nil)

	block := &kafkamsg.CorethBlock{
		Number:       big.NewInt(1647),
		Hash:         testBlockHash,
		BlockchainID: nil,
		Transactions: []*kafkamsg.CorethTransaction{},
	}

	data, err := block.Marshal()
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, evmrepo.ErrBlockChainIDRequired)
}

func TestProcess_Success_NoRepos(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	proc := NewCorethProcessor(sugar, nil, nil, nil, nil)

	block := createTestBlock()
	data, err := block.Marshal()
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.NoError(t, err)
}

func TestProcess_Success_WithBlocksRepo(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	var capturedBlock *evmrepo.BlockRow
	blocksRepo := &mockBlocksRepo{
		writeBlockFunc: func(_ context.Context, blk *evmrepo.BlockRow) error {
			capturedBlock = blk
			return nil
		},
	}
	proc := NewCorethProcessor(sugar, blocksRepo, nil, nil, nil)

	block := createTestBlock()
	data, err := block.Marshal()
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.NoError(t, err)

	require.NotNil(t, capturedBlock)
	assert.Equal(t, testBlockchainID, *capturedBlock.BlockchainID)
	assert.Equal(t, testBlockHash, capturedBlock.Hash)
}

func TestProcess_BlocksRepoError(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	expectedErr := errors.New("write block failed")
	blocksRepo := &mockBlocksRepo{
		writeBlockFunc: func(_ context.Context, _ *evmrepo.BlockRow) error {
			return expectedErr
		},
	}
	proc := NewCorethProcessor(sugar, blocksRepo, nil, nil, nil)

	block := createTestBlock()
	data, err := block.Marshal()
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestProcess_Success_WithTransactionsRepo(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	var capturedTxs []*evmrepo.TransactionRow
	txsRepo := &mockTransactionsRepo{
		writeTransactionFunc: func(_ context.Context, tx *evmrepo.TransactionRow) error {
			capturedTxs = append(capturedTxs, tx)
			return nil
		},
	}
	proc := NewCorethProcessor(sugar, nil, txsRepo, nil, nil)

	block := createTestBlock()
	data, err := block.Marshal()
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.NoError(t, err)

	require.Len(t, capturedTxs, 1)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273", capturedTxs[0].Hash)
}

func TestProcess_TransactionsRepoError(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	expectedErr := errors.New("write transaction failed")
	txsRepo := &mockTransactionsRepo{
		writeTransactionFunc: func(_ context.Context, _ *evmrepo.TransactionRow) error {
			return expectedErr
		},
	}
	proc := NewCorethProcessor(sugar, nil, txsRepo, nil, nil)

	block := createTestBlock()
	data, err := block.Marshal()
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestProcess_Success_WithLogsRepo(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	var capturedLogs []*evmrepo.LogRow
	logsRepo := &mockLogsRepo{
		writeLogFunc: func(_ context.Context, lg *evmrepo.LogRow) error {
			capturedLogs = append(capturedLogs, lg)
			return nil
		},
	}
	proc := NewCorethProcessor(sugar, nil, nil, logsRepo, nil)

	block := createTestBlockWithLogs()
	data, err := block.Marshal()
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.NoError(t, err)

	require.Len(t, capturedLogs, 2)
}

func TestProcess_LogsRepoError(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	expectedErr := errors.New("write log failed")
	logsRepo := &mockLogsRepo{
		writeLogFunc: func(_ context.Context, _ *evmrepo.LogRow) error {
			return expectedErr
		},
	}
	proc := NewCorethProcessor(sugar, nil, nil, logsRepo, nil)

	block := createTestBlockWithLogs()
	data, err := block.Marshal()
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.ErrorIs(t, err, expectedErr)
}

func TestProcess_NoTransactions_SkipsRepos(t *testing.T) {
	t.Parallel()

	sugar := zap.NewNop().Sugar()
	txsRepoCalled := false
	logsRepoCalled := false

	txsRepo := &mockTransactionsRepo{
		writeTransactionFunc: func(_ context.Context, _ *evmrepo.TransactionRow) error {
			txsRepoCalled = true
			return nil
		},
	}
	logsRepo := &mockLogsRepo{
		writeLogFunc: func(_ context.Context, _ *evmrepo.LogRow) error {
			logsRepoCalled = true
			return nil
		},
	}
	proc := NewCorethProcessor(sugar, nil, txsRepo, logsRepo, nil)

	block := createTestBlock()
	block.Transactions = []*kafkamsg.CorethTransaction{} // No transactions
	data, err := block.Marshal()
	require.NoError(t, err)

	msg := &cKafka.Message{Value: data}
	err = proc.Process(t.Context(), msg)
	require.NoError(t, err)

	assert.False(t, txsRepoCalled, "transactions repo should not be called")
	assert.False(t, logsRepoCalled, "logs repo should not be called")
}

// ============================================================================
// Helper Functions
// ============================================================================

func createTestBlock() *kafkamsg.CorethBlock {
	blockchainID := testBlockchainID
	return &kafkamsg.CorethBlock{
		BlockchainID:     &blockchainID,
		EVMChainID:       big.NewInt(43113),
		Number:           big.NewInt(1647),
		Hash:             testBlockHash,
		ParentHash:       "0x2122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		Timestamp:        1604768510,
		Size:             1331,
		GasLimit:         20006296,
		GasUsed:          183061,
		Difficulty:       big.NewInt(1),
		Nonce:            42,
		Miner:            "0x4142434445464748494a4b4c4d4e4f5051525354",
		StateRoot:        testBlockHash,
		TransactionsRoot: testBlockHash,
		ReceiptsRoot:     testBlockHash,
		UncleHash:        testBlockHash,
		MixHash:          testBlockHash,
		ExtraData:        "0xd883010916846765746888676f312e31332e38856c696e7578236a756571a22fb6b759507d25baa07790e2dcb952924471d436785469db4655",
		BaseFee:          big.NewInt(470000000000),
		Transactions: []*kafkamsg.CorethTransaction{
			{
				Hash:     "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
				From:     "0x4142434445464748494a4b4c4d4e4f5051525354",
				To:       "0x55565758595a5b5c5d5e5f6061626364656667",
				Nonce:    1,
				Value:    big.NewInt(1000000000000000000),
				Gas:      21000,
				GasPrice: big.NewInt(470000000000),
			},
		},
	}
}

func createTestBlockWithLogs() *kafkamsg.CorethBlock {
	block := createTestBlock()
	block.Transactions[0].Receipt = &kafkamsg.CorethTxReceipt{
		Status:  1,
		GasUsed: 21000,
		Logs: []*kafkamsg.CorethLog{
			createTestLog(),
			{
				Address:     common.HexToAddress("0x55565758595a5b5c5d5e5f6061626364656667"),
				Topics:      []common.Hash{common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")},
				Data:        []byte{0x01, 0x02, 0x03},
				BlockNumber: 1647,
				TxHash:      common.HexToHash("0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273"),
				TxIndex:     0,
				BlockHash:   common.HexToHash(testBlockHash),
				Index:       1,
				Removed:     false,
			},
		},
	}
	return block
}

func createTestTransaction() *kafkamsg.CorethTransaction {
	return &kafkamsg.CorethTransaction{
		Hash:     "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
		From:     "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:       "0x55565758595a5b5c5d5e5f6061626364656667",
		Nonce:    1,
		Value:    big.NewInt(1000000000000000000),
		Gas:      21000,
		GasPrice: big.NewInt(470000000000),
	}
}

func createTestLog() *kafkamsg.CorethLog {
	return &kafkamsg.CorethLog{
		Address: common.HexToAddress("0x4142434445464748494a4b4c4d4e4f5051525354"),
		Topics: []common.Hash{
			common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
			common.HexToHash("0x0000000000000000000000004142434445464748494a4b4c4d4e4f5051525354"),
			common.HexToHash("0x00000000000000000000000055565758595a5b5c5d5e5f6061626364656667"),
		},
		Data:        []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		BlockNumber: 1647,
		TxHash:      common.HexToHash("0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273"),
		TxIndex:     0,
		BlockHash:   common.HexToHash(testBlockHash),
		Index:       0,
		Removed:     false,
	}
}
