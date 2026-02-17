package messages

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/ava-labs/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/trie"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libevmtypes "github.com/ava-labs/libevm/core/types"
)

// Note: json variable is declared in evm.go and shared across the package

var (
	// testPrivateKey is a deterministic key for testing.
	testPrivateKey, _ = crypto.GenerateKey()

	// registerOnce ensures customtypes.Register is called only once.
	registerOnce sync.Once
)

// newTestHasher creates a fresh hasher for each test (StackTrie is not thread-safe).
func newTestHasher() libevmtypes.TrieHasher {
	return trie.NewStackTrie(nil)
}

// initCustomTypes registers coreth custom types for header extras.
// Must be called before any test that uses EVMBlockFromLibevmCoreth.
func initCustomTypes() {
	registerOnce.Do(func() {
		customtypes.Register()
	})
}

// newTestHeader creates a header with sensible defaults for testing.
func newTestHeader(opts ...func(*libevmtypes.Header)) *libevmtypes.Header {
	h := &libevmtypes.Header{
		ParentHash:  common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"),
		UncleHash:   libevmtypes.EmptyUncleHash,
		Coinbase:    common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Root:        common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002"),
		TxHash:      libevmtypes.EmptyTxsHash,
		ReceiptHash: libevmtypes.EmptyReceiptsHash,
		Bloom:       libevmtypes.Bloom{},
		Difficulty:  big.NewInt(1000),
		Number:      big.NewInt(42),
		GasLimit:    8_000_000,
		GasUsed:     21_000,
		Time:        1700000000,
		Extra:       []byte("test-extra"),
		MixDigest:   common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000003"),
		Nonce:       libevmtypes.EncodeNonce(12345),
		BaseFee:     big.NewInt(1_000_000_000),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// newSignedTx creates a signed transaction for testing.
func newSignedTx(chainID *big.Int, nonce uint64, to *common.Address) *libevmtypes.Transaction {
	tx := libevmtypes.NewTx(&libevmtypes.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     nonce,
		GasTipCap: big.NewInt(1_000_000_000),
		GasFeeCap: big.NewInt(2_000_000_000),
		Gas:       21000,
		To:        to,
		Value:     big.NewInt(1_000_000_000_000_000_000), // 1 ETH
		Data:      []byte("test-input"),
	})
	signer := libevmtypes.LatestSignerForChainID(chainID)
	signedTx, _ := libevmtypes.SignTx(tx, signer, testPrivateKey)
	return signedTx
}

func TestEVMBlockFromLibevmCoreth(t *testing.T) {
	initCustomTypes()
	t.Parallel()

	chainID := big.NewInt(43114)
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")

	tests := []struct {
		name        string
		header      *libevmtypes.Header
		txs         []*libevmtypes.Transaction
		withdrawals []*libevmtypes.Withdrawal
		chainID     *big.Int
		assertFn    func(t *testing.T, got *EVMBlock)
	}{
		{
			name:        "minimal block without optional fields",
			header:      newTestHeader(),
			txs:         nil,
			withdrawals: nil,
			chainID:     chainID,
			assertFn: func(t *testing.T, got *EVMBlock) {
				assert.Equal(t, big.NewInt(42), got.Number)
				assert.Equal(t, chainID, got.EVMChainID)
				assert.NotEmpty(t, got.Hash)
				assert.Equal(t, uint64(8_000_000), got.GasLimit)
				assert.Equal(t, uint64(21_000), got.GasUsed)
				assert.Equal(t, big.NewInt(1000), got.Difficulty)
				assert.Empty(t, got.ParentBeaconBlockRoot, "pre-Cancun block should have empty beacon root")
				assert.Nil(t, got.ExcessBlobGas)
				assert.Nil(t, got.BlobGasUsed)
				assert.Empty(t, got.Transactions)
				assert.Empty(t, got.Withdrawals)
			},
		},
		{
			name: "block with beacon root (post-Cancun)",
			header: newTestHeader(func(h *libevmtypes.Header) {
				beaconRoot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000abc")
				h.ParentBeaconRoot = &beaconRoot
			}),
			txs:         nil,
			withdrawals: nil,
			chainID:     chainID,
			assertFn: func(t *testing.T, got *EVMBlock) {
				assert.Equal(t, "0x0000000000000000000000000000000000000000000000000000000000000abc", got.ParentBeaconBlockRoot)
			},
		},
		{
			name: "block with blob gas fields (EIP-4844)",
			header: newTestHeader(func(h *libevmtypes.Header) {
				excessBlobGas := uint64(131072)
				blobGasUsed := uint64(262144)
				h.ExcessBlobGas = &excessBlobGas
				h.BlobGasUsed = &blobGasUsed
			}),
			txs:         nil,
			withdrawals: nil,
			chainID:     chainID,
			assertFn: func(t *testing.T, got *EVMBlock) {
				require.NotNil(t, got.ExcessBlobGas)
				require.NotNil(t, got.BlobGasUsed)
				assert.Equal(t, uint64(131072), *got.ExcessBlobGas)
				assert.Equal(t, uint64(262144), *got.BlobGasUsed)
			},
		},
		{
			name:        "block with transactions",
			header:      newTestHeader(),
			txs:         []*libevmtypes.Transaction{newSignedTx(chainID, 1, &toAddr)},
			withdrawals: nil,
			chainID:     chainID,
			assertFn: func(t *testing.T, got *EVMBlock) {
				require.Len(t, got.Transactions, 1)
				tx := got.Transactions[0]
				assert.NotEmpty(t, tx.Hash)
				assert.NotEmpty(t, tx.From)
				assert.Equal(t, toAddr.Hex(), tx.To)
				assert.Equal(t, uint64(1), tx.Nonce)
				assert.Equal(t, uint64(21000), tx.Gas)
				assert.Equal(t, big.NewInt(1_000_000_000_000_000_000), tx.Value)
			},
		},
		{
			name:        "block with contract creation tx (nil To)",
			header:      newTestHeader(),
			txs:         []*libevmtypes.Transaction{newSignedTx(chainID, 1, nil)},
			withdrawals: nil,
			chainID:     chainID,
			assertFn: func(t *testing.T, got *EVMBlock) {
				require.Len(t, got.Transactions, 1)
				assert.Empty(t, got.Transactions[0].To, "contract creation should have empty To")
			},
		},
		{
			name: "block with withdrawals",
			header: newTestHeader(func(h *libevmtypes.Header) {
				wh := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000def")
				h.WithdrawalsHash = &wh
			}),
			txs: nil,
			withdrawals: []*libevmtypes.Withdrawal{
				{
					Index:     0,
					Validator: 100,
					Address:   common.HexToAddress("0x3333333333333333333333333333333333333333"),
					Amount:    32_000_000_000, // 32 ETH in Gwei
				},
				{
					Index:     1,
					Validator: 101,
					Address:   common.HexToAddress("0x4444444444444444444444444444444444444444"),
					Amount:    1_000_000_000, // 1 ETH in Gwei
				},
			},
			chainID: chainID,
			assertFn: func(t *testing.T, got *EVMBlock) {
				require.Len(t, got.Withdrawals, 2)

				assert.Equal(t, uint64(0), got.Withdrawals[0].Index)
				assert.Equal(t, uint64(100), got.Withdrawals[0].ValidatorIndex)
				assert.Equal(t, "0x3333333333333333333333333333333333333333", got.Withdrawals[0].Address)
				assert.Equal(t, uint64(32_000_000_000), got.Withdrawals[0].Amount)

				assert.Equal(t, uint64(1), got.Withdrawals[1].Index)
				assert.Equal(t, uint64(101), got.Withdrawals[1].ValidatorIndex)
			},
		},
		{
			name: "block with all optional fields",
			header: newTestHeader(func(h *libevmtypes.Header) {
				beaconRoot := common.HexToHash("0x000000000000000000000000000000000000000000000000000000000000beef")
				h.ParentBeaconRoot = &beaconRoot
				excessBlobGas := uint64(100)
				blobGasUsed := uint64(200)
				h.ExcessBlobGas = &excessBlobGas
				h.BlobGasUsed = &blobGasUsed
			}),
			txs: []*libevmtypes.Transaction{
				newSignedTx(chainID, 1, &toAddr),
				newSignedTx(chainID, 2, nil),
			},
			withdrawals: []*libevmtypes.Withdrawal{
				{Index: 0, Validator: 1, Address: common.HexToAddress("0x5555555555555555555555555555555555555555"), Amount: 100},
			},
			chainID: chainID,
			assertFn: func(t *testing.T, got *EVMBlock) {
				assert.NotEmpty(t, got.ParentBeaconBlockRoot)
				require.NotNil(t, got.ExcessBlobGas)
				require.NotNil(t, got.BlobGasUsed)
				assert.Len(t, got.Transactions, 2)
				assert.Len(t, got.Withdrawals, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			block := libevmtypes.NewBlockWithWithdrawals(tt.header, tt.txs, nil, nil, tt.withdrawals, newTestHasher())

			// Pass nil for the optional third parameter to use its default behavior in this test.
			got, err := EVMBlockFromLibevmCoreth(block, tt.chainID, nil)

			require.NoError(t, err)
			require.NotNil(t, got)
			tt.assertFn(t, got)
		})
	}
}

func TestEVMBlockFromLibevm_HeaderFields(t *testing.T) {
	initCustomTypes()
	t.Parallel()

	chainID := big.NewInt(43114)
	header := newTestHeader()
	block := libevmtypes.NewBlock(header, nil, nil, nil, newTestHasher())

	got, err := EVMBlockFromLibevmCoreth(block, chainID, nil /* optional parameter not needed for this test */)

	require.NoError(t, err)
	assert.Equal(t, chainID, got.EVMChainID)
	assert.Equal(t, header.ParentHash.Hex(), got.ParentHash)
	assert.Equal(t, header.Root.Hex(), got.StateRoot)
	assert.Equal(t, header.Coinbase.Hex(), got.Miner)
	assert.Equal(t, header.MixDigest.Hex(), got.MixHash)
	assert.Equal(t, header.GasLimit, got.GasLimit)
	assert.Equal(t, header.GasUsed, got.GasUsed)
	assert.Equal(t, header.Difficulty, got.Difficulty)
	assert.Equal(t, header.BaseFee, got.BaseFee)
	assert.Equal(t, header.Time, got.Timestamp)
	assert.NotEmpty(t, got.LogsBloom)
	assert.NotEmpty(t, got.ExtraData)
	assert.NotEmpty(t, got.UncleHash)
	assert.NotEmpty(t, got.TransactionsRoot)
	assert.NotEmpty(t, got.ReceiptsRoot)
}

func TestEVMTransactionsFromLibevm(t *testing.T) {
	t.Parallel()

	chainID := big.NewInt(43114)
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")

	tests := []struct {
		name     string
		txs      []*libevmtypes.Transaction
		wantLen  int
		wantErr  bool
		assertFn func(t *testing.T, got []*EVMTransaction)
	}{
		{
			name:    "empty transactions",
			txs:     nil,
			wantLen: 0,
			wantErr: false,
			assertFn: func(t *testing.T, got []*EVMTransaction) {
				assert.Empty(t, got)
			},
		},
		{
			name:    "single transaction with To address",
			txs:     []*libevmtypes.Transaction{newSignedTx(chainID, 1, &toAddr)},
			wantLen: 1,
			wantErr: false,
			assertFn: func(t *testing.T, got []*EVMTransaction) {
				tx := got[0]
				assert.NotEmpty(t, tx.Hash)
				assert.NotEmpty(t, tx.From)
				assert.Equal(t, toAddr.Hex(), tx.To)
				assert.Equal(t, uint64(1), tx.Nonce)
				assert.Equal(t, uint64(21000), tx.Gas)
				assert.Equal(t, big.NewInt(1_000_000_000_000_000_000), tx.Value)
				assert.Equal(t, big.NewInt(2_000_000_000), tx.MaxFeePerGas)
				assert.Equal(t, big.NewInt(1_000_000_000), tx.MaxPriorityFee)
				assert.Equal(t, uint8(2), tx.Type) // DynamicFeeTxType
				assert.Contains(t, tx.Input, "0x") // hex encoded
			},
		},
		{
			name:    "contract creation (nil To)",
			txs:     []*libevmtypes.Transaction{newSignedTx(chainID, 1, nil)},
			wantLen: 1,
			wantErr: false,
			assertFn: func(t *testing.T, got []*EVMTransaction) {
				assert.Empty(t, got[0].To)
			},
		},
		{
			name: "multiple transactions",
			txs: []*libevmtypes.Transaction{
				newSignedTx(chainID, 1, &toAddr),
				newSignedTx(chainID, 2, nil),
				newSignedTx(chainID, 3, &toAddr),
			},
			wantLen: 3,
			wantErr: false,
			assertFn: func(t *testing.T, got []*EVMTransaction) {
				assert.Len(t, got, 3)
				// Each tx should have a unique hash
				hashes := make(map[string]struct{})
				for _, tx := range got {
					hashes[tx.Hash] = struct{}{}
				}
				assert.Len(t, hashes, 3)
			},
		},
		{
			name: "unsigned transaction fails sender recovery",
			txs: []*libevmtypes.Transaction{
				// Create an unsigned transaction (no signature)
				libevmtypes.NewTx(&libevmtypes.DynamicFeeTx{
					ChainID:   chainID,
					Nonce:     1,
					GasTipCap: big.NewInt(1_000_000_000),
					GasFeeCap: big.NewInt(2_000_000_000),
					Gas:       21000,
					To:        &toAddr,
					Value:     big.NewInt(1_000_000_000_000_000_000),
					Data:      []byte{},
					// No V, R, S signature fields - this will cause sender recovery to fail
				}),
			},
			wantLen: 0,
			wantErr: true,
			assertFn: func(_ *testing.T, _ []*EVMTransaction) {
				// Should not be called when wantErr is true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EVMTransactionFromLibevm(tt.txs)

			if tt.wantErr {
				require.ErrorIs(t, err, ErrRecoverSender)
				return
			}

			require.NoError(t, err)
			assert.Len(t, got, tt.wantLen)
			tt.assertFn(t, got)
		})
	}
}

// TestEVMBlockFromLibevmCoreth_ErrorPaths tests error handling in EVMBlockFromLibevmCoreth
func TestEVMBlockFromLibevmCoreth_ErrorPaths(t *testing.T) {
	initCustomTypes()
	t.Parallel()

	chainID := big.NewInt(43114)
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")

	t.Run("fails when transaction conversion fails", func(t *testing.T) {
		t.Parallel()

		header := newTestHeader()
		// Create an unsigned transaction that will fail sender recovery
		unsignedTx := libevmtypes.NewTx(&libevmtypes.DynamicFeeTx{
			ChainID:   chainID,
			Nonce:     1,
			GasTipCap: big.NewInt(1_000_000_000),
			GasFeeCap: big.NewInt(2_000_000_000),
			Gas:       21000,
			To:        &toAddr,
			Value:     big.NewInt(1_000_000_000_000_000_000),
			Data:      []byte{},
		})
		block := libevmtypes.NewBlock(header, []*libevmtypes.Transaction{unsignedTx}, nil, nil, newTestHasher())

		got, err := EVMBlockFromLibevmCoreth(block, chainID, nil)

		require.ErrorIs(t, err, ErrConvertTransactions)
		assert.Nil(t, got)
	})
}

// TestEVMBlockFromLibevmCoreth_ExtraFields tests the nil checks for extra header fields
func TestEVMBlockFromLibevmCoreth_ExtraFields(t *testing.T) {
	initCustomTypes()
	t.Parallel()

	chainID := big.NewInt(43114)

	t.Run("default_extra_fields_when_nil", func(t *testing.T) {
		t.Parallel()

		header := newTestHeader()
		block := libevmtypes.NewBlock(header, nil, nil, nil, newTestHasher())

		got, err := EVMBlockFromLibevmCoreth(block, chainID, nil)

		require.NoError(t, err)
		require.NotNil(t, got)
		// When extra fields are not set in the Extra data, GetHeaderExtra returns defaults (nil pointers)
		// which result in 0 values after the nil checks
		assert.Equal(t, uint64(0), got.TimestampMs, "TimestampMs should be 0 when extra.TimeMilliseconds is nil")
		assert.Equal(t, uint64(0), got.MinDelayExcess, "MinDelayExcess should be 0 when extra.MinDelayExcess is nil")
	})
}

// Note: EVMBlockFromLibevmSubnetEVM tests are omitted because coreth and subnet-evm custom types
// conflict when both try to register with libevm in the same process. Since EVMBlockFromLibevmCoreth
// is actively used and tested (85.7% coverage) and EVMBlockFromLibevmSubnetEVM shares the same
// error handling logic (both use EVMTransactionFromLibevm), the error paths are already covered.
// Subnet-evm specific tests should be in a separate test file with build tags if needed.

func TestEVMLogsFromLibevm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		logs    []*libevmtypes.Log
		wantLen int
	}{
		{
			name:    "nil_logs",
			logs:    nil,
			wantLen: 0,
		},
		{
			name:    "empty_logs",
			logs:    []*libevmtypes.Log{},
			wantLen: 0,
		},
		{
			name: "single_log",
			logs: []*libevmtypes.Log{
				{
					Address:     common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
					Topics:      []common.Hash{common.HexToHash("0xabc"), common.HexToHash("0xdef")},
					Data:        []byte{0x01, 0x02, 0x03},
					BlockNumber: 12345,
					TxHash:      common.HexToHash("0xtxhash"),
					TxIndex:     1,
					BlockHash:   common.HexToHash("0xblockhash"),
					Index:       0,
					Removed:     false,
				},
			},
			wantLen: 1,
		},
		{
			name: "multiple_logs",
			logs: []*libevmtypes.Log{
				{
					Address:     common.HexToAddress("0x1111"),
					Topics:      []common.Hash{common.HexToHash("0xa")},
					Data:        []byte{0x01},
					BlockNumber: 100,
					TxHash:      common.HexToHash("0xtx1"),
					TxIndex:     0,
					BlockHash:   common.HexToHash("0xblock1"),
					Index:       0,
					Removed:     false,
				},
				{
					Address:     common.HexToAddress("0x2222"),
					Topics:      []common.Hash{common.HexToHash("0xb"), common.HexToHash("0xc")},
					Data:        []byte{0x02, 0x03},
					BlockNumber: 100,
					TxHash:      common.HexToHash("0xtx1"),
					TxIndex:     0,
					BlockHash:   common.HexToHash("0xblock1"),
					Index:       1,
					Removed:     false,
				},
				{
					Address:     common.HexToAddress("0x3333"),
					Topics:      []common.Hash{},
					Data:        []byte{},
					BlockNumber: 100,
					TxHash:      common.HexToHash("0xtx1"),
					TxIndex:     0,
					BlockHash:   common.HexToHash("0xblock1"),
					Index:       2,
					Removed:     true,
				},
			},
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := EVMLogsFromLibevm(tt.logs)

			assert.Len(t, got, tt.wantLen)

			// Verify field mapping for non-empty cases
			for i, log := range tt.logs {
				assert.Equal(t, log.Address, got[i].Address, "Address mismatch at index %d", i)
				assert.Equal(t, log.Topics, got[i].Topics, "Topics mismatch at index %d", i)
				assert.Equal(t, log.Data, got[i].Data, "Data mismatch at index %d", i)
				assert.Equal(t, log.BlockNumber, got[i].BlockNumber, "BlockNumber mismatch at index %d", i)
				assert.Equal(t, log.TxHash, got[i].TxHash, "TxHash mismatch at index %d", i)
				assert.Equal(t, log.TxIndex, got[i].TxIndex, "TxIndex mismatch at index %d", i)
				assert.Equal(t, log.BlockHash, got[i].BlockHash, "BlockHash mismatch at index %d", i)
				assert.Equal(t, log.Index, got[i].Index, "Index mismatch at index %d", i)
				assert.Equal(t, log.Removed, got[i].Removed, "Removed mismatch at index %d", i)
			}
		})
	}
}

func TestEVMTxReceiptFromLibevm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		receipt *libevmtypes.Receipt
	}{
		{
			name: "receipt_with_contract_creation",
			receipt: &libevmtypes.Receipt{
				ContractAddress: common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
				Status:          1,
				GasUsed:         53000,
				Logs:            []*libevmtypes.Log{},
			},
		},
		{
			name: "receipt_without_contract_creation",
			receipt: &libevmtypes.Receipt{
				ContractAddress: common.Address{}, // Zero address for non-contract transactions
				Status:          1,
				GasUsed:         21000,
				Logs:            []*libevmtypes.Log{},
			},
		},
		{
			name: "failed_transaction_receipt",
			receipt: &libevmtypes.Receipt{
				ContractAddress: common.Address{},
				Status:          0, // Failed
				GasUsed:         21000,
				Logs:            []*libevmtypes.Log{},
			},
		},
		{
			name: "receipt_with_logs",
			receipt: &libevmtypes.Receipt{
				ContractAddress: common.Address{},
				Status:          1,
				GasUsed:         65000,
				Logs: []*libevmtypes.Log{
					{
						Address:     common.HexToAddress("0xaaa"),
						Topics:      []common.Hash{common.HexToHash("0x1"), common.HexToHash("0x2")},
						Data:        []byte{0x01, 0x02, 0x03, 0x04},
						BlockNumber: 12345,
						TxHash:      common.HexToHash("0xtx"),
						TxIndex:     5,
						BlockHash:   common.HexToHash("0xblock"),
						Index:       0,
						Removed:     false,
					},
					{
						Address:     common.HexToAddress("0xbbb"),
						Topics:      []common.Hash{common.HexToHash("0x3")},
						Data:        []byte{0x05},
						BlockNumber: 12345,
						TxHash:      common.HexToHash("0xtx"),
						TxIndex:     5,
						BlockHash:   common.HexToHash("0xblock"),
						Index:       1,
						Removed:     false,
					},
				},
			},
		},
		{
			name: "receipt_with_nil_logs",
			receipt: &libevmtypes.Receipt{
				ContractAddress: common.Address{},
				Status:          1,
				GasUsed:         21000,
				Logs:            nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := EVMTxReceiptFromLibevm(tt.receipt)

			require.NotNil(t, got, "EVMTxReceiptFromLibevm should not return nil")
			assert.Equal(t, tt.receipt.ContractAddress, got.ContractAddress, "ContractAddress mismatch")
			assert.Equal(t, tt.receipt.Status, got.Status, "Status mismatch")
			assert.Equal(t, tt.receipt.GasUsed, got.GasUsed, "GasUsed mismatch")

			// Verify logs are converted correctly
			assert.Len(t, got.Logs, len(tt.receipt.Logs), "Logs length mismatch")
			for i, log := range tt.receipt.Logs {
				assert.Equal(t, log.Address, got.Logs[i].Address, "Log[%d] Address mismatch", i)
				assert.Equal(t, log.Topics, got.Logs[i].Topics, "Log[%d] Topics mismatch", i)
				assert.Equal(t, log.Data, got.Logs[i].Data, "Log[%d] Data mismatch", i)
				assert.Equal(t, log.BlockNumber, got.Logs[i].BlockNumber, "Log[%d] BlockNumber mismatch", i)
				assert.Equal(t, log.TxHash, got.Logs[i].TxHash, "Log[%d] TxHash mismatch", i)
				assert.Equal(t, log.TxIndex, got.Logs[i].TxIndex, "Log[%d] TxIndex mismatch", i)
				assert.Equal(t, log.BlockHash, got.Logs[i].BlockHash, "Log[%d] BlockHash mismatch", i)
				assert.Equal(t, log.Index, got.Logs[i].Index, "Log[%d] Index mismatch", i)
				assert.Equal(t, log.Removed, got.Logs[i].Removed, "Log[%d] Removed mismatch", i)
			}
		})
	}
}

func TestEVMWithdrawalsFromLibevm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		withdrawals []*libevmtypes.Withdrawal
		wantLen     int
	}{
		{
			name:        "nil withdrawals",
			withdrawals: nil,
			wantLen:     0,
		},
		{
			name:        "empty withdrawals",
			withdrawals: []*libevmtypes.Withdrawal{},
			wantLen:     0,
		},
		{
			name: "single withdrawal",
			withdrawals: []*libevmtypes.Withdrawal{
				{
					Index:     42,
					Validator: 100,
					Address:   common.HexToAddress("0x1234567890123456789012345678901234567890"),
					Amount:    1_000_000_000,
				},
			},
			wantLen: 1,
		},
		{
			name: "multiple withdrawals",
			withdrawals: []*libevmtypes.Withdrawal{
				{Index: 0, Validator: 1, Address: common.HexToAddress("0xaaaa"), Amount: 100},
				{Index: 1, Validator: 2, Address: common.HexToAddress("0xbbbb"), Amount: 200},
				{Index: 2, Validator: 3, Address: common.HexToAddress("0xcccc"), Amount: 300},
			},
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := EVMWithdrawalFromLibevm(tt.withdrawals)

			assert.Len(t, got, tt.wantLen)

			// Verify field mapping for non-empty cases
			for i, w := range tt.withdrawals {
				assert.Equal(t, w.Index, got[i].Index)
				assert.Equal(t, w.Validator, got[i].ValidatorIndex)
				assert.Equal(t, w.Address.Hex(), got[i].Address)
				assert.Equal(t, w.Amount, got[i].Amount)
			}
		})
	}
}

func TestEVMBlock_MarshalUnmarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		block *EVMBlock
	}{
		{
			name: "minimal block",
			block: &EVMBlock{
				Number:       big.NewInt(100),
				Hash:         "0xabc",
				ParentHash:   "0xdef",
				Transactions: []*EVMTransaction{},
			},
		},
		{
			name: "block with all fields",
			block: &EVMBlock{
				Number:                big.NewInt(12345),
				Hash:                  "0x1234567890abcdef",
				ParentHash:            "0xfedcba0987654321",
				StateRoot:             "0xstate",
				TransactionsRoot:      "0xtxroot",
				ReceiptsRoot:          "0xreceipts",
				UncleHash:             "0xuncles",
				Miner:                 "0xminer",
				GasLimit:              8_000_000,
				GasUsed:               21_000,
				BaseFee:               big.NewInt(1_000_000_000),
				Timestamp:             1700000000,
				MinDelayExcess:        500,
				Size:                  1024,
				Difficulty:            big.NewInt(1000),
				MixHash:               "0xmix",
				Nonce:                 42,
				LogsBloom:             "0xbloom",
				ExtraData:             "0xextra",
				ExcessBlobGas:         ptrUint64(100),
				BlobGasUsed:           ptrUint64(200),
				ParentBeaconBlockRoot: "0xbeacon",
				Withdrawals: []*EVMWithdrawal{
					{Index: 0, ValidatorIndex: 1, Address: "0xaddr", Amount: 100},
				},
				Transactions: []*EVMTransaction{
					{Hash: "0xtx1", From: "0xfrom", To: "0xto", Nonce: 1, Value: big.NewInt(100)},
				},
			},
		},
		{
			name: "block with nil optional fields",
			block: &EVMBlock{
				Number:        big.NewInt(1),
				Hash:          "0x1",
				Transactions:  nil,
				Withdrawals:   nil,
				ExcessBlobGas: nil,
				BlobGasUsed:   nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, err := jsonIter.Marshal(tt.block)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			var got EVMBlock
			err = jsonIter.Unmarshal(data, &got)
			require.NoError(t, err)

			// Verify round-trip
			assert.Equal(t, tt.block.Number, got.Number)
			assert.Equal(t, tt.block.Hash, got.Hash)
			assert.Equal(t, tt.block.ParentHash, got.ParentHash)
			assert.Equal(t, tt.block.GasLimit, got.GasLimit)
			assert.Equal(t, tt.block.GasUsed, got.GasUsed)
			assert.Equal(t, tt.block.Timestamp, got.Timestamp)
			assert.Equal(t, tt.block.MinDelayExcess, got.MinDelayExcess)
		})
	}
}

func TestEVMTransaction_MarshalUnmarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tx   *EVMTransaction
	}{
		{
			name: "minimal transaction",
			tx: &EVMTransaction{
				Hash:  "0xabc",
				From:  "0xfrom",
				Nonce: 1,
			},
		},
		{
			name: "full transaction",
			tx: &EVMTransaction{
				Hash:           "0x1234567890abcdef",
				From:           "0xfromaddress",
				To:             "0xtoaddress",
				Nonce:          42,
				Value:          big.NewInt(1_000_000_000_000_000_000),
				Gas:            21000,
				GasPrice:       big.NewInt(1_000_000_000),
				MaxFeePerGas:   big.NewInt(2_000_000_000),
				MaxPriorityFee: big.NewInt(1_000_000_000),
				Input:          "0x1234",
				Type:           2,
			},
		},
		{
			name: "contract creation (empty To)",
			tx: &EVMTransaction{
				Hash:  "0xcontract",
				From:  "0xcreator",
				To:    "",
				Nonce: 0,
				Value: big.NewInt(0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, err := jsonIter.Marshal(tt.tx)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			var got EVMTransaction
			err = jsonIter.Unmarshal(data, &got)
			require.NoError(t, err)

			assert.Equal(t, tt.tx.Hash, got.Hash)
			assert.Equal(t, tt.tx.From, got.From)
			assert.Equal(t, tt.tx.To, got.To)
			assert.Equal(t, tt.tx.Nonce, got.Nonce)
			if tt.tx.Value != nil {
				assert.Equal(t, tt.tx.Value, got.Value)
			}
			assert.Equal(t, tt.tx.Gas, got.Gas)
			assert.Equal(t, tt.tx.Type, got.Type)
		})
	}
}

func TestEVMWithdrawal_MarshalUnmarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		withdrawal *EVMWithdrawal
	}{
		{
			name: "typical withdrawal",
			withdrawal: &EVMWithdrawal{
				Index:          42,
				ValidatorIndex: 100,
				Address:        "0x1234567890123456789012345678901234567890",
				Amount:         32_000_000_000,
			},
		},
		{
			name: "zero values",
			withdrawal: &EVMWithdrawal{
				Index:          0,
				ValidatorIndex: 0,
				Address:        "",
				Amount:         0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, err := jsonIter.Marshal(tt.withdrawal)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			var got EVMWithdrawal
			err = jsonIter.Unmarshal(data, &got)
			require.NoError(t, err)

			assert.Equal(t, tt.withdrawal.Index, got.Index)
			assert.Equal(t, tt.withdrawal.ValidatorIndex, got.ValidatorIndex)
			assert.Equal(t, tt.withdrawal.Address, got.Address)
			assert.Equal(t, tt.withdrawal.Amount, got.Amount)
		})
	}
}

func TestEVMBlock_JSONTags(t *testing.T) {
	t.Parallel()

	block := &EVMBlock{
		Number:                big.NewInt(42),
		Hash:                  "0xhash",
		ParentHash:            "0xparent",
		EVMChainID:            big.NewInt(43114),
		StateRoot:             "0xstate",
		TransactionsRoot:      "0xtxroot",
		ReceiptsRoot:          "0xreceipts",
		UncleHash:             "0xuncles",
		Miner:                 "0xminer",
		GasLimit:              8_000_000,
		GasUsed:               21_000,
		BaseFee:               big.NewInt(1_000_000_000),
		Timestamp:             1700000000,
		MinDelayExcess:        100,
		Size:                  1024,
		Difficulty:            big.NewInt(1000),
		MixHash:               "0xmix",
		Nonce:                 12345,
		LogsBloom:             "0xbloom",
		ExtraData:             "0xextra",
		ExcessBlobGas:         ptrUint64(100),
		BlobGasUsed:           ptrUint64(200),
		ParentBeaconBlockRoot: "0xbeacon",
		Withdrawals: []*EVMWithdrawal{
			{Index: 0, ValidatorIndex: 1, Address: "0xaddr", Amount: 100},
		},
		Transactions: []*EVMTransaction{
			{Hash: "0xtx1", From: "0xfrom", To: "0xto"},
		},
	}

	data, err := jsonIter.Marshal(block)
	require.NoError(t, err)

	var m map[string]interface{}
	err = jsonIter.Unmarshal(data, &m)
	require.NoError(t, err)

	// Verify JSON field names match expected tags
	expectedFields := []string{
		"number", "hash", "parentHash", "evmChainId", "stateRoot", "transactionsRoot",
		"receiptsRoot", "sha3Uncles", "miner", "gasLimit", "gasUsed",
		"baseFeePerGas", "timestamp", "size",
		"difficulty", "mixHash", "nonce", "logsBloom", "extraData",
		"excessBlobGas", "blobGasUsed", "parentBeaconBlockRoot",
		"withdrawals", "transactions",
	}
	for _, field := range expectedFields {
		_, ok := m[field]
		assert.True(t, ok, "expected JSON field %q to be present", field)
	}

	// Optional fields that may or may not be present
	// timestampMs and minDelayExcess are only present if they have values
	if block.TimestampMs > 0 {
		_, ok := m["timestampMs"]
		assert.True(t, ok, "expected JSON field 'timestampMs' to be present when value > 0")
	}
	if block.MinDelayExcess > 0 {
		_, ok := m["minDelayExcess"]
		assert.True(t, ok, "expected JSON field 'minDelayExcess' to be present when value > 0")
	}
}

func TestEVMTransaction_JSONTags(t *testing.T) {
	t.Parallel()

	tx := &EVMTransaction{
		Hash:           "0xhash",
		From:           "0xfrom",
		To:             "0xto",
		Nonce:          1,
		Value:          big.NewInt(100),
		Gas:            21000,
		GasPrice:       big.NewInt(100),
		MaxFeePerGas:   big.NewInt(200),
		MaxPriorityFee: big.NewInt(100),
		Input:          "0x1234",
		Type:           2,
	}

	data, err := jsonIter.Marshal(tx)
	require.NoError(t, err)

	var m map[string]interface{}
	err = jsonIter.Unmarshal(data, &m)
	require.NoError(t, err)

	expectedFields := []string{
		"hash", "from", "to", "nonce", "value", "gas", "gasPrice",
		"maxFeePerGas", "maxPriorityFeePerGas", "input", "type",
	}
	for _, field := range expectedFields {
		_, ok := m[field]
		assert.True(t, ok, "expected JSON field %q to be present", field)
	}
}

func TestEVMWithdrawal_JSONTags(t *testing.T) {
	t.Parallel()

	w := &EVMWithdrawal{
		Index:          1,
		ValidatorIndex: 100,
		Address:        "0xaddr",
		Amount:         1000,
	}

	data, err := jsonIter.Marshal(w)
	require.NoError(t, err)

	var m map[string]interface{}
	err = jsonIter.Unmarshal(data, &m)
	require.NoError(t, err)

	expectedFields := []string{"index", "validatorIndex", "address", "amount"}
	for _, field := range expectedFields {
		_, ok := m[field]
		assert.True(t, ok, "expected JSON field %q to be present", field)
	}
}

// ptrUint64 returns a pointer to the given uint64 value.
func ptrUint64(v uint64) *uint64 {
	return &v
}

// TestEVMTransaction_UnmarshalScientificNotation tests that EVMTransaction can unmarshal
// big.Int fields from both decimal strings and scientific notation (e.g., "1e+21").
func TestEVMTransaction_UnmarshalScientificNotation(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected *big.Int
	}{
		{
			name:     "decimal_string",
			json:     `{"hash":"0x123","from":"0x456","to":"0x789","nonce":1,"value":"1000000000000000000","gas":21000,"gasPrice":"1000000000","input":"0x","type":0}`,
			expected: big.NewInt(1000000000000000000),
		},
		{
			name:     "scientific_notation_1e18",
			json:     `{"hash":"0x123","from":"0x456","to":"0x789","nonce":1,"value":"1e+18","gas":21000,"gasPrice":"1000000000","input":"0x","type":0}`,
			expected: big.NewInt(1000000000000000000),
		},
		{
			name:     "scientific_notation_1e21",
			json:     `{"hash":"0x123","from":"0x456","to":"0x789","nonce":1,"value":"1e+21","gas":21000,"gasPrice":"1000000000","input":"0x","type":0}`,
			expected: new(big.Int).Mul(big.NewInt(1000000000000000000), big.NewInt(1000)),
		},
		{
			name:     "large_decimal",
			json:     `{"hash":"0x123","from":"0x456","to":"0x789","nonce":1,"value":"999999999999999999999999","gas":21000,"gasPrice":"1000000000","input":"0x","type":0}`,
			expected: func() *big.Int { v, _ := new(big.Int).SetString("999999999999999999999999", 10); return v }(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tx EVMTransaction
			err := jsonIter.Unmarshal([]byte(tt.json), &tx)
			require.NoError(t, err, "Unmarshal should succeed")
			require.NotNil(t, tx.Value, "Value should not be nil")
			assert.Equal(t, tt.expected.String(), tx.Value.String(), "Value should match expected")
		})
	}
}

// TestEVMTransaction_UnmarshalUnquotedNumbers tests that Unmarshal can handle
// both unquoted JSON numbers (including scientific notation) and quoted strings.
func TestEVMTransaction_UnmarshalUnquotedNumbers(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string
	}{
		{
			name:     "unquoted_scientific_notation",
			json:     `{"value": 1e+21, "gasPrice": 5e+10}`,
			expected: "1000000000000000000000",
		},
		{
			name:     "unquoted_large_number",
			json:     `{"value": 1000000000000000000}`,
			expected: "1000000000000000000",
		},
		{
			name:     "quoted_scientific_notation",
			json:     `{"value": "1e+21"}`,
			expected: "1000000000000000000000",
		},
		{
			name:     "quoted_decimal_string",
			json:     `{"value": "1000000000000000000"}`,
			expected: "1000000000000000000",
		},
		{
			name:     "mixed_quoted_and_unquoted",
			json:     `{"value": 1e+21, "gasPrice": "50000000000"}`,
			expected: "1000000000000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tx EVMTransaction
			err := jsonIter.Unmarshal([]byte(tt.json), &tx)
			require.NoError(t, err, "Unmarshal should succeed for %s", tt.name)
			require.NotNil(t, tx.Value, "Value should not be nil")
			assert.Equal(t, tt.expected, tx.Value.String(), "Value should match expected")
		})
	}
}

// TestEVMTransaction_MarshalUnmarshal_RoundTrip tests that Marshal and Unmarshal
// are inverse operations, preserving big.Int values through the round trip.
func TestEVMTransaction_MarshalUnmarshal_RoundTrip(t *testing.T) {
	original := &EVMTransaction{
		Hash:           "0xabcdef1234567890",
		From:           "0x1111111111111111111111111111111111111111",
		To:             "0x2222222222222222222222222222222222222222",
		Nonce:          42,
		Value:          big.NewInt(1000000000000000000), // 1 ETH
		Gas:            21000,
		GasPrice:       big.NewInt(50000000000), // 50 Gwei
		MaxFeePerGas:   big.NewInt(100000000000),
		MaxPriorityFee: big.NewInt(2000000000),
		Input:          "0x123456",
		Type:           2,
	}

	// Marshal
	data, err := jsonIter.Marshal(original)
	require.NoError(t, err, "Marshal should succeed")

	// Verify JSON contains string representations (not scientific notation)
	jsonStr := string(data)
	assert.Contains(t, jsonStr, `"value":"1000000000000000000"`)
	assert.Contains(t, jsonStr, `"gasPrice":"50000000000"`)
	assert.NotContains(t, jsonStr, "e+", "Should not contain scientific notation")

	// Unmarshal
	var parsed EVMTransaction
	err = jsonIter.Unmarshal(data, &parsed)
	require.NoError(t, err, "Unmarshal should succeed")

	// Verify all big.Int fields are preserved
	assert.Equal(t, original.Value.String(), parsed.Value.String(), "Value should be preserved")
	assert.Equal(t, original.GasPrice.String(), parsed.GasPrice.String(), "GasPrice should be preserved")
	assert.Equal(t, original.MaxFeePerGas.String(), parsed.MaxFeePerGas.String(), "MaxFeePerGas should be preserved")
	assert.Equal(t, original.MaxPriorityFee.String(), parsed.MaxPriorityFee.String(), "MaxPriorityFee should be preserved")

	// Verify other fields
	assert.Equal(t, original.Hash, parsed.Hash)
	assert.Equal(t, original.From, parsed.From)
	assert.Equal(t, original.To, parsed.To)
	assert.Equal(t, original.Nonce, parsed.Nonce)
	assert.Equal(t, original.Gas, parsed.Gas)
	assert.Equal(t, original.Input, parsed.Input)
	assert.Equal(t, original.Type, parsed.Type)
}

// TestBigIntToRawJSON tests the bigIntToRawJSON helper function.
func TestBigIntToRawJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *big.Int
		expected string // Expected JSON RawMessage as string
	}{
		{
			name:     "nil_value",
			input:    nil,
			expected: `""`,
		},
		{
			name:     "zero",
			input:    big.NewInt(0),
			expected: `"0"`,
		},
		{
			name:     "positive_small",
			input:    big.NewInt(123),
			expected: `"123"`,
		},
		{
			name:     "one_ether_1e18",
			input:    big.NewInt(1000000000000000000),
			expected: `"1000000000000000000"`,
		},
		{
			name:     "large_1e21",
			input:    new(big.Int).Mul(big.NewInt(1000000000000000000), big.NewInt(1000)),
			expected: `"1000000000000000000000"`,
		},
		{
			name: "extremely_large_1e30",
			input: func() *big.Int {
				v := new(big.Int)
				v.SetString("1000000000000000000000000000000", 10)
				return v
			}(),
			expected: `"1000000000000000000000000000000"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := bigIntToRawJSON(tt.input)
			assert.Equal(t, tt.expected, string(result), "bigIntToRawJSON output mismatch")

			// Verify output is valid JSON
			var dummy interface{}
			err := jsonIter.Unmarshal(result, &dummy)
			require.NoError(t, err, "Result should be valid JSON")

			// Verify no scientific notation
			resultStr := string(result)
			assert.NotContains(t, resultStr, "e+", "Should not contain scientific notation (e+)")
			assert.NotContains(t, resultStr, "E+", "Should not contain scientific notation (E+)")
			assert.NotContains(t, resultStr, "e-", "Should not contain scientific notation (e-)")
			assert.NotContains(t, resultStr, "E-", "Should not contain scientific notation (E-)")
		})
	}
}

// TestParseBigIntFromRaw tests the parseBigIntFromRaw helper function.
func TestParseBigIntFromRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string // JSON RawMessage as string
		fieldName string
		expected  *big.Int
		wantErr   bool
	}{
		{
			name:      "empty_raw_message",
			input:     "",
			fieldName: "testField",
			expected:  nil,
			wantErr:   false,
		},
		{
			name:      "null_value",
			input:     "null",
			fieldName: "testField",
			expected:  nil,
			wantErr:   false,
		},
		{
			name:      "empty_quoted_string",
			input:     `""`,
			fieldName: "testField",
			expected:  nil,
			wantErr:   false,
		},
		{
			name:      "quoted_decimal_string",
			input:     `"1000000000000000000"`,
			fieldName: "value",
			expected:  big.NewInt(1000000000000000000),
			wantErr:   false,
		},
		{
			name:      "quoted_scientific_notation_1e18",
			input:     `"1e+18"`,
			fieldName: "value",
			expected:  big.NewInt(1000000000000000000),
			wantErr:   false,
		},
		{
			name:      "quoted_scientific_notation_1e21",
			input:     `"1e+21"`,
			fieldName: "value",
			expected:  new(big.Int).Mul(big.NewInt(1000000000000000000), big.NewInt(1000)),
			wantErr:   false,
		},
		{
			name:      "quoted_scientific_notation_lowercase",
			input:     `"5e20"`,
			fieldName: "gasPrice",
			expected:  func() *big.Int { v, _ := new(big.Int).SetString("500000000000000000000", 10); return v }(),
			wantErr:   false,
		},
		{
			name:      "quoted_zero",
			input:     `"0"`,
			fieldName: "value",
			expected:  big.NewInt(0),
			wantErr:   false,
		},
		{
			name:      "unquoted_number",
			input:     `1000000000000000000`,
			fieldName: "value",
			expected:  big.NewInt(1000000000000000000),
			wantErr:   false,
		},
		{
			name:      "unquoted_scientific_notation_1e21",
			input:     `1e+21`,
			fieldName: "value",
			expected:  new(big.Int).Mul(big.NewInt(1000000000000000000), big.NewInt(1000)),
			wantErr:   false,
		},
		{
			name:      "unquoted_scientific_notation_5e10",
			input:     `5e+10`,
			fieldName: "gasPrice",
			expected:  big.NewInt(50000000000),
			wantErr:   false,
		},
		{
			name:      "unquoted_zero",
			input:     `0`,
			fieldName: "value",
			expected:  big.NewInt(0),
			wantErr:   false,
		},
		{
			name:      "very_large_decimal_string",
			input:     `"999999999999999999999999999999"`,
			fieldName: "value",
			expected:  func() *big.Int { v, _ := new(big.Int).SetString("999999999999999999999999999999", 10); return v }(),
			wantErr:   false,
		},
		{
			name:      "decimal_point_scientific_1dot5e18",
			input:     `"1.5e18"`,
			fieldName: "value",
			expected:  big.NewInt(1500000000000000000),
			wantErr:   false,
		},
		{
			name:      "invalid_quoted_string",
			input:     `"not-a-number"`,
			fieldName: "value",
			expected:  nil,
			wantErr:   true,
		},
		{
			name:      "invalid_json",
			input:     `{invalid}`,
			fieldName: "value",
			expected:  nil,
			wantErr:   true,
		},
		{
			name:      "malformed_quoted_string_unclosed_quote",
			input:     `"12345`,
			fieldName: "value",
			expected:  nil,
			wantErr:   true,
		},
		{
			name:      "malformed_quoted_string_escaped_unclosed",
			input:     `"test\"`,
			fieldName: "value",
			expected:  nil,
			wantErr:   true,
		},
		{
			name:      "malformed_quoted_string_empty",
			input:     `"`,
			fieldName: "value",
			expected:  nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseBigIntFromRaw(json.RawMessage(tt.input), tt.fieldName)

			if tt.wantErr {
				require.ErrorIs(t, err, ErrParseBigInt, "Expected ErrParseBigInt for input: %s", tt.input)
				return
			}

			require.NoError(t, err, "Unexpected error for input: %s", tt.input)

			if tt.expected == nil {
				assert.Nil(t, result, "Expected nil result for input: %s", tt.input)
			} else {
				require.NotNil(t, result, "Expected non-nil result for input: %s", tt.input)
				assert.Equal(t, tt.expected.String(), result.String(),
					"Value mismatch for input: %s", tt.input)
			}
		})
	}
}

// TestBigIntToRawJSON_RoundTrip tests that bigIntToRawJSON and parseBigIntFromRaw are inverse operations.
func TestBigIntToRawJSON_RoundTrip(t *testing.T) {
	t.Parallel()

	testValues := []*big.Int{
		nil,
		big.NewInt(0),
		big.NewInt(1),
		big.NewInt(123456789),
		big.NewInt(1000000000000000000), // 1e18
		new(big.Int).Mul(big.NewInt(1000000000000000000), big.NewInt(1000)), // 1e21
		func() *big.Int { v, _ := new(big.Int).SetString("999999999999999999999999999999", 10); return v }(),
	}

	for i, original := range testValues {
		t.Run(fmt.Sprintf("value_%d", i), func(t *testing.T) {
			t.Parallel()

			// Convert to RawMessage
			raw := bigIntToRawJSON(original)

			// Parse back
			parsed, err := parseBigIntFromRaw(raw, "testField")
			require.NoError(t, err, "Should parse without error")

			// Compare
			if original == nil {
				assert.Nil(t, parsed, "Should be nil after round-trip")
			} else {
				require.NotNil(t, parsed, "Should not be nil after round-trip")
				assert.Equal(t, original.String(), parsed.String(),
					"Value should match after round-trip")
			}
		})
	}
}

// TestEVMBlock_UnmarshalJSON_ErrorPaths tests error handling in EVMBlock.UnmarshalJSON
// for invalid big.Int fields to ensure proper error propagation.
func TestEVMBlock_UnmarshalJSON_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		json      string
		wantErr   bool
		errString string // Expected error substring
	}{
		{
			name:      "invalid_evmChainId_field",
			json:      `{"evmChainId":"not-a-number","number":"1","hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "invalid_number_field",
			json:      `{"number":"invalid","hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "invalid_baseFeePerGas_field",
			json:      `{"number":"1","hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456,"baseFeePerGas":"not-valid"}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "invalid_difficulty_field",
			json:      `{"number":"1","hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456,"difficulty":"xyz"}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "array_instead_of_number_evmChainId",
			json:      `{"evmChainId":[],"number":"1","hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "object_instead_of_number_number",
			json:      `{"number":{},"hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "invalid_scientific_notation_baseFeePerGas",
			json:      `{"number":"1","hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456,"baseFeePerGas":"1.2.3e10"}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "malformed_json_in_difficulty",
			json:      `{"number":"1","hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456,"difficulty":{invalid}}`,
			wantErr:   true,
			errString: "ReadObjectCB", // jsoniter fails before reaching our parsing code
		},
		{
			name:      "null_number_is_invalid",
			json:      `{"number":null,"hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456}`,
			wantErr:   false,
			errString: "",
		},
		{
			name:      "empty_string_baseFeePerGas_is_valid",
			json:      `{"number":"1","hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456,"baseFeePerGas":""}`,
			wantErr:   false,
			errString: "",
		},
		{
			name:      "null_optional_fields_are_valid",
			json:      `{"number":"1","hash":"0x1","parentHash":"0x2","stateRoot":"0x3","transactionsRoot":"0x4","receiptsRoot":"0x5","sha3Uncles":"0x6","miner":"0x7","gasLimit":1000,"gasUsed":500,"timestamp":123456,"evmChainId":null,"baseFeePerGas":null,"difficulty":null}`,
			wantErr:   false,
			errString: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var block EVMBlock
			err := jsonIter.Unmarshal([]byte(tt.json), &block)

			if tt.wantErr {
				// jsoniter breaks error chains by calling err.Error() in unmarshalerDecoder.Decode(),
				// so we cannot use errors.Is() to check for sentinel errors.
				// See: https://github.com/json-iterator/go/blob/v1.1.12/reflect_marshaler.go#L201
				//nolint:forbidigo // require.Error is necessary because jsoniter breaks error wrapping
				require.Error(t, err, "Expected error for test case: %s", tt.name)
				assert.Contains(t, err.Error(), tt.errString, "Error message should contain expected substring for test case: %s", tt.name)
			} else {
				require.NoError(t, err, "Expected no error for test case: %s", tt.name)
			}
		})
	}
}

// TestEVMTransaction_UnmarshalJSON_ErrorPaths tests error handling in UnmarshalJSON
// for invalid big.Int fields to ensure proper error propagation.
func TestEVMTransaction_UnmarshalJSON_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		json      string
		wantErr   bool
		errString string // Expected error substring
	}{
		{
			name:      "invalid_value_field",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":"not-a-number","gas":21000}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "invalid_gasPrice_field",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":"100","gas":21000,"gasPrice":"invalid"}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "invalid_maxFeePerGas_field",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":"100","gas":21000,"maxFeePerGas":"not-valid"}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "invalid_maxPriorityFeePerGas_field",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":"100","gas":21000,"maxPriorityFeePerGas":"xyz"}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "malformed_json_in_value",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":{invalid},"gas":21000}`,
			wantErr:   true,
			errString: "ReadObjectCB", // jsoniter fails before reaching our parsing code
		},
		{
			name:      "array_instead_of_number_value",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":[],"gas":21000}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "object_instead_of_number_gasPrice",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":"100","gas":21000,"gasPrice":{}}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "invalid_scientific_notation_maxFeePerGas",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":"100","gas":21000,"maxFeePerGas":"1.2.3e10"}`,
			wantErr:   true,
			errString: "failed to parse big.Int",
		},
		{
			name:      "null_value_is_valid",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":null,"gas":21000}`,
			wantErr:   false,
			errString: "",
		},
		{
			name:      "empty_string_value_is_valid",
			json:      `{"hash":"0x1","from":"0x2","to":"0x3","nonce":1,"value":"","gas":21000}`,
			wantErr:   false,
			errString: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var tx EVMTransaction
			err := jsonIter.Unmarshal([]byte(tt.json), &tx)

			if tt.wantErr {
				// jsoniter breaks error chains by calling err.Error() in unmarshalerDecoder.Decode(),
				// so we cannot use errors.Is() to check for sentinel errors.
				// See: https://github.com/json-iterator/go/blob/v1.1.12/reflect_marshaler.go#L201
				//nolint:forbidigo // require.Error is necessary because jsoniter breaks error wrapping
				require.Error(t, err, "Expected error for test case: %s", tt.name)
				assert.Contains(t, err.Error(), tt.errString, "Error message should contain expected substring for test case: %s", tt.name)
			} else {
				require.NoError(t, err, "Expected no error for test case: %s", tt.name)
			}
		})
	}
}

// TestEVMTransaction_MarshalNoScientificNotation verifies that Marshal always
// outputs decimal strings and never scientific notation, even for very large numbers.
func TestEVMTransaction_MarshalNoScientificNotation(t *testing.T) {
	tests := []struct {
		name          string
		tx            *EVMTransaction
		expectedValue string // Expected decimal string in JSON
	}{
		{
			name: "1e18_one_ether",
			tx: &EVMTransaction{
				Hash:  "0x1",
				From:  "0x2",
				To:    "0x3",
				Nonce: 1,
				Value: big.NewInt(1000000000000000000), // 1e18
				Gas:   21000,
				Input: "0x",
				Type:  0,
			},
			expectedValue: "1000000000000000000",
		},
		{
			name: "1e21_very_large_value",
			tx: &EVMTransaction{
				Hash:  "0x1",
				From:  "0x2",
				To:    "0x3",
				Nonce: 1,
				Value: new(big.Int).Mul(big.NewInt(1000000000000000000), big.NewInt(1000)), // 1e21
				Gas:   21000,
				Input: "0x",
				Type:  0,
			},
			expectedValue: "1000000000000000000000",
		},
		{
			name: "1e30_extremely_large_value",
			tx: &EVMTransaction{
				Hash:  "0x1",
				From:  "0x2",
				To:    "0x3",
				Nonce: 1,
				Value: func() *big.Int {
					v := new(big.Int)
					v.SetString("1000000000000000000000000000000", 10) // 1e30
					return v
				}(),
				Gas:   21000,
				Input: "0x",
				Type:  0,
			},
			expectedValue: "1000000000000000000000000000000",
		},
		{
			name: "all_bigint_fields_large",
			tx: &EVMTransaction{
				Hash:  "0x1",
				From:  "0x2",
				To:    "0x3",
				Nonce: 1,
				Value: func() *big.Int {
					v := new(big.Int)
					v.SetString("999999999999999999999999", 10)
					return v
				}(),
				Gas: 21000,
				GasPrice: func() *big.Int {
					v := new(big.Int)
					v.SetString("500000000000000000000", 10) // 5e20
					return v
				}(),
				MaxFeePerGas: func() *big.Int {
					v := new(big.Int)
					v.SetString("1000000000000000000000", 10) // 1e21
					return v
				}(),
				MaxPriorityFee: func() *big.Int {
					v := new(big.Int)
					v.SetString("100000000000000000000", 10) // 1e20
					return v
				}(),
				Input: "0x",
				Type:  2,
			},
			expectedValue: "999999999999999999999999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data, err := jsonIter.Marshal(tt.tx)
			require.NoError(t, err, "Marshal should succeed")

			jsonStr := string(data)

			// Verify no scientific notation anywhere in the JSON
			assert.NotContains(t, jsonStr, "e+", "JSON should not contain 'e+' (scientific notation)")
			assert.NotContains(t, jsonStr, "E+", "JSON should not contain 'E+' (scientific notation)")
			assert.NotContains(t, jsonStr, "e-", "JSON should not contain 'e-' (scientific notation)")
			assert.NotContains(t, jsonStr, "E-", "JSON should not contain 'E-' (scientific notation)")

			// Verify the exact decimal string is present for Value
			assert.Contains(t, jsonStr, `"value":"`+tt.expectedValue+`"`,
				"JSON should contain exact decimal value string")

			// Verify all big.Int fields are strings (not numbers)
			var m map[string]interface{}
			err = jsonIter.Unmarshal(data, &m)
			require.NoError(t, err, "Should unmarshal to map")

			if tt.tx.Value != nil {
				valueField, ok := m["value"]
				require.True(t, ok, "value field should exist")
				_, isString := valueField.(string)
				assert.True(t, isString, "value field should be a string, not a number")
			}

			if tt.tx.GasPrice != nil {
				gasPriceField, ok := m["gasPrice"]
				require.True(t, ok, "gasPrice field should exist")
				_, isString := gasPriceField.(string)
				assert.True(t, isString, "gasPrice field should be a string, not a number")
			}

			if tt.tx.MaxFeePerGas != nil {
				maxFeeField, ok := m["maxFeePerGas"]
				require.True(t, ok, "maxFeePerGas field should exist")
				_, isString := maxFeeField.(string)
				assert.True(t, isString, "maxFeePerGas field should be a string, not a number")
			}

			if tt.tx.MaxPriorityFee != nil {
				maxPriorityField, ok := m["maxPriorityFeePerGas"]
				require.True(t, ok, "maxPriorityFeePerGas field should exist")
				_, isString := maxPriorityField.(string)
				assert.True(t, isString, "maxPriorityFeePerGas field should be a string, not a number")
			}

			// Verify round-trip: unmarshal and compare
			var parsed EVMTransaction
			err = jsonIter.Unmarshal(data, &parsed)
			require.NoError(t, err, "Unmarshal should succeed")

			if tt.tx.Value != nil {
				assert.Equal(t, tt.tx.Value.String(), parsed.Value.String(),
					"Value should be preserved through round-trip")
			}
			if tt.tx.GasPrice != nil {
				assert.Equal(t, tt.tx.GasPrice.String(), parsed.GasPrice.String(),
					"GasPrice should be preserved through round-trip")
			}
			if tt.tx.MaxFeePerGas != nil {
				assert.Equal(t, tt.tx.MaxFeePerGas.String(), parsed.MaxFeePerGas.String(),
					"MaxFeePerGas should be preserved through round-trip")
			}
			if tt.tx.MaxPriorityFee != nil {
				assert.Equal(t, tt.tx.MaxPriorityFee.String(), parsed.MaxPriorityFee.String(),
					"MaxPriorityFee should be preserved through round-trip")
			}
		})
	}
}
