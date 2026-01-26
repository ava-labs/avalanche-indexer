package messages

import (
	"encoding/json"
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
// Must be called before any test that uses BlockFromLibevm.
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

func TestCorethBlockFromLibevm(t *testing.T) {
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
		assertFn    func(t *testing.T, got *CorethBlock)
	}{
		{
			name:        "minimal block without optional fields",
			header:      newTestHeader(),
			txs:         nil,
			withdrawals: nil,
			chainID:     chainID,
			assertFn: func(t *testing.T, got *CorethBlock) {
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
			assertFn: func(t *testing.T, got *CorethBlock) {
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
			assertFn: func(t *testing.T, got *CorethBlock) {
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
			assertFn: func(t *testing.T, got *CorethBlock) {
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
			assertFn: func(t *testing.T, got *CorethBlock) {
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
			assertFn: func(t *testing.T, got *CorethBlock) {
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
			assertFn: func(t *testing.T, got *CorethBlock) {
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
			got, err := CorethBlockFromLibevm(block, tt.chainID, nil)

			require.NoError(t, err)
			require.NotNil(t, got)
			tt.assertFn(t, got)
		})
	}
}

func TestCorethBlockFromLibevm_HeaderFields(t *testing.T) {
	initCustomTypes()
	t.Parallel()

	chainID := big.NewInt(43114)
	header := newTestHeader()
	block := libevmtypes.NewBlock(header, nil, nil, nil, newTestHasher())

	got, err := CorethBlockFromLibevm(block, chainID, nil /* optional parameter not needed for this test */)

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

func TestCorethTransactionsFromLibevm(t *testing.T) {
	t.Parallel()

	chainID := big.NewInt(43114)
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")

	tests := []struct {
		name     string
		txs      []*libevmtypes.Transaction
		wantLen  int
		assertFn func(t *testing.T, got []*CorethTransaction)
	}{
		{
			name:    "empty transactions",
			txs:     nil,
			wantLen: 0,
			assertFn: func(t *testing.T, got []*CorethTransaction) {
				assert.Empty(t, got)
			},
		},
		{
			name:    "single transaction with To address",
			txs:     []*libevmtypes.Transaction{newSignedTx(chainID, 1, &toAddr)},
			wantLen: 1,
			assertFn: func(t *testing.T, got []*CorethTransaction) {
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
			assertFn: func(t *testing.T, got []*CorethTransaction) {
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
			assertFn: func(t *testing.T, got []*CorethTransaction) {
				assert.Len(t, got, 3)
				// Each tx should have a unique hash
				hashes := make(map[string]struct{})
				for _, tx := range got {
					hashes[tx.Hash] = struct{}{}
				}
				assert.Len(t, hashes, 3)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CorethTransactionsFromLibevm(tt.txs)

			require.NoError(t, err)
			assert.Len(t, got, tt.wantLen)
			tt.assertFn(t, got)
		})
	}
}

func TestCorethTxReceiptFromLibevm(t *testing.T) {
	t.Parallel()

	logs := []*libevmtypes.Log{
		{
			Address:     common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Topics:      []common.Hash{common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
			Data:        []byte{0xde, 0xad, 0xbe, 0xef},
			BlockNumber: 123,
			TxHash:      common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			TxIndex:     2,
			BlockHash:   common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
			Index:       7,
			Removed:     false,
		},
	}

	receipt := &libevmtypes.Receipt{
		ContractAddress: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Status:          1,
		GasUsed:         21000,
		Logs:            logs,
	}

	got := CorethTxReceiptFromLibevm(receipt)

	require.NotNil(t, got)
	assert.Equal(t, receipt.ContractAddress, got.ContractAddress)
	assert.Equal(t, receipt.Status, got.Status)
	assert.Equal(t, receipt.GasUsed, got.GasUsed)
	require.Len(t, got.Logs, 1)
	assert.Equal(t, logs[0].Address, got.Logs[0].Address)
	assert.Equal(t, logs[0].Topics, got.Logs[0].Topics)
	assert.Equal(t, logs[0].Data, got.Logs[0].Data)
	assert.Equal(t, logs[0].BlockNumber, got.Logs[0].BlockNumber)
	assert.Equal(t, logs[0].TxHash, got.Logs[0].TxHash)
	assert.Equal(t, logs[0].TxIndex, got.Logs[0].TxIndex)
	assert.Equal(t, logs[0].BlockHash, got.Logs[0].BlockHash)
	assert.Equal(t, logs[0].Index, got.Logs[0].Index)
	assert.Equal(t, logs[0].Removed, got.Logs[0].Removed)
}

func TestCorethTxReceiptFromLibevm_NilInput(t *testing.T) {
	t.Parallel()

	got := CorethTxReceiptFromLibevm(nil)

	assert.Nil(t, got, "nil receipt input should return nil")
}

func TestCorethTxReceiptFromLibevm_NilLogs(t *testing.T) {
	t.Parallel()

	receipt := &libevmtypes.Receipt{
		ContractAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Status:          1,
		GasUsed:         21000,
		Logs:            nil,
	}

	got := CorethTxReceiptFromLibevm(receipt)

	require.NotNil(t, got)
	assert.Nil(t, got.Logs, "nil logs should return nil logs slice")
	assert.Equal(t, uint64(1), got.Status)
	assert.Equal(t, uint64(21000), got.GasUsed)
}

func TestCorethLogsFromLibevm(t *testing.T) {
	t.Parallel()

	logs := []*libevmtypes.Log{
		{
			Address:     common.HexToAddress("0x3333333333333333333333333333333333333333"),
			Topics:      []common.Hash{common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")},
			Data:        []byte{0x01, 0x02},
			BlockNumber: 456,
			TxHash:      common.HexToHash("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
			TxIndex:     1,
			BlockHash:   common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
			Index:       3,
			Removed:     true,
		},
	}

	got := CorethLogsFromLibevm(logs)

	require.Len(t, got, 1)
	assert.Equal(t, logs[0].Address, got[0].Address)
	assert.Equal(t, logs[0].Topics, got[0].Topics)
	assert.Equal(t, logs[0].Data, got[0].Data)
	assert.Equal(t, logs[0].BlockNumber, got[0].BlockNumber)
	assert.Equal(t, logs[0].TxHash, got[0].TxHash)
	assert.Equal(t, logs[0].TxIndex, got[0].TxIndex)
	assert.Equal(t, logs[0].BlockHash, got[0].BlockHash)
	assert.Equal(t, logs[0].Index, got[0].Index)
	assert.Equal(t, logs[0].Removed, got[0].Removed)
}

func TestCorethLogsFromLibevm_NilAndEmpty(t *testing.T) {
	t.Parallel()

	t.Run("nil logs", func(t *testing.T) {
		t.Parallel()
		got := CorethLogsFromLibevm(nil)
		assert.Nil(t, got, "nil input should return nil")
	})

	t.Run("empty logs", func(t *testing.T) {
		t.Parallel()
		got := CorethLogsFromLibevm([]*libevmtypes.Log{})
		require.NotNil(t, got, "empty slice should return empty slice, not nil")
		assert.Empty(t, got)
	})

	t.Run("slice with nil element", func(t *testing.T) {
		t.Parallel()
		logs := []*libevmtypes.Log{
			{
				Address: common.HexToAddress("0x1111111111111111111111111111111111111111"),
				Topics:  []common.Hash{},
			},
			nil, // nil element should be skipped
			{
				Address: common.HexToAddress("0x2222222222222222222222222222222222222222"),
				Topics:  []common.Hash{},
			},
		}
		got := CorethLogsFromLibevm(logs)
		require.Len(t, got, 2, "nil elements should be skipped")
		assert.Equal(t, logs[0].Address, got[0].Address)
		assert.Equal(t, logs[2].Address, got[1].Address)
	})
}

func TestCorethWithdrawalsFromLibevm(t *testing.T) {
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

			got := CorethWithdrawalFromLibevm(tt.withdrawals)

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

func TestCorethBlock_MarshalUnmarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		block *CorethBlock
	}{
		{
			name: "minimal block",
			block: &CorethBlock{
				Number:       big.NewInt(100),
				Hash:         "0xabc",
				ParentHash:   "0xdef",
				Transactions: []*CorethTransaction{},
			},
		},
		{
			name: "block with all fields",
			block: &CorethBlock{
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
				Withdrawals: []*CorethWithdrawal{
					{Index: 0, ValidatorIndex: 1, Address: "0xaddr", Amount: 100},
				},
				Transactions: []*CorethTransaction{
					{Hash: "0xtx1", From: "0xfrom", To: "0xto", Nonce: 1, Value: big.NewInt(100)},
				},
			},
		},
		{
			name: "block with nil optional fields",
			block: &CorethBlock{
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

			data, err := tt.block.Marshal()
			require.NoError(t, err)
			require.NotEmpty(t, data)

			var got CorethBlock
			err = got.Unmarshal(data)
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

func TestCorethTransaction_MarshalUnmarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tx   *CorethTransaction
	}{
		{
			name: "minimal transaction",
			tx: &CorethTransaction{
				Hash:  "0xabc",
				From:  "0xfrom",
				Nonce: 1,
			},
		},
		{
			name: "full transaction",
			tx: &CorethTransaction{
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
			tx: &CorethTransaction{
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

			data, err := tt.tx.Marshal()
			require.NoError(t, err)
			require.NotEmpty(t, data)

			var got CorethTransaction
			err = got.Unmarshal(data)
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

func TestCorethWithdrawal_MarshalUnmarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		withdrawal *CorethWithdrawal
	}{
		{
			name: "typical withdrawal",
			withdrawal: &CorethWithdrawal{
				Index:          42,
				ValidatorIndex: 100,
				Address:        "0x1234567890123456789012345678901234567890",
				Amount:         32_000_000_000,
			},
		},
		{
			name: "zero values",
			withdrawal: &CorethWithdrawal{
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

			data, err := tt.withdrawal.Marshal()
			require.NoError(t, err)
			require.NotEmpty(t, data)

			var got CorethWithdrawal
			err = got.Unmarshal(data)
			require.NoError(t, err)

			assert.Equal(t, tt.withdrawal.Index, got.Index)
			assert.Equal(t, tt.withdrawal.ValidatorIndex, got.ValidatorIndex)
			assert.Equal(t, tt.withdrawal.Address, got.Address)
			assert.Equal(t, tt.withdrawal.Amount, got.Amount)
		})
	}
}

func TestCorethBlock_JSONTags(t *testing.T) {
	t.Parallel()

	block := &CorethBlock{
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
		Withdrawals: []*CorethWithdrawal{
			{Index: 0, ValidatorIndex: 1, Address: "0xaddr", Amount: 100},
		},
		Transactions: []*CorethTransaction{
			{Hash: "0xtx1", From: "0xfrom", To: "0xto"},
		},
	}

	data, err := json.Marshal(block)
	require.NoError(t, err)

	var m map[string]interface{}
	err = json.Unmarshal(data, &m)
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

func TestCorethTransaction_JSONTags(t *testing.T) {
	t.Parallel()

	tx := &CorethTransaction{
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

	data, err := json.Marshal(tx)
	require.NoError(t, err)

	var m map[string]interface{}
	err = json.Unmarshal(data, &m)
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

func TestCorethTxReceipt_JSONTags(t *testing.T) {
	t.Parallel()

	r := &CorethTxReceipt{
		ContractAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Status:          1,
		GasUsed:         21000,
		Logs: []*CorethLog{
			{
				Address:     common.HexToAddress("0x2222222222222222222222222222222222222222"),
				Topics:      []common.Hash{common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
				Data:        []byte{0x01},
				BlockNumber: 1,
				TxHash:      common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
				TxIndex:     0,
				BlockHash:   common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
				Index:       0,
				Removed:     false,
			},
		},
	}

	data, err := json.Marshal(r)
	require.NoError(t, err)

	var m map[string]interface{}
	err = json.Unmarshal(data, &m)
	require.NoError(t, err)

	expectedFields := []string{"contractAddress", "status", "gasUsed", "logs"}
	for _, field := range expectedFields {
		_, ok := m[field]
		assert.True(t, ok, "expected JSON field %q to be present", field)
	}
}

func TestCorethLog_JSONTags(t *testing.T) {
	t.Parallel()

	l := &CorethLog{
		Address:     common.HexToAddress("0x3333333333333333333333333333333333333333"),
		Topics:      []common.Hash{common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")},
		Data:        []byte{0x02},
		BlockNumber: 2,
		TxHash:      common.HexToHash("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
		TxIndex:     1,
		BlockHash:   common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
		Index:       3,
		Removed:     true,
	}

	data, err := json.Marshal(l)
	require.NoError(t, err)

	var m map[string]interface{}
	err = json.Unmarshal(data, &m)
	require.NoError(t, err)

	expectedFields := []string{
		"address", "topics", "data", "blockNumber", "txHash", "txIndex", "blockHash", "index", "removed",
	}
	for _, field := range expectedFields {
		_, ok := m[field]
		assert.True(t, ok, "expected JSON field %q to be present", field)
	}
}

func TestCorethWithdrawal_JSONTags(t *testing.T) {
	t.Parallel()

	w := &CorethWithdrawal{
		Index:          1,
		ValidatorIndex: 100,
		Address:        "0xaddr",
		Amount:         1000,
	}

	data, err := json.Marshal(w)
	require.NoError(t, err)

	var m map[string]interface{}
	err = json.Unmarshal(data, &m)
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
