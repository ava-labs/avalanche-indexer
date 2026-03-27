package evmrepo

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPadBlockNumber(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0000000000000000"},
		{1, "0000000000000001"},
		{255, "00000000000000ff"},
		{19000000, "000000000121eac0"},
		{18446744073709551615, "ffffffffffffffff"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, PadBlockNumber(tt.input))
	}
}

func TestPadTxIndex(t *testing.T) {
	tests := []struct {
		input    uint
		expected string
	}{
		{0, "00000000"},
		{1, "00000001"},
		{255, "000000ff"},
		{65535, "0000ffff"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, PadTxIndex(tt.input))
	}
}

func TestPadLogIndex(t *testing.T) {
	assert.Equal(t, "00000000", PadLogIndex(0))
	assert.Equal(t, "0000000a", PadLogIndex(10))
}

func TestPadTransferIndex(t *testing.T) {
	assert.Equal(t, "00000000", PadTransferIndex(0))
	assert.Equal(t, "00000005", PadTransferIndex(5))
}

func TestPadTokenID(t *testing.T) {
	tests := []struct {
		name     string
		input    *big.Int
		expected string
	}{
		{"nil", nil, "0000000000000000000000000000000000000000000000000000000000000000"},
		{"zero", big.NewInt(0), "0000000000000000000000000000000000000000000000000000000000000000"},
		{"small", big.NewInt(42), "000000000000000000000000000000000000000000000000000000000000002a"},
		{"large", new(big.Int).SetBytes([]byte{0xff, 0xff}), "000000000000000000000000000000000000000000000000000000000000ffff"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PadTokenID(tt.input)
			assert.Equal(t, 64, len(result))
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlockPK(t *testing.T) {
	assert.Equal(t, "B#0xabc123", BlockPK("0xabc123"))
}

func TestBlockSK(t *testing.T) {
	assert.Equal(t, "0000000000000001", BlockSK(1))
}

func TestBlockSKValue(t *testing.T) {
	assert.Equal(t, "B#0000000000000001", BlockSKValue(1))
}

func TestNativeTxPK(t *testing.T) {
	assert.Equal(t, "T#N#0xtxhash", NativeTxPK("0xtxhash"))
}

func TestNativeTxSK(t *testing.T) {
	assert.Equal(t, "0000000000000001#00000000", NativeTxSK(1, 0))
	assert.Equal(t, "000000000121eac0#00000005", NativeTxSK(19000000, 5))
}

func TestNativeTxBlockSK(t *testing.T) {
	assert.Equal(t, "N#0000000000000001#00000000", NativeTxBlockSK(1, 0))
}

func TestNativeReceivablePK(t *testing.T) {
	assert.Equal(t, "A#N#0xAddress", NativeReceivablePK("0xAddress"))
}

func TestNativeReceivableSK(t *testing.T) {
	assert.Equal(t, "0000000000000001#00000000#true", NativeReceivableSK(1, 0, true))
	assert.Equal(t, "0000000000000001#00000000#false", NativeReceivableSK(1, 0, false))
}

func TestInteractionPK(t *testing.T) {
	assert.Equal(t, "I#0xAddress", InteractionPK("0xAddress"))
}

func TestERC20Keys(t *testing.T) {
	assert.Equal(t, "T#20#0xtxhash", ERC20InsertPK("0xtxhash"))
	assert.Equal(t, "0000000000000001#00000000#L#00000003", ERC20InsertSK(1, 0, 3))
	assert.Equal(t, "A#20#0xAddr", ERC20ReceivablePK("0xAddr"))
	assert.Equal(t, "0000000000000001#00000000#L#00000003#true", ERC20ReceivableSK(1, 0, 3, true))
}

func TestERC721Keys(t *testing.T) {
	assert.Equal(t, "T#721#0xtxhash", ERC721InsertPK("0xtxhash"))
	assert.Equal(t, "A#721#0xAddr", ERC721ReceivablePK("0xAddr"))
}

func TestERC1155Keys(t *testing.T) {
	assert.Equal(t, "T#1155#0xtxhash", ERC1155InsertPK("0xtxhash"))
	assert.Equal(t, "0000000000000001#00000000#L#00000003#00000000", ERC1155InsertSK(1, 0, 3, 0))
	assert.Equal(t, "A#1155#0xAddr", ERC1155ReceivablePK("0xAddr"))
	assert.Equal(t, "0000000000000001#00000000#L#00000003#00000000#true", ERC1155ReceivableSK(1, 0, 3, 0, true))
}
