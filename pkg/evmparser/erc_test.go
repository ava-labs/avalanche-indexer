package evmparser

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkamsg "github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
)

func makeLog(address common.Address, topics []common.Hash, data []byte, index uint) *kafkamsg.EVMLog {
	return &kafkamsg.EVMLog{
		Address: address,
		Topics:  topics,
		Data:    data,
		Index:   index,
		TxHash:  common.HexToHash("0xabc123"),
	}
}

func TestParseERC20Transfers(t *testing.T) {
	transferTopic := common.HexToHash(TransferMethodHash)
	from := common.HexToHash("0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	to := common.HexToHash("0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	value := common.BigToHash(big.NewInt(1000))

	tx := &kafkamsg.EVMTransaction{
		Hash: "0xtxhash",
		Receipt: &kafkamsg.EVMTxReceipt{
			Logs: []*kafkamsg.EVMLog{
				makeLog(
					common.HexToAddress("0xContractAddr"),
					[]common.Hash{transferTopic, from, to},
					value.Bytes(),
					0,
				),
			},
		},
	}

	transfers := ParseERC20Transfers(tx)
	require.Len(t, transfers, 1)
	assert.Equal(t, "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa", transfers[0].From)
	assert.Equal(t, "0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB", transfers[0].To)
	assert.Equal(t, big.NewInt(1000), transfers[0].Value)
	assert.Equal(t, uint(0), transfers[0].LogIndex)
}

func TestParseERC20Transfers_IgnoresERC721(t *testing.T) {
	// 4 topics = ERC-721, not ERC-20
	transferTopic := common.HexToHash(TransferMethodHash)
	tx := &kafkamsg.EVMTransaction{
		Hash: "0xtxhash",
		Receipt: &kafkamsg.EVMTxReceipt{
			Logs: []*kafkamsg.EVMLog{
				makeLog(
					common.HexToAddress("0xContract"),
					[]common.Hash{transferTopic, {}, {}, {}},
					nil,
					0,
				),
			},
		},
	}

	transfers := ParseERC20Transfers(tx)
	assert.Empty(t, transfers)
}

func TestParseERC20Transfers_NilReceipt(t *testing.T) {
	tx := &kafkamsg.EVMTransaction{Hash: "0xtxhash"}
	transfers := ParseERC20Transfers(tx)
	assert.Nil(t, transfers)
}

func TestParseERC721Transfers(t *testing.T) {
	transferTopic := common.HexToHash(TransferMethodHash)
	from := common.HexToHash("0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	to := common.HexToHash("0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	tokenID := common.BigToHash(big.NewInt(42))

	tx := &kafkamsg.EVMTransaction{
		Hash: "0xtxhash",
		Receipt: &kafkamsg.EVMTxReceipt{
			Logs: []*kafkamsg.EVMLog{
				makeLog(
					common.HexToAddress("0xNFTContract"),
					[]common.Hash{transferTopic, from, to, tokenID},
					nil,
					3,
				),
			},
		},
	}

	transfers := ParseERC721Transfers(tx)
	require.Len(t, transfers, 1)
	assert.Equal(t, big.NewInt(42), transfers[0].TokenID)
	assert.Equal(t, uint(3), transfers[0].LogIndex)
}

func TestParseERC1155TransferSingle(t *testing.T) {
	topic0 := common.HexToHash(TransferSingleMethodHash)
	operator := common.HexToHash("0x0000000000000000000000001111111111111111111111111111111111111111")
	from := common.HexToHash("0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	to := common.HexToHash("0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	// Data: tokenID (32 bytes) + value (32 bytes)
	data := make([]byte, 64)
	big.NewInt(99).FillBytes(data[:32])
	big.NewInt(5).FillBytes(data[32:64])

	tx := &kafkamsg.EVMTransaction{
		Hash: "0xtxhash",
		Receipt: &kafkamsg.EVMTxReceipt{
			Logs: []*kafkamsg.EVMLog{
				makeLog(
					common.HexToAddress("0x1155Contract"),
					[]common.Hash{topic0, operator, from, to},
					data,
					7,
				),
			},
		},
	}

	transfers := ParseERC1155Transfers(tx)
	require.Len(t, transfers, 1)
	assert.Equal(t, big.NewInt(99), transfers[0].TokenID)
	assert.Equal(t, big.NewInt(5), transfers[0].Value)
	assert.Equal(t, uint(0), transfers[0].TransferIndex)
}

func TestParseERC1155TransferBatch(t *testing.T) {
	topic0 := common.HexToHash(TransferBatchMethodHash)
	operator := common.HexToHash("0x0000000000000000000000001111111111111111111111111111111111111111")
	from := common.HexToHash("0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	to := common.HexToHash("0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	// Data: offset1 (32) + offset2 (32) + length (32) + tokenId1 (32) + tokenId2 (32) + value1 (32) + value2 (32)
	data := make([]byte, 32*7)
	// offset1 (skip)
	// offset2 (skip)
	big.NewInt(2).FillBytes(data[64:96])      // length = 2
	big.NewInt(10).FillBytes(data[96:128])     // tokenId1
	big.NewInt(20).FillBytes(data[128:160])    // tokenId2
	big.NewInt(100).FillBytes(data[160:192])   // value1
	big.NewInt(200).FillBytes(data[192:224])   // value2

	tx := &kafkamsg.EVMTransaction{
		Hash: "0xtxhash",
		Receipt: &kafkamsg.EVMTxReceipt{
			Logs: []*kafkamsg.EVMLog{
				makeLog(
					common.HexToAddress("0x1155Contract"),
					[]common.Hash{topic0, operator, from, to},
					data,
					2,
				),
			},
		},
	}

	transfers := ParseERC1155Transfers(tx)
	require.Len(t, transfers, 2)

	assert.Equal(t, big.NewInt(10), transfers[0].TokenID)
	assert.Equal(t, big.NewInt(100), transfers[0].Value)
	assert.Equal(t, uint(0), transfers[0].TransferIndex)

	assert.Equal(t, big.NewInt(20), transfers[1].TokenID)
	assert.Equal(t, big.NewInt(200), transfers[1].Value)
	assert.Equal(t, uint(1), transfers[1].TransferIndex)
}

func TestParseERC1155TransferSingle_InvalidData(t *testing.T) {
	topic0 := common.HexToHash(TransferSingleMethodHash)
	tx := &kafkamsg.EVMTransaction{
		Hash: "0xtxhash",
		Receipt: &kafkamsg.EVMTxReceipt{
			Logs: []*kafkamsg.EVMLog{
				makeLog(
					common.HexToAddress("0x1155Contract"),
					[]common.Hash{topic0, {}, {}, {}},
					[]byte{1, 2, 3}, // too short
					0,
				),
			},
		},
	}

	transfers := ParseERC1155Transfers(tx)
	assert.Empty(t, transfers)
}

func TestParseERC1155TransferBatch_EmptyLength(t *testing.T) {
	topic0 := common.HexToHash(TransferBatchMethodHash)
	data := make([]byte, 96) // offset1 + offset2 + length=0

	tx := &kafkamsg.EVMTransaction{
		Hash: "0xtxhash",
		Receipt: &kafkamsg.EVMTxReceipt{
			Logs: []*kafkamsg.EVMLog{
				makeLog(
					common.HexToAddress("0x1155Contract"),
					[]common.Hash{topic0, {}, {}, {}},
					data,
					0,
				),
			},
		},
	}

	transfers := ParseERC1155Transfers(tx)
	assert.Empty(t, transfers)
}

func TestParseERC20_WrongTopic(t *testing.T) {
	wrongTopic := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	tx := &kafkamsg.EVMTransaction{
		Hash: "0xtxhash",
		Receipt: &kafkamsg.EVMTxReceipt{
			Logs: []*kafkamsg.EVMLog{
				makeLog(
					common.HexToAddress("0xContract"),
					[]common.Hash{wrongTopic, {}, {}},
					make([]byte, 32),
					0,
				),
			},
		},
	}

	transfers := ParseERC20Transfers(tx)
	assert.Empty(t, transfers)
}

func TestParseMultipleTransfers(t *testing.T) {
	transferTopic := common.HexToHash(TransferMethodHash)
	from1 := common.HexToHash("0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	to1 := common.HexToHash("0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	value1 := common.BigToHash(big.NewInt(100))

	from2 := common.HexToHash("0x000000000000000000000000cccccccccccccccccccccccccccccccccccccccc")
	to2 := common.HexToHash("0x000000000000000000000000dddddddddddddddddddddddddddddddddddddddd")
	value2 := common.BigToHash(big.NewInt(200))

	tx := &kafkamsg.EVMTransaction{
		Hash: "0xtxhash",
		Receipt: &kafkamsg.EVMTxReceipt{
			Logs: []*kafkamsg.EVMLog{
				makeLog(common.HexToAddress("0xToken1"), []common.Hash{transferTopic, from1, to1}, value1.Bytes(), 0),
				makeLog(common.HexToAddress("0xToken2"), []common.Hash{transferTopic, from2, to2}, value2.Bytes(), 1),
			},
		},
	}

	transfers := ParseERC20Transfers(tx)
	require.Len(t, transfers, 2)
	assert.Equal(t, big.NewInt(100), transfers[0].Value)
	assert.Equal(t, big.NewInt(200), transfers[1].Value)
}
