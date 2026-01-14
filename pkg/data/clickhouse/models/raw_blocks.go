package models

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// RawBlock represents a block row in the raw_blocks ClickHouse table
type RawBlock struct {
	ChainID               uint32
	BlockNumber           uint32
	Hash                  [32]byte
	ParentHash            [32]byte
	BlockTime             time.Time
	Miner                 [20]byte
	Difficulty            uint8
	TotalDifficulty       uint64
	Size                  uint32
	GasLimit              uint32
	GasUsed               uint32
	BaseFeePerGas         uint64
	BlockGasCost          uint64
	StateRoot             [32]byte
	TransactionsRoot      [32]byte
	ReceiptsRoot          [32]byte
	ExtraData             string
	BlockExtraData        string
	ExtDataHash           [32]byte
	ExtDataGasUsed        uint32
	MixHash               [32]byte
	Nonce                 [8]byte
	Sha3Uncles            [32]byte
	Uncles                [][32]byte
	BlobGasUsed           uint32
	ExcessBlobGas         uint64
	ParentBeaconBlockRoot [32]byte
	MinDelayExcess        uint64
}

// KafkaBlockJSON represents the JSON structure from Kafka
type KafkaBlockJSON struct {
	Number           uint64                   `json:"number"`
	Hash             string                   `json:"hash"`
	ParentHash       string                   `json:"parentHash"`
	StateRoot        string                   `json:"stateRoot"`
	TransactionsRoot string                   `json:"transactionsRoot"`
	ReceiptsRoot     string                   `json:"receiptsRoot"`
	Sha3Uncles       string                   `json:"sha3Uncles"`
	Miner            string                   `json:"miner"`
	GasLimit         uint64                   `json:"gasLimit"`
	GasUsed          uint64                   `json:"gasUsed"`
	Timestamp        uint64                   `json:"timestamp"`
	Size             uint64                   `json:"size"`
	Difficulty       uint64                   `json:"difficulty"`
	MixHash          string                   `json:"mixHash"`
	Nonce            interface{}              `json:"nonce"` // Can be number or string
	LogsBloom        string                   `json:"logsBloom"`
	ExtraData        string                   `json:"extraData"`
	BaseFeePerGas    *uint64                  `json:"baseFeePerGas,omitempty"`
	BlockGasCost     *uint64                  `json:"blockGasCost,omitempty"`
	BlockExtraData   *string                  `json:"blockExtraData,omitempty"`
	ExtDataHash      *string                  `json:"extDataHash,omitempty"`
	ExtDataGasUsed   *uint64                  `json:"extDataGasUsed,omitempty"`
	BlobGasUsed      *uint64                  `json:"blobGasUsed,omitempty"`
	ExcessBlobGas    *uint64                  `json:"excessBlobGas,omitempty"`
	ParentBeaconRoot *string                  `json:"parentBeaconBlockRoot,omitempty"`
	MinDelayExcess   *uint64                  `json:"minDelayExcess,omitempty"`
	Uncles           []string                 `json:"uncles,omitempty"`
	Transactions     []map[string]interface{} `json:"transactions"`
}

// ParseBlockFromJSON parses a JSON block from Kafka and converts it to RawBlock
// It extracts chainID from the block or transactions automatically
func ParseBlockFromJSON(data []byte) (*RawBlock, error) {
	var kafkaBlock KafkaBlockJSON
	if err := json.Unmarshal(data, &kafkaBlock); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block JSON: %w", err)
	}

	// Extract chainID from transactions array (chainID is not at block level in KafkaBlockJSON)
	var chainID uint32
	foundChainID := false

	if len(kafkaBlock.Transactions) > 0 {
		for _, tx := range kafkaBlock.Transactions {
			if chainIDVal, ok := tx["chainId"]; ok {
				foundChainID = true
				if v, ok := chainIDVal.(float64); ok {
					chainID = uint32(v)
				}
				// Found chainID (even if it's 0), so break
				break
			}
		}
	}

	if !foundChainID {
		return nil, fmt.Errorf("could not extract chainID from block transactions")
	}

	block := &RawBlock{
		ChainID:     chainID,
		BlockNumber: uint32(kafkaBlock.Number),
		Size:        uint32(kafkaBlock.Size),
		GasLimit:    uint32(kafkaBlock.GasLimit),
		GasUsed:     uint32(kafkaBlock.GasUsed),
		BlockTime:   time.Unix(int64(kafkaBlock.Timestamp), 0).UTC(),
	}

	// Convert difficulty - assuming it's small enough for uint8
	if kafkaBlock.Difficulty > 255 {
		block.Difficulty = 255
	} else {
		block.Difficulty = uint8(kafkaBlock.Difficulty)
	}
	block.TotalDifficulty = kafkaBlock.Difficulty // Using same value for now

	// Convert hex strings to byte arrays
	var err error
	if block.Hash, err = utils.HexToBytes32(kafkaBlock.Hash); err != nil {
		return nil, fmt.Errorf("failed to parse hash: %w", err)
	}
	if block.ParentHash, err = utils.HexToBytes32(kafkaBlock.ParentHash); err != nil {
		return nil, fmt.Errorf("failed to parse parentHash: %w", err)
	}
	if block.StateRoot, err = utils.HexToBytes32(kafkaBlock.StateRoot); err != nil {
		return nil, fmt.Errorf("failed to parse stateRoot: %w", err)
	}
	if block.TransactionsRoot, err = utils.HexToBytes32(kafkaBlock.TransactionsRoot); err != nil {
		return nil, fmt.Errorf("failed to parse transactionsRoot: %w", err)
	}
	if block.ReceiptsRoot, err = utils.HexToBytes32(kafkaBlock.ReceiptsRoot); err != nil {
		return nil, fmt.Errorf("failed to parse receiptsRoot: %w", err)
	}
	if block.Sha3Uncles, err = utils.HexToBytes32(kafkaBlock.Sha3Uncles); err != nil {
		return nil, fmt.Errorf("failed to parse sha3Uncles: %w", err)
	}
	if block.MixHash, err = utils.HexToBytes32(kafkaBlock.MixHash); err != nil {
		return nil, fmt.Errorf("failed to parse mixHash: %w", err)
	}
	if block.Miner, err = utils.HexToBytes20(kafkaBlock.Miner); err != nil {
		return nil, fmt.Errorf("failed to parse miner: %w", err)
	}

	// Parse nonce - can be number or string
	var nonceStr string
	switch v := kafkaBlock.Nonce.(type) {
	case string:
		nonceStr = v
	case float64:
		nonceStr = fmt.Sprintf("%x", uint64(v))
	case uint64:
		nonceStr = fmt.Sprintf("%x", v)
	case uint32:
		nonceStr = fmt.Sprintf("%x", v)
	default:
		nonceStr = "0"
	}
	if block.Nonce, err = utils.HexToBytes8(nonceStr); err != nil {
		return nil, fmt.Errorf("failed to parse nonce: %w", err)
	}

	// Optional fields
	block.ExtraData = kafkaBlock.ExtraData
	if kafkaBlock.BlockExtraData != nil {
		block.BlockExtraData = *kafkaBlock.BlockExtraData
	}
	if kafkaBlock.BaseFeePerGas != nil {
		block.BaseFeePerGas = *kafkaBlock.BaseFeePerGas
	}
	if kafkaBlock.BlockGasCost != nil {
		block.BlockGasCost = *kafkaBlock.BlockGasCost
	}
	if kafkaBlock.ExtDataHash != nil {
		if block.ExtDataHash, err = utils.HexToBytes32(*kafkaBlock.ExtDataHash); err != nil {
			return nil, fmt.Errorf("failed to parse extDataHash: %w", err)
		}
	}
	if kafkaBlock.ExtDataGasUsed != nil {
		block.ExtDataGasUsed = uint32(*kafkaBlock.ExtDataGasUsed)
	}
	if kafkaBlock.BlobGasUsed != nil {
		block.BlobGasUsed = uint32(*kafkaBlock.BlobGasUsed)
	}
	if kafkaBlock.ExcessBlobGas != nil {
		block.ExcessBlobGas = *kafkaBlock.ExcessBlobGas
	}
	if kafkaBlock.ParentBeaconRoot != nil {
		if block.ParentBeaconBlockRoot, err = utils.HexToBytes32(*kafkaBlock.ParentBeaconRoot); err != nil {
			return nil, fmt.Errorf("failed to parse parentBeaconBlockRoot: %w", err)
		}
	}
	if kafkaBlock.MinDelayExcess != nil {
		block.MinDelayExcess = *kafkaBlock.MinDelayExcess
	}

	// Parse uncles array
	if kafkaBlock.Uncles != nil {
		block.Uncles = make([][32]byte, len(kafkaBlock.Uncles))
		for i, uncle := range kafkaBlock.Uncles {
			if block.Uncles[i], err = utils.HexToBytes32(uncle); err != nil {
				return nil, fmt.Errorf("failed to parse uncle[%d]: %w", i, err)
			}
		}
	}

	return block, nil
}
