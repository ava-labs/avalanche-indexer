package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	corethtypes "github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

const (
	// chainIDKey is the JSON key used to extract chainID from transaction data
	chainIDKey = "chainId"
)

// RawBlock represents a block row in the raw_blocks ClickHouse table
type RawBlock struct {
	ChainID               uint32
	BlockNumber           uint32
	Hash                  [32]byte
	ParentHash            [32]byte
	BlockTime             time.Time
	Miner                 [20]byte
	Difficulty            uint64
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

// ParseBlockFromJSON parses a JSON block from Kafka and converts it to RawBlock
// It extracts chainID from the block or transactions automatically
func ParseBlockFromJSON(data []byte) (*RawBlock, error) {
	var kafkaBlock corethtypes.Block
	if err := json.Unmarshal(data, &kafkaBlock); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block JSON: %w", err)
	}

	// Extract chainID from transactions array (chainID is not at block level)
	var chainID uint32
	foundChainID := false

	if len(kafkaBlock.Transactions) > 0 {
		for _, tx := range kafkaBlock.Transactions {
			if tx.ChainID != nil {
				foundChainID = true
				chainID = uint32(tx.ChainID.Uint64())
				// Found chainID (even if it's 0), so break
				break
			}
		}
	}

	if !foundChainID {
		return nil, errors.New("could not extract chainID from block transactions")
	}

	// Extract number from big.Int
	var blockNumber uint64
	if kafkaBlock.Number != nil {
		blockNumber = kafkaBlock.Number.Uint64()
	}

	block := &RawBlock{
		ChainID:     chainID,
		BlockNumber: uint32(blockNumber),
		Size:        uint32(kafkaBlock.Size),
		GasLimit:    uint32(kafkaBlock.GasLimit),
		GasUsed:     uint32(kafkaBlock.GasUsed),
		BlockTime:   time.Unix(int64(kafkaBlock.Timestamp), 0).UTC(),
	}

	// Set difficulty from big.Int
	if kafkaBlock.Difficulty != nil {
		block.Difficulty = kafkaBlock.Difficulty.Uint64()
		block.TotalDifficulty = kafkaBlock.Difficulty.Uint64() // Using same value for now
	}

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
	if block.Sha3Uncles, err = utils.HexToBytes32(kafkaBlock.UncleHash); err != nil {
		return nil, fmt.Errorf("failed to parse sha3Uncles: %w", err)
	}
	if block.MixHash, err = utils.HexToBytes32(kafkaBlock.MixHash); err != nil {
		return nil, fmt.Errorf("failed to parse mixHash: %w", err)
	}
	if block.Miner, err = utils.HexToBytes20(kafkaBlock.Miner); err != nil {
		return nil, fmt.Errorf("failed to parse miner: %w", err)
	}

	// Parse nonce - convert uint64 to hex string
	nonceStr := fmt.Sprintf("%x", kafkaBlock.Nonce)
	if block.Nonce, err = utils.HexToBytes8(nonceStr); err != nil {
		return nil, fmt.Errorf("failed to parse nonce: %w", err)
	}

	// Optional fields
	block.ExtraData = kafkaBlock.ExtraData
	if kafkaBlock.BaseFee != nil {
		block.BaseFeePerGas = kafkaBlock.BaseFee.Uint64()
	}
	if kafkaBlock.BlobGasUsed != nil {
		block.BlobGasUsed = uint32(*kafkaBlock.BlobGasUsed)
	}
	if kafkaBlock.ExcessBlobGas != nil {
		block.ExcessBlobGas = *kafkaBlock.ExcessBlobGas
	}
	if kafkaBlock.ParentBeaconBlockRoot != "" {
		if block.ParentBeaconBlockRoot, err = utils.HexToBytes32(kafkaBlock.ParentBeaconBlockRoot); err != nil {
			return nil, fmt.Errorf("failed to parse parentBeaconBlockRoot: %w", err)
		}
	}
	if kafkaBlock.MinDelayExcess != 0 {
		block.MinDelayExcess = kafkaBlock.MinDelayExcess
	}

	return block, nil
}
