package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// ClickhouseBlock represents a block row in the raw_blocks ClickHouse table
type ClickhouseBlock struct {
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

// ParseBlockFromJSON parses a JSON block from Kafka and converts it to ClickhouseBlock
func ParseBlockFromJSON(data []byte) (*ClickhouseBlock, error) {
	// Unmarshal to coreth.Block
	var block coreth.Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block JSON: %w", err)
	}

	// Extract chainID - check block level first, then transactions
	chainID, err := getChainIDFromBlock(&block)
	if err != nil {
		return nil, err
	}

	return corethBlockToClickhouseBlock(&block, chainID)
}

// getChainIDFromBlock extracts chainID from block, checking block level first, then transactions
func getChainIDFromBlock(block *coreth.Block) (uint32, error) {
	// Check block level first (if present in JSON)
	if block.ChainID != nil {
		return uint32(block.ChainID.Uint64()), nil
	}

	// If not found at block level, extract from transactions
	if len(block.Transactions) > 0 {
		for _, tx := range block.Transactions {
			if tx.ChainID != nil {
				return uint32(tx.ChainID.Uint64()), nil
			}
		}
	}

	return 0, errors.New("could not extract chainID from block or transactions")
}

// corethBlockToClickhouseBlock converts a coreth.Block to ClickhouseBlock
func corethBlockToClickhouseBlock(block *coreth.Block, chainID uint32) (*ClickhouseBlock, error) {

	// Extract number from big.Int
	var blockNumber uint32
	if block.Number != nil {
		blockNumber = uint32(block.Number.Uint64())
	}

	clickhouseBlock := &ClickhouseBlock{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Size:        uint32(block.Size),
		GasLimit:    uint32(block.GasLimit),
		GasUsed:     uint32(block.GasUsed),
		BlockTime:   time.Unix(int64(block.Timestamp), 0).UTC(),
		ExtraData:   block.ExtraData,
	}

	// Set difficulty from big.Int
	if block.Difficulty != nil {
		clickhouseBlock.Difficulty = block.Difficulty.Uint64()
		clickhouseBlock.TotalDifficulty = block.Difficulty.Uint64() // Using same value for now
	}

	// Convert hex strings to byte arrays
	var err error
	if clickhouseBlock.Hash, err = utils.HexToBytes32(block.Hash); err != nil {
		return nil, fmt.Errorf("failed to parse hash: %w", err)
	}
	if clickhouseBlock.ParentHash, err = utils.HexToBytes32(block.ParentHash); err != nil {
		return nil, fmt.Errorf("failed to parse parentHash: %w", err)
	}
	if clickhouseBlock.StateRoot, err = utils.HexToBytes32(block.StateRoot); err != nil {
		return nil, fmt.Errorf("failed to parse stateRoot: %w", err)
	}
	if clickhouseBlock.TransactionsRoot, err = utils.HexToBytes32(block.TransactionsRoot); err != nil {
		return nil, fmt.Errorf("failed to parse transactionsRoot: %w", err)
	}
	if clickhouseBlock.ReceiptsRoot, err = utils.HexToBytes32(block.ReceiptsRoot); err != nil {
		return nil, fmt.Errorf("failed to parse receiptsRoot: %w", err)
	}
	if clickhouseBlock.Sha3Uncles, err = utils.HexToBytes32(block.UncleHash); err != nil {
		return nil, fmt.Errorf("failed to parse sha3Uncles: %w", err)
	}
	if clickhouseBlock.MixHash, err = utils.HexToBytes32(block.MixHash); err != nil {
		return nil, fmt.Errorf("failed to parse mixHash: %w", err)
	}
	if clickhouseBlock.Miner, err = utils.HexToBytes20(block.Miner); err != nil {
		return nil, fmt.Errorf("failed to parse miner: %w", err)
	}

	// Parse nonce - convert uint64 to hex string
	nonceStr := strconv.FormatUint(block.Nonce, 16)
	if clickhouseBlock.Nonce, err = utils.HexToBytes8(nonceStr); err != nil {
		return nil, fmt.Errorf("failed to parse nonce: %w", err)
	}

	// Optional fields
	if block.BaseFee != nil {
		clickhouseBlock.BaseFeePerGas = block.BaseFee.Uint64()
	}
	if block.BlobGasUsed != nil {
		clickhouseBlock.BlobGasUsed = uint32(*block.BlobGasUsed)
	}
	if block.ExcessBlobGas != nil {
		clickhouseBlock.ExcessBlobGas = *block.ExcessBlobGas
	}
	if block.ParentBeaconBlockRoot != "" {
		if clickhouseBlock.ParentBeaconBlockRoot, err = utils.HexToBytes32(block.ParentBeaconBlockRoot); err != nil {
			return nil, fmt.Errorf("failed to parse parentBeaconBlockRoot: %w", err)
		}
	}
	if block.MinDelayExcess != 0 {
		clickhouseBlock.MinDelayExcess = block.MinDelayExcess
	}

	return clickhouseBlock, nil
}
