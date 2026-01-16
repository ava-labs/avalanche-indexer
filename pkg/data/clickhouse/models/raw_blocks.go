package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/types/coreth"
)

// BlockRow represents a block row in the database
type BlockRow struct {
	ChainID               uint32
	BlockNumber           uint64
	Hash                  string
	ParentHash            string
	BlockTime             time.Time
	Miner                 string
	Difficulty            uint64
	TotalDifficulty       uint64
	Size                  uint64
	GasLimit              uint64
	GasUsed               uint64
	BaseFeePerGas         uint64
	BlockGasCost          uint64
	StateRoot             string
	TransactionsRoot      string
	ReceiptsRoot          string
	ExtraData             string
	BlockExtraData        string
	ExtDataHash           string
	ExtDataGasUsed        uint32
	MixHash               string
	Nonce                 string
	Sha3Uncles            string
	Uncles                []string
	BlobGasUsed           uint64
	ExcessBlobGas         uint64
	ParentBeaconBlockRoot string
	MinDelayExcess        uint64
}

// ParseBlockFromJSON parses a JSON block from Kafka and converts it to BlockRow
// Returns both the BlockRow and the transactions from the parsed block
func ParseBlockFromJSON(data []byte) (*BlockRow, []*coreth.Transaction, error) {
	// Unmarshal to coreth.Block
	var block coreth.Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal block JSON: %w", err)
	}

	// Extract chainID from block (should always be set by BlockFromLibevm)
	if block.ChainID == nil {
		return nil, nil, errors.New("block chainID is required but was not set")
	}
	chainID := uint32(block.ChainID.Uint64())

	blockRow := corethBlockToBlockRow(&block, chainID)

	// Return the block and transactions (already unmarshaled, no need to parse again)
	return blockRow, block.Transactions, nil
}

// corethBlockToBlockRow converts a coreth.Block to BlockRow
func corethBlockToBlockRow(block *coreth.Block, chainID uint32) *BlockRow {
	// Extract number from big.Int
	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	blockRow := &BlockRow{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Size:        block.Size,
		GasLimit:    block.GasLimit,
		GasUsed:     block.GasUsed,
		BlockTime:   time.Unix(int64(block.Timestamp), 0).UTC(),
		ExtraData:   block.ExtraData,
	}

	// Set difficulty from big.Int
	if block.Difficulty != nil {
		blockRow.Difficulty = block.Difficulty.Uint64()
		blockRow.TotalDifficulty = block.Difficulty.Uint64() // Using same value for now
	}

	// Direct string assignments - no conversions needed
	blockRow.Hash = block.Hash
	blockRow.ParentHash = block.ParentHash
	blockRow.StateRoot = block.StateRoot
	blockRow.TransactionsRoot = block.TransactionsRoot
	blockRow.ReceiptsRoot = block.ReceiptsRoot
	blockRow.Sha3Uncles = block.UncleHash
	blockRow.MixHash = block.MixHash
	blockRow.Miner = block.Miner

	// Parse nonce - convert uint64 to hex string
	blockRow.Nonce = strconv.FormatUint(block.Nonce, 16)

	// Optional fields
	if block.BaseFee != nil {
		blockRow.BaseFeePerGas = block.BaseFee.Uint64()
	}
	if block.BlobGasUsed != nil {
		blockRow.BlobGasUsed = *block.BlobGasUsed
	}
	if block.ExcessBlobGas != nil {
		blockRow.ExcessBlobGas = *block.ExcessBlobGas
	}
	if block.ParentBeaconBlockRoot != "" {
		blockRow.ParentBeaconBlockRoot = block.ParentBeaconBlockRoot
	}
	if block.MinDelayExcess != 0 {
		blockRow.MinDelayExcess = block.MinDelayExcess
	}

	return blockRow
}
