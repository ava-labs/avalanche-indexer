package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/types/coreth"
)

// ClickhouseBlock represents a block row in the raw_blocks ClickHouse table
type ClickhouseBlock struct {
	EVMChainID            *big.Int
	BlockchainID          *string
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

// ParseBlockFromJSON parses a JSON block from Kafka and converts it to ClickhouseBlock
func ParseBlockFromJSON(data []byte) (*ClickhouseBlock, error) {
	// Unmarshal to coreth.Block
	var block coreth.Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block JSON: %w", err)
	}

	// Extract chainID from block (should always be set by BlockFromLibevm)
	if block.EVMChainID == nil {
		return nil, errors.New("block evmChainID is required but was not set")
	}
	if block.BlockchainID == nil {
		return nil, errors.New("block blockchainID is required but was not set")
	}

	return corethBlockToClickhouseBlock(&block)
}

// corethBlockToClickhouseBlock converts a coreth.Block to ClickhouseBlock
func corethBlockToClickhouseBlock(block *coreth.Block) (*ClickhouseBlock, error) {
	// Extract number from big.Int
	var blockNumber uint64
	if block.Number != nil {
		blockNumber = block.Number.Uint64()
	}

	clickhouseBlock := &ClickhouseBlock{
		EVMChainID:   block.EVMChainID,
		BlockchainID: block.BlockchainID,
		BlockNumber:  blockNumber,
		Size:         block.Size,
		GasLimit:     block.GasLimit,
		GasUsed:      block.GasUsed,
		BlockTime:    time.Unix(int64(block.Timestamp), 0).UTC(),
		ExtraData:    block.ExtraData,
	}

	// Set difficulty from big.Int
	if block.Difficulty != nil {
		clickhouseBlock.Difficulty = block.Difficulty.Uint64()
		clickhouseBlock.TotalDifficulty = block.Difficulty.Uint64() // Using same value for now
	}

	// Direct string assignments - no conversions needed
	clickhouseBlock.Hash = block.Hash
	clickhouseBlock.ParentHash = block.ParentHash
	clickhouseBlock.StateRoot = block.StateRoot
	clickhouseBlock.TransactionsRoot = block.TransactionsRoot
	clickhouseBlock.ReceiptsRoot = block.ReceiptsRoot
	clickhouseBlock.Sha3Uncles = block.UncleHash
	clickhouseBlock.MixHash = block.MixHash
	clickhouseBlock.Miner = block.Miner

	// Parse nonce - convert uint64 to hex string
	clickhouseBlock.Nonce = strconv.FormatUint(block.Nonce, 16)

	// Optional fields
	if block.BaseFee != nil {
		clickhouseBlock.BaseFeePerGas = block.BaseFee.Uint64()
	}
	if block.BlobGasUsed != nil {
		clickhouseBlock.BlobGasUsed = *block.BlobGasUsed
	}
	if block.ExcessBlobGas != nil {
		clickhouseBlock.ExcessBlobGas = *block.ExcessBlobGas
	}
	if block.ParentBeaconBlockRoot != "" {
		clickhouseBlock.ParentBeaconBlockRoot = block.ParentBeaconBlockRoot
	}
	if block.MinDelayExcess != 0 {
		clickhouseBlock.MinDelayExcess = block.MinDelayExcess
	}

	return clickhouseBlock, nil
}
