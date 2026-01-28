package evmrepo

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// Blocks provides methods to write blocks to ClickHouse
type Blocks interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteBlock(ctx context.Context, block *BlockRow) error
}

type blocks struct {
	client    clickhouse.Client
	tableName string
}

// NewBlocks creates a new raw blocks repository and initializes the table
func NewBlocks(ctx context.Context, client clickhouse.Client, tableName string) (Blocks, error) {
	repo := &blocks{
		client:    client,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize blocks table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the raw_blocks table if it doesn't exist
func (r *blocks) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			blockchain_id String,
			evm_chain_id UInt256,
			block_number UInt64,
			hash FixedString(32),
			parent_hash FixedString(32),
			block_time DateTime64(3, 'UTC'),
			miner FixedString(20),
			difficulty UInt256,
			total_difficulty UInt256,
			size UInt64,
			gas_limit UInt64,
			gas_used UInt64,
			base_fee_per_gas UInt256,
			block_gas_cost UInt256,
			state_root FixedString(32),
			transactions_root FixedString(32),
			receipts_root FixedString(32),
			extra_data String,
			block_extra_data String,
			ext_data_hash FixedString(32),
			ext_data_gas_used UInt32,
			mix_hash FixedString(32),
			nonce Nullable(FixedString(8)),
			sha3_uncles FixedString(32),
			uncles Array(FixedString(32)),
			blob_gas_used UInt64,
			excess_blob_gas UInt64,
			parent_beacon_block_root Nullable(FixedString(32)),
			min_delay_excess UInt64,
			partition_month INTEGER
		)
		ENGINE = MergeTree
		PARTITION BY (toString(evm_chain_id), partition_month)
		ORDER BY (toString(evm_chain_id), partition_month, block_time, block_number)
		SETTINGS index_granularity = 8192, allow_nullable_key = 1
	`, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create blocks table: %w", err)
	}
	return nil
}

// WriteBlock inserts a raw block into ClickHouse
func (r *blocks) WriteBlock(ctx context.Context, block *BlockRow) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			blockchain_id, evm_chain_id, block_number, hash, parent_hash, block_time, miner,
			difficulty, total_difficulty, size, gas_limit, gas_used,
			base_fee_per_gas, block_gas_cost, state_root, transactions_root, receipts_root,
			extra_data, block_extra_data, ext_data_hash, ext_data_gas_used,
			mix_hash, nonce, sha3_uncles, uncles,
			blob_gas_used, excess_blob_gas, parent_beacon_block_root, min_delay_excess, partition_month
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.tableName)

	// Convert hex strings to bytes for FixedString fields
	hashBytes, err := utils.HexToBytes32(block.Hash)
	if err != nil {
		return fmt.Errorf("failed to convert hash to bytes: %w", err)
	}
	parentHashBytes, err := utils.HexToBytes32(block.ParentHash)
	if err != nil {
		return fmt.Errorf("failed to convert parent_hash to bytes: %w", err)
	}
	minerBytes, err := utils.HexToBytes20(block.Miner)
	if err != nil {
		return fmt.Errorf("failed to convert miner to bytes: %w", err)
	}
	stateRootBytes, err := utils.HexToBytes32(block.StateRoot)
	if err != nil {
		return fmt.Errorf("failed to convert state_root to bytes: %w", err)
	}
	transactionsRootBytes, err := utils.HexToBytes32(block.TransactionsRoot)
	if err != nil {
		return fmt.Errorf("failed to convert transactions_root to bytes: %w", err)
	}
	receiptsRootBytes, err := utils.HexToBytes32(block.ReceiptsRoot)
	if err != nil {
		return fmt.Errorf("failed to convert receipts_root to bytes: %w", err)
	}
	extDataHashBytes, err := utils.HexToBytes32(block.ExtDataHash)
	if err != nil {
		return fmt.Errorf("failed to convert ext_data_hash to bytes: %w", err)
	}
	mixHashBytes, err := utils.HexToBytes32(block.MixHash)
	if err != nil {
		return fmt.Errorf("failed to convert mix_hash to bytes: %w", err)
	}
	sha3UnclesBytes, err := utils.HexToBytes32(block.Sha3Uncles)
	if err != nil {
		return fmt.Errorf("failed to convert sha3_uncles to bytes: %w", err)
	}

	// Convert uncles array
	unclesBytes := make([][32]byte, len(block.Uncles))
	for i, uncle := range block.Uncles {
		uncleBytes, err := utils.HexToBytes32(uncle)
		if err != nil {
			return fmt.Errorf("failed to convert uncle %d to bytes: %w", i, err)
		}
		unclesBytes[i] = uncleBytes
	}

	// For nullable nonce - convert empty string to nil, otherwise convert to bytes then string
	var nonceBytes interface{}
	if block.Nonce == "" {
		nonceBytes = nil
	} else {
		nonce, err := utils.HexToBytes8(block.Nonce)
		if err != nil {
			return fmt.Errorf("failed to convert nonce to bytes: %w", err)
		}
		nonceBytes = string(nonce[:])
	}

	// For nullable parent_beacon_block_root - convert empty string to nil, otherwise convert to bytes then string
	var parentBeaconBlockRootBytes interface{}
	if block.ParentBeaconBlockRoot == "" {
		parentBeaconBlockRootBytes = nil
	} else {
		beaconRoot, err := utils.HexToBytes32(block.ParentBeaconBlockRoot)
		if err != nil {
			return fmt.Errorf("failed to convert parent_beacon_block_root to bytes: %w", err)
		}
		parentBeaconBlockRootBytes = string(beaconRoot[:])
	}

	// Convert BlockchainID (string) and EVMChainID (*big.Int) for ClickHouse
	var blockchainID interface{}
	if block.BlockchainID != nil {
		blockchainID = *block.BlockchainID
	} else {
		blockchainID = ""
	}
	// Convert *big.Int to string for ClickHouse UInt256 fields
	// ClickHouse accepts UInt256 as string representation
	evmChainIDStr := "0"
	if block.EVMChainID != nil {
		evmChainIDStr = block.EVMChainID.String()
	}

	// Convert BlockNumber from *big.Int to uint64 for ClickHouse UInt64
	var blockNumber uint64
	if block.BlockNumber != nil {
		blockNumber = block.BlockNumber.Uint64()
	}

	// Convert *big.Int to string for ClickHouse UInt256 fields
	// ClickHouse accepts UInt256 as string representation
	difficultyStr := "0"
	if block.Difficulty != nil {
		difficultyStr = block.Difficulty.String()
	}
	totalDifficultyStr := "0"
	if block.TotalDifficulty != nil {
		totalDifficultyStr = block.TotalDifficulty.String()
	}
	baseFeeStr := "0"
	if block.BaseFeePerGas != nil {
		baseFeeStr = block.BaseFeePerGas.String()
	}
	blockGasCostStr := "0"
	if block.BlockGasCost != nil {
		blockGasCostStr = block.BlockGasCost.String()
	}

	// Convert byte arrays to strings for ClickHouse FixedString columns
	// ClickHouse FixedString expects strings of exact length, not byte slices
	unclesStrings := make([]string, len(unclesBytes))
	for i, uncle := range unclesBytes {
		unclesStrings[i] = string(uncle[:])
	}

	// Calculate partition_month as YYYYMM from block_time
	partitionMonth := block.BlockTime.Year()*100 + int(block.BlockTime.Month())

	err = r.client.Conn().Exec(ctx, query,
		blockchainID,
		evmChainIDStr,
		blockNumber,
		string(hashBytes[:]),
		string(parentHashBytes[:]),
		block.BlockTime,
		string(minerBytes[:]),
		difficultyStr,
		totalDifficultyStr,
		block.Size,
		block.GasLimit,
		block.GasUsed,
		baseFeeStr,
		blockGasCostStr,
		string(stateRootBytes[:]),
		string(transactionsRootBytes[:]),
		string(receiptsRootBytes[:]),
		block.ExtraData,
		block.BlockExtraData,
		string(extDataHashBytes[:]),
		block.ExtDataGasUsed,
		string(mixHashBytes[:]),
		nonceBytes,
		string(sha3UnclesBytes[:]),
		unclesStrings,
		block.BlobGasUsed,
		block.ExcessBlobGas,
		parentBeaconBlockRootBytes,
		block.MinDelayExcess,
		partitionMonth,
	)
	if err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}
	return nil
}
