package evmrepo

import (
	"context"
	"fmt"

	_ "embed"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// Blocks provides methods to write blocks to ClickHouse
type Blocks interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteBlock(ctx context.Context, block *BlockRow) error
	DeleteBlocks(ctx context.Context, chainID uint64) error
}

//go:embed queries/block/create-blocks-table-local.sql
var createBlocksTableLocalQuery string

//go:embed queries/block/create-blocks-table.sql
var createBlocksTableQuery string

//go:embed queries/block/write-block.sql
var writeBlockQuery string

//go:embed queries/block/delete-blocks.sql
var deleteBlocksQuery string

type blocks struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

// NewBlocks creates a new raw blocks repository and initializes the table
func NewBlocks(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (Blocks, error) {
	repo := &blocks{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize blocks table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the raw_blocks table if it doesn't exist
func (r *blocks) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createBlocksTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create blocks local table: %w", err)
	}

	query = fmt.Sprintf(createBlocksTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create blocks table: %w", err)
	}
	return nil
}

// WriteBlock inserts a raw block into ClickHouse
func (r *blocks) WriteBlock(ctx context.Context, block *BlockRow) error {
	query := fmt.Sprintf(writeBlockQuery, r.database, r.tableName)

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
	)
	if err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}
	return nil
}

func (r *blocks) DeleteBlocks(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteBlocksQuery, r.database, r.tableName, r.cluster)
	if err := r.client.Conn().Exec(ctx, query, chainID); err != nil {
		return fmt.Errorf("failed to delete blocks: %w", err)
	}

	return nil
}
