package evmrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	_ "embed"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// Blocks provides methods to write blocks to ClickHouse
type Blocks interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteBlock(ctx context.Context, block *BlockRow) error
	BatchInsertBlocks(ctx context.Context, blocks []*BlockRow) error
	DeleteBlocks(ctx context.Context, chainID uint64) error
}

//go:embed queries/block/create-blocks-table-local.sql
var createBlocksTableLocalQuery string

//go:embed queries/block/create-blocks-table.sql
var createBlocksTableQuery string

//go:embed queries/block/write-block.sql
var writeBlockQuery string

//go:embed queries/block/batch-insert-blocks.sql
var batchInsertBlocksQuery string

//go:embed queries/block/delete-blocks.sql
var deleteBlocksQuery string

type blocks struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chBlockRow struct {
	hashBytes                  string
	parentHashBytes            string
	minerBytes                 string
	stateRootBytes             string
	transactionsRootBytes      string
	receiptsRootBytes          string
	extDataHashBytes           string
	mixHashBytes               string
	sha3UnclesBytes            string
	unclesStrings              []string
	nonceBytes                 interface{}
	parentBeaconBlockRootBytes interface{}
	blockchainID               interface{}
	evmChainID                 string
	blockNumber                uint64
	difficulty                 string
	totalDifficulty            string
	size                       uint64
	gasLimit                   uint64
	gasUsed                    uint64
	baseFeePerGas              string
	blockGasCost               string
	blobGasUsed                uint64
	excessBlobGas              uint64
	minDelayExcess             uint64
	numTxns                    uint32
	blockExtraData             string
	extDataGasUsed             uint32
	extraData                  string
	timestampMs                uint64
	blockTime                  time.Time
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

// CreateTableIfNotExists creates the raw_blocks table if it doesn't exist,
// then runs all numbered migrations from queries/migrations/block/ to ensure
// the schema is up to date for existing tables.
func (r *blocks) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createBlocksTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create blocks local table: %w", err)
	}

	query = fmt.Sprintf(createBlocksTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create blocks table: %w", err)
	}

	if err := clickhouse.RunMigrations(ctx, r.client.Conn(), blocksMigrationsFS, "queries/migrations/block", r.database, r.tableName, r.cluster); err != nil {
		return fmt.Errorf("failed to run blocks migrations: %w", err)
	}

	return nil
}

func convertBlockRowToChBlockRow(block *BlockRow) (*chBlockRow, error) {
	if block == nil {
		return nil, errors.New("block is nil")
	}

	// Convert hex strings to bytes for FixedString fields
	hashBytes, err := utils.HexToBytes32(block.Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert hash to bytes: %w", err)
	}

	parentHashBytes, err := utils.HexToBytes32(block.ParentHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert parent_hash to bytes: %w", err)
	}

	minerBytes, err := utils.HexToBytes20(block.Miner)
	if err != nil {
		return nil, fmt.Errorf("failed to convert miner to bytes: %w", err)
	}

	stateRootBytes, err := utils.HexToBytes32(block.StateRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to convert state_root to bytes: %w", err)
	}

	transactionsRootBytes, err := utils.HexToBytes32(block.TransactionsRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to convert transactions_root to bytes: %w", err)
	}

	receiptsRootBytes, err := utils.HexToBytes32(block.ReceiptsRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to convert receipts_root to bytes: %w", err)
	}

	extDataHashBytes, err := utils.HexToBytes32(block.ExtDataHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert ext_data_hash to bytes: %w", err)
	}

	mixHashBytes, err := utils.HexToBytes32(block.MixHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert mix_hash to bytes: %w", err)
	}

	sha3UnclesBytes, err := utils.HexToBytes32(block.Sha3Uncles)
	if err != nil {
		return nil, fmt.Errorf("failed to convert sha3_uncles to bytes: %w", err)
	}

	// Convert byte arrays to strings for ClickHouse FixedString columns
	// ClickHouse FixedString expects strings of exact length, not byte slices
	unclesStrings := make([]string, len(block.Uncles))
	for i, uncle := range block.Uncles {
		uncleBytes, err := utils.HexToBytes32(uncle)
		if err != nil {
			return nil, fmt.Errorf("failed to convert uncle %d to bytes: %w", i, err)
		}
		unclesStrings[i] = string(uncleBytes[:])
	}

	// For nullable nonce - convert empty string to nil, otherwise convert to bytes then string
	var nonceBytes interface{}
	if block.Nonce == "" {
		nonceBytes = nil
	} else {
		nonce, err := utils.HexToBytes8(block.Nonce)
		if err != nil {
			return nil, fmt.Errorf("failed to convert nonce to bytes: %w", err)
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
			return nil, fmt.Errorf("failed to convert parent_beacon_block_root to bytes: %w", err)
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

	return &chBlockRow{
		hashBytes:                  string(hashBytes[:]),
		parentHashBytes:            string(parentHashBytes[:]),
		minerBytes:                 string(minerBytes[:]),
		stateRootBytes:             string(stateRootBytes[:]),
		transactionsRootBytes:      string(transactionsRootBytes[:]),
		receiptsRootBytes:          string(receiptsRootBytes[:]),
		extDataHashBytes:           string(extDataHashBytes[:]),
		mixHashBytes:               string(mixHashBytes[:]),
		sha3UnclesBytes:            string(sha3UnclesBytes[:]),
		unclesStrings:              unclesStrings,
		nonceBytes:                 nonceBytes,
		parentBeaconBlockRootBytes: parentBeaconBlockRootBytes,
		blockchainID:               blockchainID,
		evmChainID:                 evmChainIDStr,
		blockNumber:                blockNumber,
		difficulty:                 difficultyStr,
		totalDifficulty:            totalDifficultyStr,
		size:                       block.Size,
		gasLimit:                   block.GasLimit,
		gasUsed:                    block.GasUsed,
		baseFeePerGas:              baseFeeStr,
		blockGasCost:               blockGasCostStr,
		blobGasUsed:                block.BlobGasUsed,
		excessBlobGas:              block.ExcessBlobGas,
		minDelayExcess:             block.MinDelayExcess,
		numTxns:                    block.NumTxns,
		blockExtraData:             block.BlockExtraData,
		extDataGasUsed:             block.ExtDataGasUsed,
		extraData:                  block.ExtraData,
		timestampMs:                block.TimestampMs,
		blockTime:                  block.BlockTime,
	}, nil
}

// WriteBlock inserts a raw block into ClickHouse
func (r *blocks) WriteBlock(ctx context.Context, block *BlockRow) error {
	query := fmt.Sprintf(writeBlockQuery, r.database, r.tableName)

	row, err := convertBlockRowToChBlockRow(block)
	if err != nil {
		return fmt.Errorf("failed to convert block row of block %d to ch row: %w", block.BlockNumber, err)
	}

	err = r.client.Conn().Exec(ctx, query,
		row.blockchainID,
		row.evmChainID,
		row.blockNumber,
		row.hashBytes,
		row.parentHashBytes,
		row.blockTime,
		row.timestampMs,
		row.minerBytes,
		row.difficulty,
		row.totalDifficulty,
		row.size,
		row.gasLimit,
		row.gasUsed,
		row.baseFeePerGas,
		row.blockGasCost,
		row.stateRootBytes,
		row.transactionsRootBytes,
		row.receiptsRootBytes,
		row.extraData,
		row.blockExtraData,
		row.extDataHashBytes,
		row.extDataGasUsed,
		row.mixHashBytes,
		row.nonceBytes,
		row.sha3UnclesBytes,
		row.unclesStrings,
		row.blobGasUsed,
		row.excessBlobGas,
		row.parentBeaconBlockRootBytes,
		row.minDelayExcess,
		row.numTxns,
	)
	if err != nil {
		return fmt.Errorf("failed to write block of block %d: %w", block.BlockNumber, err)
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

func (r *blocks) BatchInsertBlocks(ctx context.Context, blocks []*BlockRow) error {
	if len(blocks) == 0 {
		return nil
	}

	query := fmt.Sprintf(batchInsertBlocksQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, block := range blocks {
		row, err := convertBlockRowToChBlockRow(block)
		if err != nil {
			return fmt.Errorf("failed to convert block row of block %d to ch row: %w", block.BlockNumber, err)
		}
		err = batch.Append(
			row.blockchainID,
			row.evmChainID,
			row.blockNumber,
			row.hashBytes,
			row.parentHashBytes,
			row.blockTime,
			row.timestampMs,
			row.minerBytes,
			row.difficulty,
			row.totalDifficulty,
			row.size,
			row.gasLimit,
			row.gasUsed,
			row.baseFeePerGas,
			row.blockGasCost,
			row.stateRootBytes,
			row.transactionsRootBytes,
			row.receiptsRootBytes,
			row.extraData,
			row.blockExtraData,
			row.extDataHashBytes,
			row.extDataGasUsed,
			row.mixHashBytes,
			row.nonceBytes,
			row.sha3UnclesBytes,
			row.unclesStrings,
			row.blobGasUsed,
			row.excessBlobGas,
			row.parentBeaconBlockRootBytes,
			row.minDelayExcess,
			row.numTxns,
		)
		if err != nil {
			return fmt.Errorf("failed to append block %d: %w", block.BlockNumber, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}
