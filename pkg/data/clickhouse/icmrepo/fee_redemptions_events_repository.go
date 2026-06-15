package icmrepo

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	_ "embed"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// FeeRedemptionsEvents provides methods to write ICM relayer rewards redeemed events to ClickHouse.
type FeeRedemptionsEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteFeeRedemptionsEvent(ctx context.Context, row *FeeRedemptionsEventRow) error
	BatchInsertFeeRedemptionsEvents(ctx context.Context, rows []*FeeRedemptionsEventRow) error
	DeleteFeeRedemptionsEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/fee_redemptions_events/create-fee-redemptions-events-table-local.sql
var createFeeRedemptionsEventsTableLocalQuery string

//go:embed queries/fee_redemptions_events/create-fee-redemptions-events-table.sql
var createFeeRedemptionsEventsTableQuery string

//go:embed queries/fee_redemptions_events/write-fee-redemptions-event.sql
var writeFeeRedemptionsEventQuery string

//go:embed queries/fee_redemptions_events/batch-insert-fee-redemptions-events.sql
var batchInsertFeeRedemptionsEventsQuery string

//go:embed queries/fee_redemptions_events/delete-fee-redemptions-events.sql
var deleteFeeRedemptionsEventsQuery string

type feeRedemptionsEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chFeeRedemptionsEventRow struct {
	BlockchainID    string   `ch:"blockchain_id"`
	EVMChainID      *big.Int `ch:"evm_chain_id"`
	BlockNumber     uint64   `ch:"block_number"`
	BlockTime       time.Time `ch:"block_time"`
	TxHash          string   `ch:"tx_hash"`
	TxIndex         uint32   `ch:"tx_index"`
	LogIndex        uint32   `ch:"log_index"`
	ContractAddress string   `ch:"contract_address"`
	RedeemerAddress string   `ch:"redeemer_address"`
	FeeTokenAddress string   `ch:"fee_token_address"`
	Amount          *big.Int `ch:"amount"`
}

func convertFeeRedemptionsEventRow(row *FeeRedemptionsEventRow) (*chFeeRedemptionsEventRow, error) {
	if row == nil {
		return nil, errors.New("fee redemptions event row is nil")
	}
	txHash, err := hexToFixed32(row.TxHash)
	if err != nil {
		return nil, fmt.Errorf("tx_hash: %w", err)
	}
	contractAddress, err := hexToFixed20(row.ContractAddress)
	if err != nil {
		return nil, fmt.Errorf("contract_address: %w", err)
	}
	redeemerAddress, err := hexToFixed20(row.RedeemerAddress)
	if err != nil {
		return nil, fmt.Errorf("redeemer_address: %w", err)
	}
	feeTokenAddress, err := hexToFixed20(row.FeeTokenAddress)
	if err != nil {
		return nil, fmt.Errorf("fee_token_address: %w", err)
	}
	return &chFeeRedemptionsEventRow{
		BlockchainID:    row.BlockchainID,
		EVMChainID:      bigIntOrZero(row.EVMChainID),
		BlockNumber:     row.BlockNumber,
		BlockTime:       row.BlockTime,
		TxHash:          txHash,
		TxIndex:         row.TxIndex,
		LogIndex:        row.LogIndex,
		ContractAddress: contractAddress,
		RedeemerAddress: redeemerAddress,
		FeeTokenAddress: feeTokenAddress,
		Amount:          bigIntOrZero(row.Amount),
	}, nil
}

// NewFeeRedemptionsEvents creates a new fee redemptions events repository and initializes the table.
func NewFeeRedemptionsEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (FeeRedemptionsEvents, error) {
	repo := &feeRedemptionsEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize fee redemptions events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed icm_fee_redemptions_events tables.
func (r *feeRedemptionsEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createFeeRedemptionsEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create fee redemptions events local table: %w", err)
	}
	query = fmt.Sprintf(createFeeRedemptionsEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create fee redemptions events distributed table: %w", err)
	}
	return nil
}

// WriteFeeRedemptionsEvent inserts a single fee redemptions event row into ClickHouse.
func (r *feeRedemptionsEvents) WriteFeeRedemptionsEvent(ctx context.Context, row *FeeRedemptionsEventRow) error {
	chRow, err := convertFeeRedemptionsEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert fee redemptions event row: %w", err)
	}
	query := fmt.Sprintf(writeFeeRedemptionsEventQuery, r.database, r.tableName)
	return r.client.Conn().Exec(ctx, query,
		chRow.BlockchainID,
		bigIntStr(chRow.EVMChainID),
		chRow.BlockNumber,
		chRow.BlockTime,
		chRow.TxHash,
		chRow.TxIndex,
		chRow.LogIndex,
		chRow.ContractAddress,
		chRow.RedeemerAddress,
		chRow.FeeTokenAddress,
		bigIntStr(chRow.Amount),
	)
}

// BatchInsertFeeRedemptionsEvents inserts a batch of fee redemptions event rows into ClickHouse.
func (r *feeRedemptionsEvents) BatchInsertFeeRedemptionsEvents(ctx context.Context, rows []*FeeRedemptionsEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertFeeRedemptionsEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		chRow, err := convertFeeRedemptionsEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert fee redemptions event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append fee redemptions event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send fee redemptions events batch: %w", err)
	}
	return nil
}

// DeleteFeeRedemptionsEvents deletes all fee redemptions events for the given EVM chain ID.
func (r *feeRedemptionsEvents) DeleteFeeRedemptionsEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteFeeRedemptionsEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ FeeRedemptionsEvents = (*feeRedemptionsEvents)(nil)
