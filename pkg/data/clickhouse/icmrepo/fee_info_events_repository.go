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

// FeeInfoEvents provides methods to write ICM add fee amount events to ClickHouse.
type FeeInfoEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteFeeInfoEvent(ctx context.Context, row *FeeInfoEventRow) error
	BatchInsertFeeInfoEvents(ctx context.Context, rows []*FeeInfoEventRow) error
	DeleteFeeInfoEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/fee_info_events/create-fee-info-events-table-local.sql
var createFeeInfoEventsTableLocalQuery string

//go:embed queries/fee_info_events/create-fee-info-events-table.sql
var createFeeInfoEventsTableQuery string

//go:embed queries/fee_info_events/write-fee-info-event.sql
var writeFeeInfoEventQuery string

//go:embed queries/fee_info_events/batch-insert-fee-info-events.sql
var batchInsertFeeInfoEventsQuery string

//go:embed queries/fee_info_events/delete-fee-info-events.sql
var deleteFeeInfoEventsQuery string

type feeInfoEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chFeeInfoEventRow struct {
	BlockchainID            string    `ch:"blockchain_id"`
	EVMChainID              *big.Int  `ch:"evm_chain_id"`
	BlockNumber             uint64    `ch:"block_number"`
	BlockTime               time.Time `ch:"block_time"`
	TxHash                  string    `ch:"tx_hash"`
	TxIndex                 uint32    `ch:"tx_index"`
	LogIndex                uint32    `ch:"log_index"`
	ContractAddress         string    `ch:"contract_address"`
	MessageID               string    `ch:"message_id"`
	DestinationBlockchainID string    `ch:"destination_blockchain_id"`
	FeeTokenAddress         string    `ch:"fee_token_address"`
	AdditionalFeeAmount     *big.Int  `ch:"additional_fee_amount"`
}

func convertFeeInfoEventRow(row *FeeInfoEventRow) (*chFeeInfoEventRow, error) {
	if row == nil {
		return nil, errors.New("fee info event row is nil")
	}
	txHash, err := hexToFixed32(row.TxHash)
	if err != nil {
		return nil, fmt.Errorf("tx_hash: %w", err)
	}
	contractAddress, err := hexToFixed20(row.ContractAddress)
	if err != nil {
		return nil, fmt.Errorf("contract_address: %w", err)
	}
	messageID, err := hexToFixed32(row.MessageID)
	if err != nil {
		return nil, fmt.Errorf("message_id: %w", err)
	}
	feeTokenAddress, err := hexToFixed20(row.FeeTokenAddress)
	if err != nil {
		return nil, fmt.Errorf("fee_token_address: %w", err)
	}
	return &chFeeInfoEventRow{
		BlockchainID:            row.BlockchainID,
		EVMChainID:              bigIntOrZero(row.EVMChainID),
		BlockNumber:             row.BlockNumber,
		BlockTime:               row.BlockTime,
		TxHash:                  txHash,
		TxIndex:                 row.TxIndex,
		LogIndex:                row.LogIndex,
		ContractAddress:         contractAddress,
		MessageID:               messageID,
		DestinationBlockchainID: row.DestinationBlockchainID,
		FeeTokenAddress:         feeTokenAddress,
		AdditionalFeeAmount:     bigIntOrZero(row.AdditionalFeeAmount),
	}, nil
}

// NewFeeInfoEvents creates a new fee info events repository and initializes the table.
func NewFeeInfoEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (FeeInfoEvents, error) {
	repo := &feeInfoEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize fee info events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed icm_fee_info_events tables.
func (r *feeInfoEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createFeeInfoEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create fee info events local table: %w", err)
	}
	query = fmt.Sprintf(createFeeInfoEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create fee info events distributed table: %w", err)
	}
	return nil
}

// WriteFeeInfoEvent inserts a single fee info event row into ClickHouse.
func (r *feeInfoEvents) WriteFeeInfoEvent(ctx context.Context, row *FeeInfoEventRow) error {
	chRow, err := convertFeeInfoEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert fee info event row: %w", err)
	}
	query := fmt.Sprintf(writeFeeInfoEventQuery, r.database, r.tableName)
	return r.client.Conn().Exec(ctx, query,
		chRow.BlockchainID,
		bigIntStr(chRow.EVMChainID),
		chRow.BlockNumber,
		chRow.BlockTime,
		chRow.TxHash,
		chRow.TxIndex,
		chRow.LogIndex,
		chRow.ContractAddress,
		chRow.MessageID,
		chRow.DestinationBlockchainID,
		chRow.FeeTokenAddress,
		bigIntStr(chRow.AdditionalFeeAmount),
	)
}

// BatchInsertFeeInfoEvents inserts a batch of fee info event rows into ClickHouse.
func (r *feeInfoEvents) BatchInsertFeeInfoEvents(ctx context.Context, rows []*FeeInfoEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertFeeInfoEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		chRow, err := convertFeeInfoEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert fee info event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append fee info event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send fee info events batch: %w", err)
	}
	return nil
}

// DeleteFeeInfoEvents deletes all fee info events for the given EVM chain ID.
func (r *feeInfoEvents) DeleteFeeInfoEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteFeeInfoEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ FeeInfoEvents = (*feeInfoEvents)(nil)
