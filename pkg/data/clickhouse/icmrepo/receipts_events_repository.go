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

// ReceiptsEvents provides methods to write ICM receipt received events to ClickHouse.
type ReceiptsEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteReceiptsEvent(ctx context.Context, row *ReceiptsEventRow) error
	BatchInsertReceiptsEvents(ctx context.Context, rows []*ReceiptsEventRow) error
	DeleteReceiptsEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/receipts_events/create-receipts-events-table-local.sql
var createReceiptsEventsTableLocalQuery string

//go:embed queries/receipts_events/create-receipts-events-table.sql
var createReceiptsEventsTableQuery string

//go:embed queries/receipts_events/write-receipts-event.sql
var writeReceiptsEventQuery string

//go:embed queries/receipts_events/batch-insert-receipts-events.sql
var batchInsertReceiptsEventsQuery string

//go:embed queries/receipts_events/delete-receipts-events.sql
var deleteReceiptsEventsQuery string

type receiptsEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chReceiptsEventRow struct {
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
	RelayerRewardAddress    string    `ch:"relayer_reward_address"`
	FeeTokenAddress         string    `ch:"fee_token_address"`
	FeeAmount               *big.Int  `ch:"fee_amount"`
}

func convertReceiptsEventRow(row *ReceiptsEventRow) (*chReceiptsEventRow, error) {
	if row == nil {
		return nil, errors.New("receipts event row is nil")
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
	relayerRewardAddress, err := hexToFixed20(row.RelayerRewardAddress)
	if err != nil {
		return nil, fmt.Errorf("relayer_reward_address: %w", err)
	}
	feeTokenAddress, err := hexToFixed20(row.FeeTokenAddress)
	if err != nil {
		return nil, fmt.Errorf("fee_token_address: %w", err)
	}
	return &chReceiptsEventRow{
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
		RelayerRewardAddress:    relayerRewardAddress,
		FeeTokenAddress:         feeTokenAddress,
		FeeAmount:               bigIntOrZero(row.FeeAmount),
	}, nil
}

// NewReceiptsEvents creates a new receipts events repository and initializes the table.
func NewReceiptsEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (ReceiptsEvents, error) {
	repo := &receiptsEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize receipts events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed icm_receipts_events tables.
func (r *receiptsEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createReceiptsEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create receipts events local table: %w", err)
	}
	query = fmt.Sprintf(createReceiptsEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create receipts events distributed table: %w", err)
	}
	return nil
}

// WriteReceiptsEvent inserts a single receipts event row into ClickHouse.
func (r *receiptsEvents) WriteReceiptsEvent(ctx context.Context, row *ReceiptsEventRow) error {
	chRow, err := convertReceiptsEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert receipts event row: %w", err)
	}
	query := fmt.Sprintf(writeReceiptsEventQuery, r.database, r.tableName)
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
		chRow.RelayerRewardAddress,
		chRow.FeeTokenAddress,
		bigIntStr(chRow.FeeAmount),
	)
}

// BatchInsertReceiptsEvents inserts a batch of receipts event rows into ClickHouse.
func (r *receiptsEvents) BatchInsertReceiptsEvents(ctx context.Context, rows []*ReceiptsEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertReceiptsEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		chRow, err := convertReceiptsEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert receipts event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append receipts event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send receipts events batch: %w", err)
	}
	return nil
}

// DeleteReceiptsEvents deletes all receipts events for the given EVM chain ID.
func (r *receiptsEvents) DeleteReceiptsEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteReceiptsEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ ReceiptsEvents = (*receiptsEvents)(nil)
