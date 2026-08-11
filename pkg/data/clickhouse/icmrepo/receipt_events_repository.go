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

// ReceiptEvents provides methods to write ICM receipt received events to ClickHouse.
type ReceiptEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteReceiptEvent(ctx context.Context, row *ReceiptEventRow) error
	BatchInsertReceiptEvents(ctx context.Context, rows []*ReceiptEventRow) error
	DeleteReceiptEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/receipt_events/create-receipt-events-table-local.sql
var createReceiptEventsTableLocalQuery string

//go:embed queries/receipt_events/create-receipt-events-table.sql
var createReceiptEventsTableQuery string

//go:embed queries/receipt_events/write-receipt-event.sql
var writeReceiptEventQuery string

//go:embed queries/receipt_events/batch-insert-receipt-events.sql
var batchInsertReceiptEventsQuery string

//go:embed queries/receipt_events/delete-receipt-events.sql
var deleteReceiptEventsQuery string

type receiptEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chReceiptEventRow struct {
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

func convertReceiptEventRow(row *ReceiptEventRow) (*chReceiptEventRow, error) {
	if row == nil {
		return nil, errors.New("receipt event row is nil")
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
	return &chReceiptEventRow{
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

// NewReceiptEvents creates a new receipt events repository and initializes the table.
func NewReceiptEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (ReceiptEvents, error) {
	repo := &receiptEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize receipt events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed receipt_events tables.
func (r *receiptEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createReceiptEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create receipt events local table: %w", err)
	}
	query = fmt.Sprintf(createReceiptEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create receipt events distributed table: %w", err)
	}
	return nil
}

// WriteReceiptEvent inserts a single receipt event row into ClickHouse.
func (r *receiptEvents) WriteReceiptEvent(ctx context.Context, row *ReceiptEventRow) error {
	chRow, err := convertReceiptEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert receipt event row: %w", err)
	}
	query := fmt.Sprintf(writeReceiptEventQuery, r.database, r.tableName)
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

// BatchInsertReceiptEvents inserts a batch of receipt event rows into ClickHouse.
func (r *receiptEvents) BatchInsertReceiptEvents(ctx context.Context, rows []*ReceiptEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertReceiptEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		chRow, err := convertReceiptEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert receipt event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append receipt event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send receipt events batch: %w", err)
	}
	return nil
}

// DeleteReceiptEvents deletes all receipt events for the given EVM chain ID.
func (r *receiptEvents) DeleteReceiptEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteReceiptEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ ReceiptEvents = (*receiptEvents)(nil)
