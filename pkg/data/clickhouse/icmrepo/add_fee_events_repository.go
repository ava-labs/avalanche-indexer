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

// AddFeeEvents provides methods to write ICM add fee amount events to ClickHouse.
type AddFeeEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteAddFeeEvent(ctx context.Context, row *AddFeeEventRow) error
	BatchInsertAddFeeEvents(ctx context.Context, rows []*AddFeeEventRow) error
	DeleteAddFeeEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/add_fee_events/create-add-fee-events-table-local.sql
var createAddFeeEventsTableLocalQuery string

//go:embed queries/add_fee_events/create-add-fee-events-table.sql
var createAddFeeEventsTableQuery string

//go:embed queries/add_fee_events/write-add-fee-event.sql
var writeAddFeeEventQuery string

//go:embed queries/add_fee_events/batch-insert-add-fee-events.sql
var batchInsertAddFeeEventsQuery string

//go:embed queries/add_fee_events/delete-add-fee-events.sql
var deleteAddFeeEventsQuery string

type addFeeEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chAddFeeEventRow struct {
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

func convertAddFeeEventRow(row *AddFeeEventRow) (*chAddFeeEventRow, error) {
	if row == nil {
		return nil, errors.New("add fee event row is nil")
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
	return &chAddFeeEventRow{
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

// NewAddFeeEvents creates a new add fee events repository and initializes the table.
func NewAddFeeEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (AddFeeEvents, error) {
	repo := &addFeeEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize add fee events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed add_fee_events tables.
func (r *addFeeEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createAddFeeEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create add fee events local table: %w", err)
	}
	query = fmt.Sprintf(createAddFeeEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create add fee events distributed table: %w", err)
	}
	return nil
}

// WriteAddFeeEvent inserts a single add fee event row into ClickHouse.
func (r *addFeeEvents) WriteAddFeeEvent(ctx context.Context, row *AddFeeEventRow) error {
	chRow, err := convertAddFeeEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert add fee event row: %w", err)
	}
	query := fmt.Sprintf(writeAddFeeEventQuery, r.database, r.tableName)
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

// BatchInsertAddFeeEvents inserts a batch of add fee event rows into ClickHouse.
func (r *addFeeEvents) BatchInsertAddFeeEvents(ctx context.Context, rows []*AddFeeEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertAddFeeEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		chRow, err := convertAddFeeEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert add fee event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append add fee event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send add fee events batch: %w", err)
	}
	return nil
}

// DeleteAddFeeEvents deletes all add fee events for the given EVM chain ID.
func (r *addFeeEvents) DeleteAddFeeEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteAddFeeEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ AddFeeEvents = (*addFeeEvents)(nil)
