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

// MessageExecutedEvents provides methods to write ICM message executed events to ClickHouse.
type MessageExecutedEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteMessageExecutedEvent(ctx context.Context, row *MessageExecutedEventRow) error
	BatchInsertMessageExecutedEvents(ctx context.Context, rows []*MessageExecutedEventRow) error
	DeleteMessageExecutedEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/message_executed_events/create-message-executed-events-table-local.sql
var createMessageExecutedEventsTableLocalQuery string

//go:embed queries/message_executed_events/create-message-executed-events-table.sql
var createMessageExecutedEventsTableQuery string

//go:embed queries/message_executed_events/write-message-executed-event.sql
var writeMessageExecutedEventQuery string

//go:embed queries/message_executed_events/batch-insert-message-executed-events.sql
var batchInsertMessageExecutedEventsQuery string

//go:embed queries/message_executed_events/delete-message-executed-events.sql
var deleteMessageExecutedEventsQuery string

type messageExecutedEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chMessageExecutedEventRow struct {
	BlockchainID       string    `ch:"blockchain_id"`
	EVMChainID         *big.Int  `ch:"evm_chain_id"`
	BlockNumber        uint64    `ch:"block_number"`
	BlockTime          time.Time `ch:"block_time"`
	TxHash             string    `ch:"tx_hash"`
	TxIndex            uint32    `ch:"tx_index"`
	LogIndex           uint32    `ch:"log_index"`
	ContractAddress    string    `ch:"contract_address"`
	MessageID          string    `ch:"message_id"`
	SourceBlockchainID string    `ch:"source_blockchain_id"`
}

func convertMessageExecutedEventRow(row *MessageExecutedEventRow) (*chMessageExecutedEventRow, error) {
	if row == nil {
		return nil, errors.New("message executed event row is nil")
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
	return &chMessageExecutedEventRow{
		BlockchainID:       row.BlockchainID,
		EVMChainID:         bigIntOrZero(row.EVMChainID),
		BlockNumber:        row.BlockNumber,
		BlockTime:          row.BlockTime,
		TxHash:             txHash,
		TxIndex:            row.TxIndex,
		LogIndex:           row.LogIndex,
		ContractAddress:    contractAddress,
		MessageID:          messageID,
		SourceBlockchainID: row.SourceBlockchainID,
	}, nil
}

// NewMessageExecutedEvents creates a new message executed events repository and initializes the table.
func NewMessageExecutedEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (MessageExecutedEvents, error) {
	repo := &messageExecutedEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize message executed events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed icm_message_executed_events tables.
func (r *messageExecutedEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createMessageExecutedEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create message executed events local table: %w", err)
	}
	query = fmt.Sprintf(createMessageExecutedEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create message executed events distributed table: %w", err)
	}
	return nil
}

// WriteMessageExecutedEvent inserts a single message executed event row into ClickHouse.
func (r *messageExecutedEvents) WriteMessageExecutedEvent(ctx context.Context, row *MessageExecutedEventRow) error {
	chRow, err := convertMessageExecutedEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert message executed event row: %w", err)
	}
	query := fmt.Sprintf(writeMessageExecutedEventQuery, r.database, r.tableName)
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
		chRow.SourceBlockchainID,
	)
}

// BatchInsertMessageExecutedEvents inserts a batch of message executed event rows into ClickHouse.
func (r *messageExecutedEvents) BatchInsertMessageExecutedEvents(ctx context.Context, rows []*MessageExecutedEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertMessageExecutedEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		chRow, err := convertMessageExecutedEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert message executed event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append message executed event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send message executed events batch: %w", err)
	}
	return nil
}

// DeleteMessageExecutedEvents deletes all message executed events for the given EVM chain ID.
func (r *messageExecutedEvents) DeleteMessageExecutedEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteMessageExecutedEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ MessageExecutedEvents = (*messageExecutedEvents)(nil)
