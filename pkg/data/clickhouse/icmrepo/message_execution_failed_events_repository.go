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

// MessageExecutionFailedEvents provides methods to write ICM message execution failed events to ClickHouse.
type MessageExecutionFailedEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteMessageExecutionFailedEvent(ctx context.Context, row *MessageExecutionFailedEventRow) error
	BatchInsertMessageExecutionFailedEvents(ctx context.Context, rows []*MessageExecutionFailedEventRow) error
	DeleteMessageExecutionFailedEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/message_execution_failed_events/create-message-execution-failed-events-table-local.sql
var createMessageExecutionFailedEventsTableLocalQuery string

//go:embed queries/message_execution_failed_events/create-message-execution-failed-events-table.sql
var createMessageExecutionFailedEventsTableQuery string

//go:embed queries/message_execution_failed_events/write-message-execution-failed-event.sql
var writeMessageExecutionFailedEventQuery string

//go:embed queries/message_execution_failed_events/batch-insert-message-execution-failed-events.sql
var batchInsertMessageExecutionFailedEventsQuery string

//go:embed queries/message_execution_failed_events/delete-message-execution-failed-events.sql
var deleteMessageExecutionFailedEventsQuery string

type messageExecutionFailedEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chMessageExecutionFailedEventRow struct {
	BlockchainID             string     `ch:"blockchain_id"`
	EVMChainID               *big.Int   `ch:"evm_chain_id"`
	BlockNumber              uint64     `ch:"block_number"`
	BlockTime                time.Time  `ch:"block_time"`
	TxHash                   string     `ch:"tx_hash"`
	TxIndex                  uint32     `ch:"tx_index"`
	LogIndex                 uint32     `ch:"log_index"`
	ContractAddress          string     `ch:"contract_address"`
	MessageID                string     `ch:"message_id"`
	SourceBlockchainID       string     `ch:"source_blockchain_id"`
	MessageNonce             *big.Int   `ch:"message_nonce"`
	OriginSenderAddress      string     `ch:"origin_sender_address"`
	DestinationBlockchainID  string     `ch:"destination_blockchain_id"`
	DestinationAddress       string     `ch:"destination_address"`
	RequiredGasLimit         *big.Int   `ch:"required_gas_limit"`
	AllowedRelayerAddresses  []string   `ch:"allowed_relayer_addresses"`
	MessageData              string     `ch:"message_data"`
	ReceiptsMessageNonces    []*big.Int `ch:"receipts_message_nonces"`
	ReceiptsRelayerAddresses []string   `ch:"receipts_relayer_addresses"`
}

func convertMessageExecutionFailedEventRow(row *MessageExecutionFailedEventRow) (*chMessageExecutionFailedEventRow, error) {
	if row == nil {
		return nil, errors.New("message execution failed event row is nil")
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
	originSenderAddress, err := hexToFixed20(row.OriginSenderAddress)
	if err != nil {
		return nil, fmt.Errorf("origin_sender_address: %w", err)
	}
	destinationAddress, err := hexToFixed20(row.DestinationAddress)
	if err != nil {
		return nil, fmt.Errorf("destination_address: %w", err)
	}
	receiptsRelayerAddresses, err := hexAddressesToBinary(row.ReceiptsRelayerAddresses)
	if err != nil {
		return nil, fmt.Errorf("receipts_relayer_addresses: %w", err)
	}
	allowedRelayerAddresses := row.AllowedRelayerAddresses
	if allowedRelayerAddresses == nil {
		allowedRelayerAddresses = []string{}
	}
	return &chMessageExecutionFailedEventRow{
		BlockchainID:             row.BlockchainID,
		EVMChainID:               bigIntOrZero(row.EVMChainID),
		BlockNumber:              row.BlockNumber,
		BlockTime:                row.BlockTime,
		TxHash:                   txHash,
		TxIndex:                  row.TxIndex,
		LogIndex:                 row.LogIndex,
		ContractAddress:          contractAddress,
		MessageID:                messageID,
		SourceBlockchainID:       row.SourceBlockchainID,
		MessageNonce:             bigIntOrZero(row.MessageNonce),
		OriginSenderAddress:      originSenderAddress,
		DestinationBlockchainID:  row.DestinationBlockchainID,
		DestinationAddress:       destinationAddress,
		RequiredGasLimit:         bigIntOrZero(row.RequiredGasLimit),
		AllowedRelayerAddresses:  allowedRelayerAddresses,
		MessageData:              string(row.MessageData),
		ReceiptsMessageNonces:    bigIntsOrZero(row.ReceiptsMessageNonces),
		ReceiptsRelayerAddresses: receiptsRelayerAddresses,
	}, nil
}

// NewMessageExecutionFailedEvents creates a new message execution failed events repository and initializes the table.
func NewMessageExecutionFailedEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (MessageExecutionFailedEvents, error) {
	repo := &messageExecutionFailedEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize message execution failed events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed icm_message_execution_failed_events tables.
func (r *messageExecutionFailedEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createMessageExecutionFailedEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create message execution failed events local table: %w", err)
	}
	query = fmt.Sprintf(createMessageExecutionFailedEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create message execution failed events distributed table: %w", err)
	}
	return nil
}

// WriteMessageExecutionFailedEvent inserts a single message execution failed event row into ClickHouse.
func (r *messageExecutionFailedEvents) WriteMessageExecutionFailedEvent(ctx context.Context, row *MessageExecutionFailedEventRow) error {
	chRow, err := convertMessageExecutionFailedEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert message execution failed event row: %w", err)
	}
	query := fmt.Sprintf(writeMessageExecutionFailedEventQuery, r.database, r.tableName)
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
		bigIntStr(chRow.MessageNonce),
		chRow.OriginSenderAddress,
		chRow.DestinationBlockchainID,
		chRow.DestinationAddress,
		bigIntStr(chRow.RequiredGasLimit),
		chRow.AllowedRelayerAddresses,
		chRow.MessageData,
		bigIntStrs(chRow.ReceiptsMessageNonces),
		chRow.ReceiptsRelayerAddresses,
	)
}

// BatchInsertMessageExecutionFailedEvents inserts a batch of message execution failed event rows into ClickHouse.
func (r *messageExecutionFailedEvents) BatchInsertMessageExecutionFailedEvents(ctx context.Context, rows []*MessageExecutionFailedEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertMessageExecutionFailedEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		chRow, err := convertMessageExecutionFailedEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert message execution failed event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append message execution failed event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send message execution failed events batch: %w", err)
	}
	return nil
}

// DeleteMessageExecutionFailedEvents deletes all message execution failed events for the given EVM chain ID.
func (r *messageExecutionFailedEvents) DeleteMessageExecutionFailedEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteMessageExecutionFailedEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ MessageExecutionFailedEvents = (*messageExecutionFailedEvents)(nil)
