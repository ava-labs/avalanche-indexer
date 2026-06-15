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

// SendEvents provides methods to write ICM send events to ClickHouse.
type SendEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteSendEvent(ctx context.Context, row *SendEventRow) error
	BatchInsertSendEvents(ctx context.Context, rows []*SendEventRow) error
	DeleteSendEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/send_events/create-send-events-table-local.sql
var createSendEventsTableLocalQuery string

//go:embed queries/send_events/create-send-events-table.sql
var createSendEventsTableQuery string

//go:embed queries/send_events/write-send-event.sql
var writeSendEventQuery string

//go:embed queries/send_events/batch-insert-send-events.sql
var batchInsertSendEventsQuery string

//go:embed queries/send_events/delete-send-events.sql
var deleteSendEventsQuery string

type sendEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chSendEventRow struct {
	BlockchainID             string     `ch:"blockchain_id"`
	EVMChainID               *big.Int   `ch:"evm_chain_id"`
	BlockNumber              uint64     `ch:"block_number"`
	BlockTime                time.Time  `ch:"block_time"`
	TxHash                   string     `ch:"tx_hash"`
	TxIndex                  uint32     `ch:"tx_index"`
	LogIndex                 uint32     `ch:"log_index"`
	ContractAddress          string     `ch:"contract_address"`
	MessageID                string     `ch:"message_id"`
	DestinationBlockchainID  string     `ch:"destination_blockchain_id"`
	SenderAddress            string     `ch:"sender_address"`
	DestinationAddress       string     `ch:"destination_address"`
	RequiredGasLimit         *big.Int   `ch:"required_gas_limit"`
	AllowedRelayerAddresses  []string   `ch:"allowed_relayer_addresses"`
	FeeTokenAddress          string     `ch:"fee_token_address"`
	FeeAmount                *big.Int   `ch:"fee_amount"`
	MessageNonce             *big.Int   `ch:"message_nonce"`
	MessageData              string     `ch:"message_data"`
	ReceiptsMessageNonces    []*big.Int `ch:"receipts_message_nonces"`
	ReceiptsRelayerAddresses []string   `ch:"receipts_relayer_addresses"`
}

func convertSendEventRow(row *SendEventRow) (*chSendEventRow, error) {
	if row == nil {
		return nil, errors.New("send event row is nil")
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
	senderAddress, err := hexToFixed20(row.SenderAddress)
	if err != nil {
		return nil, fmt.Errorf("sender_address: %w", err)
	}
	destinationAddress, err := hexToFixed20(row.DestinationAddress)
	if err != nil {
		return nil, fmt.Errorf("destination_address: %w", err)
	}
	feeTokenAddress, err := hexToFixed20(row.FeeTokenAddress)
	if err != nil {
		return nil, fmt.Errorf("fee_token_address: %w", err)
	}
	receiptsRelayerAddresses, err := hexAddressesToBinary(row.ReceiptsRelayerAddresses)
	if err != nil {
		return nil, fmt.Errorf("receipts_relayer_addresses: %w", err)
	}
	allowedRelayerAddresses := row.AllowedRelayerAddresses
	if allowedRelayerAddresses == nil {
		allowedRelayerAddresses = []string{}
	}
	return &chSendEventRow{
		BlockchainID:             row.BlockchainID,
		EVMChainID:               bigIntOrZero(row.EVMChainID),
		BlockNumber:              row.BlockNumber,
		BlockTime:                row.BlockTime,
		TxHash:                   txHash,
		TxIndex:                  row.TxIndex,
		LogIndex:                 row.LogIndex,
		ContractAddress:          contractAddress,
		MessageID:                messageID,
		DestinationBlockchainID:  row.DestinationBlockchainID,
		SenderAddress:            senderAddress,
		DestinationAddress:       destinationAddress,
		RequiredGasLimit:         bigIntOrZero(row.RequiredGasLimit),
		AllowedRelayerAddresses:  allowedRelayerAddresses,
		FeeTokenAddress:          feeTokenAddress,
		FeeAmount:                bigIntOrZero(row.FeeAmount),
		MessageNonce:             bigIntOrZero(row.MessageNonce),
		MessageData:              string(row.MessageData),
		ReceiptsMessageNonces:    bigIntsOrZero(row.ReceiptsMessageNonces),
		ReceiptsRelayerAddresses: receiptsRelayerAddresses,
	}, nil
}

// NewSendEvents creates a new send events repository and initializes the table.
func NewSendEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (SendEvents, error) {
	repo := &sendEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize send events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed icm_send_events tables.
func (r *sendEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createSendEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create send events local table: %w", err)
	}
	query = fmt.Sprintf(createSendEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create send events distributed table: %w", err)
	}
	return nil
}

// WriteSendEvent inserts a single send event row into ClickHouse.
func (r *sendEvents) WriteSendEvent(ctx context.Context, row *SendEventRow) error {
	chRow, err := convertSendEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert send event row: %w", err)
	}
	query := fmt.Sprintf(writeSendEventQuery, r.database, r.tableName)
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
		chRow.SenderAddress,
		chRow.DestinationAddress,
		bigIntStr(chRow.RequiredGasLimit),
		chRow.AllowedRelayerAddresses,
		chRow.FeeTokenAddress,
		bigIntStr(chRow.FeeAmount),
		bigIntStr(chRow.MessageNonce),
		chRow.MessageData,
		chRow.ReceiptsMessageNonces,
		chRow.ReceiptsRelayerAddresses,
	)
}

// BatchInsertSendEvents inserts a batch of send event rows into ClickHouse.
func (r *sendEvents) BatchInsertSendEvents(ctx context.Context, rows []*SendEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertSendEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		chRow, err := convertSendEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert send event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append send event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send send events batch: %w", err)
	}
	return nil
}

// DeleteSendEvents deletes all send events for the given EVM chain ID.
func (r *sendEvents) DeleteSendEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteSendEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ SendEvents = (*sendEvents)(nil)
