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

// ReceiveEvents provides methods to write ICM receive events to ClickHouse.
type ReceiveEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteReceiveEvent(ctx context.Context, row *ReceiveEventRow) error
	BatchInsertReceiveEvents(ctx context.Context, rows []*ReceiveEventRow) error
	DeleteReceiveEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/receive_events/create-receive-events-table-local.sql
var createReceiveEventsTableLocalQuery string

//go:embed queries/receive_events/create-receive-events-table.sql
var createReceiveEventsTableQuery string

//go:embed queries/receive_events/write-receive-event.sql
var writeReceiveEventQuery string

//go:embed queries/receive_events/batch-insert-receive-events.sql
var batchInsertReceiveEventsQuery string

//go:embed queries/receive_events/delete-receive-events.sql
var deleteReceiveEventsQuery string

type receiveEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chReceiveEventRow struct {
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
	DelivererAddress         string     `ch:"deliverer_address"`
	RewardRedeemerAddress    string     `ch:"reward_redeemer_address"`
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

func convertReceiveEventRow(row *ReceiveEventRow) (*chReceiveEventRow, error) {
	if row == nil {
		return nil, errors.New("receive event row is nil")
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
	delivererAddress, err := hexToFixed20(row.DelivererAddress)
	if err != nil {
		return nil, fmt.Errorf("deliverer_address: %w", err)
	}
	rewardRedeemerAddress, err := hexToFixed20(row.RewardRedeemerAddress)
	if err != nil {
		return nil, fmt.Errorf("reward_redeemer_address: %w", err)
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
	return &chReceiveEventRow{
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
		DelivererAddress:         delivererAddress,
		RewardRedeemerAddress:    rewardRedeemerAddress,
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

// NewReceiveEvents creates a new receive events repository and initializes the table.
func NewReceiveEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (ReceiveEvents, error) {
	repo := &receiveEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize receive events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed icm_receive_events tables.
func (r *receiveEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createReceiveEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create receive events local table: %w", err)
	}
	query = fmt.Sprintf(createReceiveEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create receive events distributed table: %w", err)
	}
	return nil
}

// WriteReceiveEvent inserts a single receive event row into ClickHouse.
func (r *receiveEvents) WriteReceiveEvent(ctx context.Context, row *ReceiveEventRow) error {
	chRow, err := convertReceiveEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert receive event row: %w", err)
	}
	query := fmt.Sprintf(writeReceiveEventQuery, r.database, r.tableName)
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
		chRow.DelivererAddress,
		chRow.RewardRedeemerAddress,
		bigIntStr(chRow.MessageNonce),
		chRow.OriginSenderAddress,
		chRow.DestinationBlockchainID,
		chRow.DestinationAddress,
		bigIntStr(chRow.RequiredGasLimit),
		chRow.AllowedRelayerAddresses,
		chRow.MessageData,
		chRow.ReceiptsMessageNonces,
		chRow.ReceiptsRelayerAddresses,
	)
}

// BatchInsertReceiveEvents inserts a batch of receive event rows into ClickHouse.
func (r *receiveEvents) BatchInsertReceiveEvents(ctx context.Context, rows []*ReceiveEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertReceiveEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		chRow, err := convertReceiveEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert receive event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append receive event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send receive events batch: %w", err)
	}
	return nil
}

// DeleteReceiveEvents deletes all receive events for the given EVM chain ID.
func (r *receiveEvents) DeleteReceiveEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteReceiveEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ ReceiveEvents = (*receiveEvents)(nil)
