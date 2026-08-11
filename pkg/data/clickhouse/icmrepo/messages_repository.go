package icmrepo

import (
	"context"
	"errors"
	"fmt"

	_ "embed"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Messages provides methods to write partial rows to the messages table.
// Each method covers exactly the columns produced by one event type; unwritten
// columns retain their zero/NULL aggregate state and are merged by
// AggregatingMergeTree when both consumers have written their halves.
type Messages interface {
	CreateTableIfNotExists(ctx context.Context) error
	WritePartialSend(ctx context.Context, row *MessagePartialSendRow) error
	WritePartialReceive(ctx context.Context, row *MessagePartialReceiveRow) error
	WritePartialExecuted(ctx context.Context, row *MessagePartialExecutedRow) error
	WritePartialExecutionFailed(ctx context.Context, row *MessagePartialExecutionFailedRow) error
	WritePartialReceipt(ctx context.Context, row *MessagePartialReceiptRow) error
}

//go:embed queries/messages/create-messages-table-local.sql
var createMessagesTableLocalQuery string

//go:embed queries/messages/create-messages-table.sql
var createMessagesTableQuery string

//go:embed queries/messages/write-partial-send.sql
var writePartialSendQuery string

//go:embed queries/messages/write-partial-receive.sql
var writePartialReceiveQuery string

//go:embed queries/messages/write-partial-executed.sql
var writePartialExecutedQuery string

//go:embed queries/messages/write-partial-execution-failed.sql
var writePartialExecutionFailedQuery string

//go:embed queries/messages/write-partial-receipt.sql
var writePartialReceiptQuery string

var errNilRow = errors.New("row is nil")

type messages struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

// NewMessages creates a new messages repository and initializes the table.
func NewMessages(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (Messages, error) {
	repo := &messages{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize messages table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed messages tables.
func (r *messages) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createMessagesTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create messages local table: %w", err)
	}
	query = fmt.Sprintf(createMessagesTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create messages distributed table: %w", err)
	}
	return nil
}

// WritePartialSend writes the source-chain columns for a SendCrossChainMessage event.
func (r *messages) WritePartialSend(ctx context.Context, row *MessagePartialSendRow) error {
	if row == nil {
		return errNilRow
	}
	messageID, err := hexToFixed32(row.MessageID)
	if err != nil {
		return fmt.Errorf("message_id: %w", err)
	}
	sourceTxHash, err := hexToFixed32(row.SourceTxHash)
	if err != nil {
		return fmt.Errorf("source_tx_hash: %w", err)
	}
	contractAddress, err := hexToFixed20(row.ContractAddress)
	if err != nil {
		return fmt.Errorf("contract_address: %w", err)
	}
	senderAddress, err := hexToFixed20(row.SenderAddress)
	if err != nil {
		return fmt.Errorf("sender_address: %w", err)
	}
	destinationAddress, err := hexToFixed20(row.DestinationAddress)
	if err != nil {
		return fmt.Errorf("destination_address: %w", err)
	}
	feeTokenAddress, err := hexToFixed20(row.FeeTokenAddress)
	if err != nil {
		return fmt.Errorf("fee_token_address: %w", err)
	}
	allowedRelayerAddresses := row.AllowedRelayerAddresses
	if allowedRelayerAddresses == nil {
		allowedRelayerAddresses = []string{}
	}
	query := fmt.Sprintf(writePartialSendQuery, r.database, r.tableName)
	return r.client.Conn().Exec(ctx, query,
		row.SourceBlockchainID,
		row.DestinationBlockchainID,
		messageID,
		row.SourceBlockTime,
		sourceTxHash,
		bigIntStr(row.EVMChainID),
		contractAddress,
		bigIntStr(row.MessageNonce),
		senderAddress,
		destinationAddress,
		bigIntStr(row.RequiredGasLimit),
		allowedRelayerAddresses,
		feeTokenAddress,
		bigIntStr(row.FeeAmount),
		row.MessageData,
		bigIntPtrStr(row.SourceGasSpent),
		row.MessageReceipts,
	)
}

// WritePartialReceive writes the destination-chain columns for a ReceiveCrossChainMessage event.
func (r *messages) WritePartialReceive(ctx context.Context, row *MessagePartialReceiveRow) error {
	if row == nil {
		return errNilRow
	}
	messageID, err := hexToFixed32(row.MessageID)
	if err != nil {
		return fmt.Errorf("message_id: %w", err)
	}
	receiveTxHash, err := hexToFixed32(row.ReceiveTxHash)
	if err != nil {
		return fmt.Errorf("receive_tx_hash: %w", err)
	}
	delivererAddress, err := hexToFixed20(row.DelivererAddress)
	if err != nil {
		return fmt.Errorf("deliverer_address: %w", err)
	}
	rewardRedeemerAddress, err := hexToFixed20(row.RewardRedeemerAddress)
	if err != nil {
		return fmt.Errorf("reward_redeemer_address: %w", err)
	}
	query := fmt.Sprintf(writePartialReceiveQuery, r.database, r.tableName)
	return r.client.Conn().Exec(ctx, query,
		row.SourceBlockchainID,
		row.DestinationBlockchainID,
		messageID,
		row.ReceiveBlockTime,
		receiveTxHash,
		delivererAddress,
		rewardRedeemerAddress,
		bigIntPtrStr(row.DestinationEVMChainID),
		bigIntPtrStr(row.DestinationGasSpent),
	)
}

// WritePartialExecuted writes the executed_block_time and executed_tx_hash columns
// for a MessageExecuted event.
func (r *messages) WritePartialExecuted(ctx context.Context, row *MessagePartialExecutedRow) error {
	if row == nil {
		return errNilRow
	}
	messageID, err := hexToFixed32(row.MessageID)
	if err != nil {
		return fmt.Errorf("message_id: %w", err)
	}
	executedTxHash, err := hexToFixed32(row.ExecutedTxHash)
	if err != nil {
		return fmt.Errorf("executed_tx_hash: %w", err)
	}
	query := fmt.Sprintf(writePartialExecutedQuery, r.database, r.tableName)
	return r.client.Conn().Exec(ctx, query,
		row.SourceBlockchainID,
		row.DestinationBlockchainID,
		messageID,
		row.ExecutedBlockTime,
		executedTxHash,
	)
}

// WritePartialExecutionFailed writes the last_execution_failed_time column
// for a MessageExecutionFailed event.
func (r *messages) WritePartialExecutionFailed(ctx context.Context, row *MessagePartialExecutionFailedRow) error {
	if row == nil {
		return errNilRow
	}
	messageID, err := hexToFixed32(row.MessageID)
	if err != nil {
		return fmt.Errorf("message_id: %w", err)
	}
	query := fmt.Sprintf(writePartialExecutionFailedQuery, r.database, r.tableName)
	return r.client.Conn().Exec(ctx, query,
		row.SourceBlockchainID,
		row.DestinationBlockchainID,
		messageID,
		row.LastExecutionFailedTime,
	)
}

// WritePartialReceipt writes the receipt_delivered column for a ReceiptReceived event.
func (r *messages) WritePartialReceipt(ctx context.Context, row *MessagePartialReceiptRow) error {
	if row == nil {
		return errNilRow
	}
	messageID, err := hexToFixed32(row.MessageID)
	if err != nil {
		return fmt.Errorf("message_id: %w", err)
	}
	query := fmt.Sprintf(writePartialReceiptQuery, r.database, r.tableName)
	return r.client.Conn().Exec(ctx, query,
		row.SourceBlockchainID,
		row.DestinationBlockchainID,
		messageID,
		row.ReceiptDelivered,
	)
}

var _ Messages = (*messages)(nil)
