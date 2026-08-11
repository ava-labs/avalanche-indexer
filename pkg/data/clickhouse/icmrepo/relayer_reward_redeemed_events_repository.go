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

// RelayerRewardRedeemedEvents provides methods to write ICM relayer rewards redeemed events to ClickHouse.
type RelayerRewardRedeemedEvents interface {
	CreateTableIfNotExists(ctx context.Context) error
	WriteRelayerRewardRedeemedEvent(ctx context.Context, row *RelayerRewardRedeemedEventRow) error
	BatchInsertRelayerRewardRedeemedEvents(ctx context.Context, rows []*RelayerRewardRedeemedEventRow) error
	DeleteRelayerRewardRedeemedEvents(ctx context.Context, chainID uint64) error
}

//go:embed queries/relayer_reward_redeemed_events/create-relayer-reward-redeemed-events-table-local.sql
var createRelayerRewardRedeemedEventsTableLocalQuery string

//go:embed queries/relayer_reward_redeemed_events/create-relayer-reward-redeemed-events-table.sql
var createRelayerRewardRedeemedEventsTableQuery string

//go:embed queries/relayer_reward_redeemed_events/write-relayer-reward-redeemed-event.sql
var writeRelayerRewardRedeemedEventQuery string

//go:embed queries/relayer_reward_redeemed_events/batch-insert-relayer-reward-redeemed-events.sql
var batchInsertRelayerRewardRedeemedEventsQuery string

//go:embed queries/relayer_reward_redeemed_events/delete-relayer-reward-redeemed-events.sql
var deleteRelayerRewardRedeemedEventsQuery string

type relayerRewardRedeemedEvents struct {
	client    clickhouse.Client
	cluster   string
	database  string
	tableName string
}

type chRelayerRewardRedeemedEventRow struct {
	BlockchainID    string    `ch:"blockchain_id"`
	EVMChainID      *big.Int  `ch:"evm_chain_id"`
	BlockNumber     uint64    `ch:"block_number"`
	BlockTime       time.Time `ch:"block_time"`
	TxHash          string    `ch:"tx_hash"`
	TxIndex         uint32    `ch:"tx_index"`
	LogIndex        uint32    `ch:"log_index"`
	ContractAddress string    `ch:"contract_address"`
	RedeemerAddress string    `ch:"redeemer_address"`
	FeeTokenAddress string    `ch:"fee_token_address"`
	Amount          *big.Int  `ch:"amount"`
}

func convertRelayerRewardRedeemedEventRow(row *RelayerRewardRedeemedEventRow) (*chRelayerRewardRedeemedEventRow, error) {
	if row == nil {
		return nil, errors.New("relayer reward redeemed event row is nil")
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
	return &chRelayerRewardRedeemedEventRow{
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

// NewRelayerRewardRedeemedEvents creates a new relayer reward redeemed events repository and initializes the table.
func NewRelayerRewardRedeemedEvents(ctx context.Context, client clickhouse.Client, cluster, database, tableName string) (RelayerRewardRedeemedEvents, error) {
	repo := &relayerRewardRedeemedEvents{
		client:    client,
		cluster:   cluster,
		database:  database,
		tableName: tableName,
	}
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize relayer reward redeemed events table: %w", err)
	}
	return repo, nil
}

// CreateTableIfNotExists creates the local and distributed relayer_reward_redeemed_events tables.
func (r *relayerRewardRedeemedEvents) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(createRelayerRewardRedeemedEventsTableLocalQuery, r.database, r.tableName, r.cluster, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create relayer reward redeemed events local table: %w", err)
	}
	query = fmt.Sprintf(createRelayerRewardRedeemedEventsTableQuery, r.database, r.tableName, r.cluster, r.cluster, r.database, r.tableName)
	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create relayer reward redeemed events distributed table: %w", err)
	}
	return nil
}

// WriteRelayerRewardRedeemedEvent inserts a single relayer reward redeemed event row into ClickHouse.
func (r *relayerRewardRedeemedEvents) WriteRelayerRewardRedeemedEvent(ctx context.Context, row *RelayerRewardRedeemedEventRow) error {
	chRow, err := convertRelayerRewardRedeemedEventRow(row)
	if err != nil {
		return fmt.Errorf("failed to convert relayer reward redeemed event row: %w", err)
	}
	query := fmt.Sprintf(writeRelayerRewardRedeemedEventQuery, r.database, r.tableName)
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

// BatchInsertRelayerRewardRedeemedEvents inserts a batch of relayer reward redeemed event rows into ClickHouse.
func (r *relayerRewardRedeemedEvents) BatchInsertRelayerRewardRedeemedEvents(ctx context.Context, rows []*RelayerRewardRedeemedEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	query := fmt.Sprintf(batchInsertRelayerRewardRedeemedEventsQuery, r.database, r.tableName)
	batch, err := r.client.Conn().PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		chRow, err := convertRelayerRewardRedeemedEventRow(row)
		if err != nil {
			return fmt.Errorf("failed to convert relayer reward redeemed event row: %w", err)
		}
		if err := batch.AppendStruct(chRow); err != nil {
			return fmt.Errorf("failed to append relayer reward redeemed event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send relayer reward redeemed events batch: %w", err)
	}
	return nil
}

// DeleteRelayerRewardRedeemedEvents deletes all relayer reward redeemed events for the given EVM chain ID.
func (r *relayerRewardRedeemedEvents) DeleteRelayerRewardRedeemedEvents(ctx context.Context, chainID uint64) error {
	query := fmt.Sprintf(deleteRelayerRewardRedeemedEventsQuery, r.database, r.tableName, r.cluster)
	return r.client.Conn().Exec(ctx, query, chainID)
}

var _ RelayerRewardRedeemedEvents = (*relayerRewardRedeemedEvents)(nil)
