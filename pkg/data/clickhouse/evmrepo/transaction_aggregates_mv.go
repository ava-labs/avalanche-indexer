package evmrepo

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// TransactionAggregatesMV provides methods to create the materialized view for transaction aggregations
type TransactionAggregatesMV interface {
	CreateMaterializedViewIfNotExists(ctx context.Context, sourceTableName, mvTableName string) error
}

type transactionAggregatesMV struct {
	client clickhouse.Client
}

// NewTransactionAggregatesMV creates a new transaction aggregates materialized view manager
func NewTransactionAggregatesMV(client clickhouse.Client) TransactionAggregatesMV {
	return &transactionAggregatesMV{
		client: client,
	}
}

// CreateMaterializedViewIfNotExists creates a materialized view that aggregates transactions by:
// - chain_id: The blockchain/chain identifier (from evm_chain_id)
// - wallet: Wallet addresses (both from_address and to_address when they are wallets)
// - token: Token contract addresses (to_address when it represents a token contract)
// - pair: Trading pair addresses (to_address when it represents a DEX pair)
//
// This creates an incremental materialized view that automatically updates as new transactions are inserted.
// The view aggregates transaction metrics grouped by these dimensions.
func (r *transactionAggregatesMV) CreateMaterializedViewIfNotExists(ctx context.Context, sourceTableName, mvTableName string) error {
	query := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS %s
		ENGINE = SummingMergeTree()
		PARTITION BY (chain_id, partition_month)
		ORDER BY (chain_id, partition_month, wallet_address, token_address, pair_address)
		SETTINGS index_granularity = 8192
		AS
		SELECT
			toString(evm_chain_id) AS chain_id,
			partition_month,
			from_address AS wallet_address,
			to_address AS token_address,
			to_address AS pair_address,
			count() AS transaction_count,
			sum(value) AS total_value,
			sum(gas) AS total_gas,
			sum(gas_price * gas) AS total_gas_cost,
			min(block_time) AS first_seen,
			max(block_time) AS last_seen
		FROM %s
		WHERE to_address IS NOT NULL
		GROUP BY toString(evm_chain_id), partition_month, from_address, to_address
	`, mvTableName, sourceTableName)

	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create transaction aggregates materialized view: %w", err)
	}
	return nil
}
