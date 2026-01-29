package evmrepo

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// LogAggregatesMV provides methods to create the materialized view for log aggregations
type LogAggregatesMV interface {
	CreateMaterializedViewIfNotExists(ctx context.Context, sourceTableName, mvTableName string) error
}

type logAggregatesMV struct {
	client clickhouse.Client
}

// NewLogAggregatesMV creates a new log aggregates materialized view manager
func NewLogAggregatesMV(client clickhouse.Client) LogAggregatesMV {
	return &logAggregatesMV{
		client: client,
	}
}

// CreateMaterializedViewIfNotExists creates a materialized view that aggregates ERC20 Transfer events
// to produce token/address pairs for balance lookups.
//
// Ethereum log structure for Transfer events:
//   - topic0: Transfer event signature (0xddf252ad...)
//   - topic1: from_address (20 bytes, padded to 32 bytes)
//   - topic2: to_address (20 bytes, padded to 32 bytes)
//   - data: transfer value (uint256)
//
// The view extracts both from and to addresses (via UNION ALL) and groups by:
//   - chain_id, partition_date, token_contract, wallet_address
//
// This produces rows suitable for querying which token/address pairs need balance lookups.
func (r *logAggregatesMV) CreateMaterializedViewIfNotExists(ctx context.Context, sourceTableName, mvTableName string) error {
	// ERC20 Transfer event signature: keccak256("Transfer(address,address,uint256)")
	transferEventSignatureHex := "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

	query := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS %s
		ENGINE = SummingMergeTree()
		PARTITION BY (chain_id, partition_date)
		ORDER BY (chain_id, partition_date, token_contract, wallet_address)
		SETTINGS index_granularity = 8192, allow_nullable_key = 1
		AS
		SELECT
			toString(evm_chain_id) AS chain_id,
			toDate(block_time) AS partition_date,
			address AS token_contract,
			wallet_address,
			count() AS transfer_count,
			uniqExact(tx_hash) AS unique_tx_count,
			uniqExact(block_number) AS unique_block_count,
			min(block_time) AS first_seen,
			max(block_time) AS last_seen
		FROM (
			-- Extract from_address from topic1
			SELECT evm_chain_id, block_time, address, tx_hash, block_number,
				lower(hex(substring(topic1, 13, 20))) AS wallet_address
			FROM %s
			WHERE topic0 = unhex('%s') AND topic1 IS NOT NULL
			
			UNION ALL
			
			-- Extract to_address from topic2
			SELECT evm_chain_id, block_time, address, tx_hash, block_number,
				lower(hex(substring(topic2, 13, 20))) AS wallet_address
			FROM %s
			WHERE topic0 = unhex('%s') AND topic2 IS NOT NULL
		)
		GROUP BY toString(evm_chain_id), toDate(block_time), address, wallet_address
	`, mvTableName, sourceTableName, transferEventSignatureHex, sourceTableName, transferEventSignatureHex)

	if err := r.client.Conn().Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create log aggregates materialized view: %w", err)
	}
	return nil
}
