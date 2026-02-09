CREATE TABLE IF NOT EXISTS %s.%s_local
ON CLUSTER %s
(
	blockchain_id String,
	evm_chain_id UInt256,
	block_number UInt64,
	hash FixedString(32),
	parent_hash FixedString(32),
	block_time DateTime64(3, 'UTC'),
	miner FixedString(20),
	difficulty UInt256,
	total_difficulty UInt256,
	size UInt64,
	gas_limit UInt64,
	gas_used UInt64,
	base_fee_per_gas UInt256,
	block_gas_cost UInt256,
	state_root FixedString(32),
	transactions_root FixedString(32),
	receipts_root FixedString(32),
	extra_data String,
	block_extra_data String,
	ext_data_hash FixedString(32),
	ext_data_gas_used UInt32,
	mix_hash FixedString(32),
	nonce Nullable(FixedString(8)),
	sha3_uncles FixedString(32),
	uncles Array(FixedString(32)),
	blob_gas_used UInt64,
	excess_blob_gas UInt64,
	parent_beacon_block_root Nullable(FixedString(32)),
	min_delay_excess UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/%s_local', '{replica}')
ORDER BY (blockchain_id, block_time, block_number)
SETTINGS index_granularity = 8192
