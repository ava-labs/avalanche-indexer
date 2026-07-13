CREATE TABLE IF NOT EXISTS `%s`.`%s_local`
ON CLUSTER `%s`
(
	blockchain_id String,
	evm_chain_id UInt256,
	block_number UInt64,
	block_hash FixedString(32),
	block_time DateTime64(3, 'UTC'),
	timestamp_ms UInt64,
	hash FixedString(32),
	from_address FixedString(20),
	to_address Nullable(FixedString(20)),
	nonce UInt64,
	value UInt256,
	gas UInt64,
	gas_used UInt64,
	effective_gas_price UInt256,
	gas_price UInt256,
	max_fee_per_gas Nullable(UInt256),
	max_priority_fee Nullable(UInt256),
	input String,
	type UInt8,
	transaction_index UInt64,
	success UInt8,
	num_logs UInt32
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/%s_local', '{replica}')
ORDER BY (blockchain_id, block_number, hash)
SETTINGS index_granularity = 8192
