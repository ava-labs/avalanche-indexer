CREATE TABLE IF NOT EXISTS `%s`.`%s_local`
ON CLUSTER `%s`
(
	blockchain_id String,
	evm_chain_id UInt256,
	block_number UInt64,
	block_hash FixedString(32),
	block_time DateTime64(3, 'UTC'),
	hash FixedString(32),
	from_address FixedString(20),
	to_address Nullable(FixedString(20)),
	nonce UInt64,
	value UInt256,
	gas UInt64,
	gas_price UInt256,
	max_fee_per_gas Nullable(UInt256),
	max_priority_fee Nullable(UInt256),
	input String,
	type UInt8,
	transaction_index UInt64,
	success UInt8
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/%s_local', '{replica}')
ORDER BY (blockchain_id, block_time, hash)
SETTINGS index_granularity = 8192
