CREATE TABLE IF NOT EXISTS `%s`.`%s_local`
ON CLUSTER `%s`
(
	blockchain_id String,
	evm_chain_id UInt256,
	block_number UInt64,
	block_hash FixedString(32),
	block_time DateTime64(3, 'UTC'),
	tx_hash FixedString(32),
	tx_index UInt32,
	address FixedString(20),
	topic0 Nullable(FixedString(32)),
	topic1 Nullable(FixedString(32)),
	topic2 Nullable(FixedString(32)),
	topic3 Nullable(FixedString(32)),
	data String,
	log_index UInt32,
	removed Bool
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/%s_local', '{replica}')
ORDER BY (blockchain_id, block_time, tx_hash, log_index)
SETTINGS index_granularity = 8192
