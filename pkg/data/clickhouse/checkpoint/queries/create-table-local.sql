CREATE TABLE IF NOT EXISTS `%s`.`%s_local`
ON CLUSTER `%s`
(
	chain_id UInt64,
	mode String,
	lowest_unprocessed_block UInt64,
	timestamp Int64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/%s_local', '{replica}')
ORDER BY (chain_id, mode) VERSION timestamp
SETTINGS index_granularity = 8192
