CREATE TABLE IF NOT EXISTS %s.%s
ON CLUSTER %s
(
	chain_id UInt64,
	lowest_unprocessed_block UInt64,
	timestamp Int64
)
ENGINE = Distributed(%s, %s, %s_local, sipHash64(chain_id))
