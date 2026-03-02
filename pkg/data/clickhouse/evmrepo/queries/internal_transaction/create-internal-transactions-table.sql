CREATE TABLE IF NOT EXISTS `%s`.`%s`
ON CLUSTER `%s`
(
	blockchain_id String,
	evm_chain_id UInt256,
	block_number UInt64,
	block_timestamp DateTime64(3),
	transaction_hash FixedString(32),
	type String,
	from_address FixedString(20),
	to_address FixedString(20),
	value UInt256,
	gas UInt256,
	gas_used UInt256,
	revert Bool,
	error String,
	revert_reason String,
	input String,
	output String,
	call_index String
)
ENGINE = Distributed(`%s`, `%s`, `%s_local`, sipHash64(blockchain_id))
