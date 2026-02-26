CREATE TABLE IF NOT EXISTS `%s`.`%s`
ON CLUSTER `%s`
(
	blockchain_id String,
	evm_chain_id UInt256,
	block_number UInt64,
	transaction_hash FixedString(32),
	type String,
	from FixedString(20),
	to FixedString(20),
	value String,
	gas String,
	gas_used String,
	revert Bool,
	error String,
	revert_reason String,
	input String,
	output String,
	call_index String
)
ENGINE = Distributed(`%s`, `%s`, `%s_local`, sipHash64(blockchain_id))
