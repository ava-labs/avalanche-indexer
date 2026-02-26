INSERT INTO `%s`.`%s` (
	blockchain_id, evm_chain_id, block_number, transaction_hash,
	type, from, to, value, gas, gas_used, revert, error, revert_reason,
	input, output, call_index
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
