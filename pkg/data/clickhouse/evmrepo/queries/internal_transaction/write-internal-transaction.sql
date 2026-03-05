INSERT INTO `%s`.`%s` (
	blockchain_id, evm_chain_id, block_number, block_timestamp, timestamp_ms, transaction_hash,
	type, from_address, to_address, value, gas, gas_used, revert, error, revert_reason,
	input, output, call_index
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
