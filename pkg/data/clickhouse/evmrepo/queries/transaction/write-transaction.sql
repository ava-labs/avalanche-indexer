INSERT INTO `%s`.`%s` (
	blockchain_id, evm_chain_id, block_number, block_hash, block_time, timestamp_ms, hash,
	from_address, to_address, nonce, value, gas, gas_used, effective_gas_price, gas_price,
	max_fee_per_gas, max_priority_fee, input, type, transaction_index, success, num_logs
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
