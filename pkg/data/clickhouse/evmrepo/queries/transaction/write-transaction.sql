INSERT INTO `%s`.`%s` (
	blockchain_id, evm_chain_id, block_number, block_hash, block_time, hash,
	from_address, to_address, nonce, value, gas, gas_price,
	max_fee_per_gas, max_priority_fee, input, type, transaction_index, success
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
