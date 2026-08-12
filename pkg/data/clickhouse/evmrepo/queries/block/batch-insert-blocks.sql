INSERT INTO `%s`.`%s` 
(
	blockchain_id, evm_chain_id, block_number, hash, parent_hash, block_time, timestamp_ms, miner,
	difficulty, total_difficulty, size, gas_limit, gas_used,
	base_fee_per_gas, block_gas_cost, state_root, transactions_root, receipts_root,
	extra_data, block_extra_data, ext_data_hash, ext_data_gas_used,
	mix_hash, nonce, sha3_uncles, uncles,
	blob_gas_used, excess_blob_gas, parent_beacon_block_root, min_delay_excess, num_txns,
	target_exponent, min_price_exponent,
	settled_height, settled_gas_unix, settled_gas_numerator, settled_excess,
	executed_gas_used
)