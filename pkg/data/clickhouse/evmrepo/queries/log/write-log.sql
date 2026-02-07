INSERT INTO %s.%s (
	blockchain_id, evm_chain_id, block_number, block_hash, block_time,
	tx_hash, tx_index, address, topic0, topic1, topic2, topic3, data, log_index, removed
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
