INSERT INTO `%s`.`%s` (
    blockchain_id, evm_chain_id, block_number, block_time, tx_hash, tx_index, log_index,
    contract_address, message_id, source_blockchain_id, message_nonce, origin_sender_address,
    destination_blockchain_id, destination_address, required_gas_limit,
    allowed_relayer_addresses, message_data, receipts_message_nonces, receipts_relayer_addresses
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
