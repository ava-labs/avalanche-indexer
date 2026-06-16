INSERT INTO `%s`.`%s` (
    source_blockchain_id, destination_blockchain_id, message_id,
    source_block_time, source_tx_hash, evm_chain_id, contract_address,
    message_nonce, sender_address, destination_address, required_gas_limit,
    allowed_relayer_addresses, fee_token_address, fee_amount,
    message_data, source_gas_spent, message_receipts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
