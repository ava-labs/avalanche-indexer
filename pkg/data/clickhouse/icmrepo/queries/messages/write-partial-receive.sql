INSERT INTO `%s`.`%s` (
    source_blockchain_id, destination_blockchain_id, message_id,
    receive_block_time, receive_tx_hash, deliverer_address, reward_redeemer_address,
    destination_evm_chain_id, destination_gas_spent
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
