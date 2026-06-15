CREATE TABLE IF NOT EXISTS `%s`.`%s`
ON CLUSTER `%s`
(
    blockchain_id               String,
    evm_chain_id                UInt256,
    block_number                UInt64,
    block_time                  DateTime64(3, 'UTC'),
    tx_hash                     FixedString(32),
    tx_index                    UInt32,
    log_index                   UInt32,
    contract_address            FixedString(20),
    message_id                  FixedString(32),
    source_blockchain_id        String,
    deliverer_address           FixedString(20),
    reward_redeemer_address     FixedString(20),
    message_nonce               UInt256,
    origin_sender_address       FixedString(20),
    destination_blockchain_id   String,
    destination_address         FixedString(20),
    required_gas_limit          UInt256,
    allowed_relayer_addresses   Array(String),
    message_data                String,
    receipts_message_nonces     Array(UInt256),
    receipts_relayer_addresses  Array(FixedString(20))
)
ENGINE = Distributed(`%s`, `%s`, `%s_local`, sipHash64(blockchain_id))
