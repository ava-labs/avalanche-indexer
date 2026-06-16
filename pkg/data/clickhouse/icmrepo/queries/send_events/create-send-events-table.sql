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
    destination_blockchain_id   String,
    sender_address              FixedString(20),
    destination_address         FixedString(20),
    required_gas_limit          UInt256,
    allowed_relayer_addresses   Array(String),
    fee_token_address           FixedString(20),
    fee_amount                  UInt256,
    message_nonce               UInt256,
    message_data                String,
    receipts_message_nonces     Array(UInt256),
    receipts_relayer_addresses  Array(FixedString(20)),
    created_at                  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = Distributed(`%s`, `%s`, `%s_local`, sipHash64(blockchain_id))
