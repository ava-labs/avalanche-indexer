CREATE TABLE IF NOT EXISTS `%s`.`%s_local`
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
    receipts_relayer_addresses  Array(FixedString(20)),
    created_at                  DateTime64(3, 'UTC') DEFAULT now64(3),
    PROJECTION by_evm_chain_id (
        SELECT * ORDER BY evm_chain_id, block_time, tx_hash, log_index
    )
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/%s_local', '{replica}')
PARTITION BY toYYYYMM(block_time)
ORDER BY (blockchain_id, tx_hash, log_index)
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'drop'
