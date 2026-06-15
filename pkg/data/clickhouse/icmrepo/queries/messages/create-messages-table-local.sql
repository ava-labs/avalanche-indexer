CREATE TABLE IF NOT EXISTS `%s`.`%s_local`
ON CLUSTER `%s`
(
    -- Deduplication key (ORDER BY)
    source_blockchain_id        LowCardinality(String),
    destination_blockchain_id   LowCardinality(String),
    message_id                  FixedString(32),

    -- Fields from SendCrossChainMessage (source chain consumer)
    source_block_time           SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    source_tx_hash              SimpleAggregateFunction(max, Nullable(FixedString(32))),
    evm_chain_id                SimpleAggregateFunction(max, Nullable(UInt64)),
    contract_address            SimpleAggregateFunction(max, Nullable(FixedString(20))),
    message_nonce               SimpleAggregateFunction(max, Nullable(UInt256)),
    sender_address              SimpleAggregateFunction(max, Nullable(FixedString(20))),
    destination_address         SimpleAggregateFunction(max, Nullable(FixedString(20))),
    required_gas_limit          SimpleAggregateFunction(max, Nullable(UInt256)),
    allowed_relayer_addresses   SimpleAggregateFunction(max, Array(String)),
    fee_token_address           SimpleAggregateFunction(max, Nullable(FixedString(20))),
    fee_amount                  SimpleAggregateFunction(max, Nullable(UInt256)),
    message_data                SimpleAggregateFunction(max, String),
    source_gas_spent            SimpleAggregateFunction(max, Nullable(UInt256)),
    message_receipts            SimpleAggregateFunction(max, String),

    -- Fields from ReceiveCrossChainMessage (destination chain consumer)
    receive_block_time          SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    receive_tx_hash             SimpleAggregateFunction(max, Nullable(FixedString(32))),
    deliverer_address           SimpleAggregateFunction(max, Nullable(FixedString(20))),
    reward_redeemer_address     SimpleAggregateFunction(max, Nullable(FixedString(20))),
    destination_evm_chain_id    SimpleAggregateFunction(max, Nullable(UInt64)),
    destination_gas_spent       SimpleAggregateFunction(max, Nullable(UInt256)),

    -- Fields from MessageExecuted (destination chain consumer)
    executed_block_time         SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    executed_tx_hash            SimpleAggregateFunction(max, Nullable(FixedString(32))),

    -- Fields from MessageExecutionFailed (destination chain consumer)
    last_execution_failed_time  SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))),

    -- Fields from ReceiptReceived (source chain consumer)
    receipt_delivered           SimpleAggregateFunction(max, UInt8),

    PROJECTION by_source_chain_time (
        SELECT * ORDER BY source_blockchain_id, source_block_time, message_id
    ),
    PROJECTION by_destination_chain_time (
        SELECT * ORDER BY destination_blockchain_id, receive_block_time, message_id
    ),
    PROJECTION by_sender (
        SELECT * ORDER BY sender_address, source_block_time, message_id
    ),
    PROJECTION by_recipient (
        SELECT * ORDER BY destination_address, source_block_time, message_id
    ),
    PROJECTION by_message_id (
        SELECT * ORDER BY message_id
    )
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/%s_local', '{replica}')
ORDER BY (source_blockchain_id, destination_blockchain_id, message_id)
SETTINGS index_granularity = 8192
