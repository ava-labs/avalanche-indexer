CREATE TABLE IF NOT EXISTS `%s`.`%s`
ON CLUSTER `%s`
(
    source_blockchain_id        LowCardinality(String),
    destination_blockchain_id   LowCardinality(String),
    message_id                  FixedString(32),
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
    receive_block_time          SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    receive_tx_hash             SimpleAggregateFunction(max, Nullable(FixedString(32))),
    deliverer_address           SimpleAggregateFunction(max, Nullable(FixedString(20))),
    reward_redeemer_address     SimpleAggregateFunction(max, Nullable(FixedString(20))),
    destination_evm_chain_id    SimpleAggregateFunction(max, Nullable(UInt64)),
    destination_gas_spent       SimpleAggregateFunction(max, Nullable(UInt256)),
    executed_block_time         SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    executed_tx_hash            SimpleAggregateFunction(max, Nullable(FixedString(32))),
    last_execution_failed_time  SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))),
    receipt_delivered           SimpleAggregateFunction(max, UInt8)
)
-- Shard on message_id so source-chain and destination-chain partial rows for the
-- same message always land on the same shard, enabling FINAL to merge them.
ENGINE = Distributed(`%s`, `%s`, `%s_local`, sipHash64(message_id))
