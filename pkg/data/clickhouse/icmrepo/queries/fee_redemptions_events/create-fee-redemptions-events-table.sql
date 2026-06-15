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
    redeemer_address            FixedString(20),
    fee_token_address           FixedString(20),
    amount                      UInt256
)
ENGINE = Distributed(`%s`, `%s`, `%s_local`, sipHash64(blockchain_id))
