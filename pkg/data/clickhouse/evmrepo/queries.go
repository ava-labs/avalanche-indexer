package evmrepo

const (
	// blockColumns is the column list for blocks table (29 columns)
	blockColumns = `blockchain_id, evm_chain_id, block_number, hash, parent_hash, block_time, miner,
		difficulty, total_difficulty, size, gas_limit, gas_used,
		base_fee_per_gas, block_gas_cost, state_root, transactions_root, receipts_root,
		extra_data, block_extra_data, ext_data_hash, ext_data_gas_used,
		mix_hash, nonce, sha3_uncles, uncles,
		blob_gas_used, excess_blob_gas, parent_beacon_block_root, min_delay_excess`

	// blockValuesPlaceholders is the VALUES placeholder string for blocks (29 placeholders)
	blockValuesPlaceholders = `?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?`
)

// BlockInsertQuery returns the INSERT query for blocks table with VALUES clause
// The query expects 29 parameters in this order matching blockColumns
func BlockInsertQuery(tableName string) string {
	return `INSERT INTO ` + tableName + ` (` + blockColumns + `) VALUES (` + blockValuesPlaceholders + `)`
}

// BlockInsertQueryForBatch returns the INSERT query for blocks table without VALUES clause (for PrepareBatch)
// The query expects the same 29 columns as BlockInsertQuery
func BlockInsertQueryForBatch(tableName string) string {
	return `INSERT INTO ` + tableName + ` (` + blockColumns + `)`
}

const (
	// transactionColumns is the column list for transactions table (17 columns)
	transactionColumns = `blockchain_id, evm_chain_id, block_number, block_hash, block_time, hash,
		from_address, to_address, nonce, value, gas, gas_price,
		max_fee_per_gas, max_priority_fee, input, type, transaction_index`

	// transactionValuesPlaceholders is the VALUES placeholder string for transactions (17 placeholders)
	transactionValuesPlaceholders = `?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?`
)

// TransactionInsertQuery returns the INSERT query for transactions table with VALUES clause
// The query expects 17 parameters in this order matching transactionColumns
func TransactionInsertQuery(tableName string) string {
	return `INSERT INTO ` + tableName + ` (` + transactionColumns + `) VALUES (` + transactionValuesPlaceholders + `)`
}

// TransactionInsertQueryForBatch returns the INSERT query for transactions table without VALUES clause (for PrepareBatch)
// The query expects the same 17 columns as TransactionInsertQuery
func TransactionInsertQueryForBatch(tableName string) string {
	return `INSERT INTO ` + tableName + ` (` + transactionColumns + `)`
}

// CreateBlocksTableQuery returns the CREATE TABLE query for blocks table
func CreateBlocksTableQuery(tableName string) string {
	return `CREATE TABLE IF NOT EXISTS ` + tableName + ` (
		blockchain_id String,
		evm_chain_id UInt256,
		block_number UInt64,
		hash String,
		parent_hash String,
		block_time DateTime64(3, 'UTC'),
		miner String,
		difficulty UInt256,
		total_difficulty UInt256,
		size UInt64,
		gas_limit UInt64,
		gas_used UInt64,
		base_fee_per_gas UInt256,
		block_gas_cost UInt256,
		state_root String,
		transactions_root String,
		receipts_root String,
		extra_data String,
		block_extra_data String,
		ext_data_hash String,
		ext_data_gas_used UInt32,
		mix_hash String,
		nonce Nullable(String),
		sha3_uncles String,
		uncles Array(String),
		blob_gas_used UInt64,
		excess_blob_gas UInt64,
		parent_beacon_block_root Nullable(String),
		min_delay_excess UInt64
	)
	ENGINE = MergeTree
	ORDER BY (blockchain_id, block_time, block_number)
	SETTINGS index_granularity = 8192`
}

// CreateTransactionsTableQuery returns the CREATE TABLE query for transactions table
func CreateTransactionsTableQuery(tableName string) string {
	return `CREATE TABLE IF NOT EXISTS ` + tableName + ` (
		blockchain_id String,
		evm_chain_id UInt256,
		block_number UInt64,
		block_hash String,
		block_time DateTime64(3, 'UTC'),
		hash String,
		from_address String,
		to_address Nullable(String),
		nonce UInt64,
		value UInt256,
		gas UInt64,
		gas_price UInt256,
		max_fee_per_gas Nullable(UInt256),
		max_priority_fee Nullable(UInt256),
		input String,
		type UInt8,
		transaction_index UInt64
	)
	ENGINE = MergeTree
	ORDER BY (blockchain_id, block_time, hash)
	SETTINGS index_granularity = 8192`
}
