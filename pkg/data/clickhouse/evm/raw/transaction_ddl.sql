CREATE TABLE IF NOT EXISTS transactions (
  hash FixedString(32),
  block_number UInt64,
  `to` FixedString(20),
  `from` FixedString(20)
)
ENGINE = MergeTree
ORDER BY (hash);
