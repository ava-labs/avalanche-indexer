ALTER TABLE `%s`.`%s_local` ON CLUSTER `%s` ADD COLUMN IF NOT EXISTS effective_gas_price UInt256 DEFAULT 0
