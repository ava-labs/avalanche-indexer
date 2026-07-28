ALTER TABLE `%s`.`%s_local` ON CLUSTER `%s` ADD INDEX IF NOT EXISTS idx_address_bloom address TYPE bloom_filter(0.01) GRANULARITY 1
