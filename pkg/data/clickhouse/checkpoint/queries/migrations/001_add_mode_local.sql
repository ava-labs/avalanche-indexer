ALTER TABLE `%s`.`%s_local` ON CLUSTER `%s` ADD COLUMN IF NOT EXISTS mode String DEFAULT 'blocks';
ALTER TABLE `%s`.`%s_local` ON CLUSTER `%s` MODIFY ORDER BY (chain_id, mode);
