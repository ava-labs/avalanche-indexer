-- One-time backfill for migration 005_add_bloom_indexes_local.sql.
--
-- Migration 005 adds idx_hash_bloom, idx_from_address_bloom and
-- idx_to_address_bloom to raw_transactions_local; new and merged parts pick them
-- up automatically, but pre-existing parts are not indexed until these
-- MATERIALIZE statements run. This is NOT a migration: RunMigrations executes
-- every file in queries/migrations/transaction/ on every service boot, and
-- MATERIALIZE INDEX enqueues a full-table mutation each time it runs.
--
-- Run once per environment, off-peak, and monitor the mutations to completion.
-- Substitute <database> and <cluster> for the target environment.

ALTER TABLE `<database>`.`raw_transactions_local` ON CLUSTER `<cluster>`
MATERIALIZE INDEX idx_hash_bloom;

ALTER TABLE `<database>`.`raw_transactions_local` ON CLUSTER `<cluster>`
MATERIALIZE INDEX idx_from_address_bloom;

ALTER TABLE `<database>`.`raw_transactions_local` ON CLUSTER `<cluster>`
MATERIALIZE INDEX idx_to_address_bloom;

-- Track progress (done when the rows disappear / is_done = 1):
-- SELECT mutation_id, command, parts_to_do, is_done, latest_fail_reason
-- FROM system.mutations
-- WHERE table = 'raw_transactions_local' AND command LIKE '%_bloom%';
