-- One-time backfill for migration 002_add_address_bloom_index_local.sql.
--
-- Migration 002 adds idx_address_bloom to raw_logs_local; new and merged parts
-- pick it up automatically, but pre-existing parts are not indexed until this
-- MATERIALIZE runs. This is NOT a migration: RunMigrations executes every file
-- in queries/migrations/log/ on every service boot, and MATERIALIZE INDEX
-- enqueues a full-table mutation each time it runs.
--
-- Run once per environment, off-peak, and monitor the mutation to completion.
-- Substitute <database> and <cluster> for the target environment.

ALTER TABLE `<database>`.`raw_logs_local` ON CLUSTER `<cluster>`
MATERIALIZE INDEX idx_address_bloom;

-- Track progress (done when the row disappears / is_done = 1):
-- SELECT mutation_id, command, parts_to_do, is_done, latest_fail_reason
-- FROM system.mutations
-- WHERE table = 'raw_logs_local' AND command LIKE '%idx_address_bloom%';
