-- Manual runbook: add the by_message_id projection to the ICM send events local table.
--
-- Gives detail lookups keyed on (evm_chain_id, message_id) a granule-level index
-- seek instead of a full scan, so getSentMessageDetail does not need a
-- derived materialized view. Mirrors the existing by_evm_chain_id projection.
--
-- Run once per environment, off-peak. Substitute <database>, <cluster>, and
-- <table> (the base table name, default icm_send_events, configurable via
-- --icm-send-events-table-name; the local table is always <table>_local).
-- Step 1 adds the projection definition (covers new and merged parts).
-- Step 2 materializes it over pre-existing parts (a full-table mutation).

ALTER TABLE `<database>`.`<table>_local` ON CLUSTER `<cluster>`
ADD PROJECTION IF NOT EXISTS by_message_id (SELECT * ORDER BY evm_chain_id, message_id);

ALTER TABLE `<database>`.`<table>_local` ON CLUSTER `<cluster>`
MATERIALIZE PROJECTION by_message_id;

-- Track progress (done when the rows disappear / is_done = 1):
-- SELECT mutation_id, command, parts_to_do, is_done, latest_fail_reason
-- FROM system.mutations
-- WHERE table = '<table>_local' AND command LIKE '%by_message_id%';
