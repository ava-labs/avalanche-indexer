SELECT chain_id, mode, lowest_unprocessed_block, timestamp FROM `%s`.`%s` WHERE chain_id = ? AND mode = ? ORDER BY timestamp DESC LIMIT 1
