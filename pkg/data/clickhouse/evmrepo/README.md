# evmrepo

ClickHouse repositories for EVM blocks, transactions, logs, and internal transactions.

## Block gas semantics after Helicon (ACP-194)

Helicon activates Continuous Execution (Streaming Asynchronous Execution) on the **C-Chain
only**. Execution is decoupled from consensus: a block is accepted, executed some time later,
and settled τ=5s after that. Several header fields are redefined to describe the blocks
*newly settled by this block* rather than this block's own transactions.

The indexer stores header fields verbatim, so those columns change meaning at the Helicon
activation boundary. **No query fails; results silently change.**

| Column | Pre-Helicon C-Chain, and all Subnet-EVM L1s | Post-Helicon C-Chain |
|---|---|---|
| `gas_used` | Gas used by this block's transactions | Gas **charged across newly settled blocks** |
| `executed_gas_used` | Same as `gas_used` | Gas used by **this block's** transactions |
| `state_root` | State after this block | State after the most recently **settled** block |
| `receipts_root` | Receipts of this block | Receipts of all **newly settled** blocks |
| `logs_bloom` | Logs of this block | Aggregated over **newly settled** blocks |
| `base_fee_per_gas` | Base fee from execution | Derived from **worst-case** gas bounds |

**Use `executed_gas_used`, not `gas_used`, for per-block gas usage, utilization, and fee
analytics.** It is populated for every chain and every block height, so it is safe to use
unconditionally and requires no branching on activation time.

Per-transaction values are unaffected: `transactions.gas_used` and
`transactions.effective_gas_price` come from the receipt and remain authoritative for what a
transaction actually consumed and paid.

### Settlement columns

`settled_height` names the highest block settled by this block; the rest expose the executor's
gas-time state. All are zero pre-Helicon and on Subnet-EVM L1s.

| Column | Meaning |
|---|---|
| `settled_height` | Highest block settled by this block |
| `settled_excess` | Gas excess used for price discovery |
| `settled_gas_unix` | Executor timestamp, seconds |
| `settled_gas_numerator` | Sub-second remainder of the executor timestamp |

`block_number - settled_height` is the settlement lag in blocks, and is a useful indexer
freshness signal.

### Gas target and minimum price (ACP-283)

`target_exponent` and `min_price_exponent` are stored as the **raw exponents** from the header,
not the derived values. The minimum gas price is `e^(q/D)` with
`D = 415828534307635077`:

```sql
SELECT exp(min_price_exponent / 415828534307635077.0) AS min_gas_price_wei
FROM raw_blocks WHERE min_price_exponent > 0
```

## Adding a column

A block column has six representations that must change together — miss one and rows silently
misalign:

1. `queries/block/create-blocks-table-local.sql` and `create-blocks-table.sql` (fresh installs)
2. A numbered pair under `queries/migrations/block/` (existing clusters)
3. `queries/block/write-block.sql` — column list **and** `?` placeholder count
4. `queries/block/batch-insert-blocks.sql` — column list
5. `block_row.go` (`BlockRow`) and `blocks_repository.go` (`chBlockRow`, the converter, and the
   positional `Exec` args, which must match the SQL order exactly)
6. The mapping in `pkg/kafka/processor/coreth.go`

`TestRepository_WriteBlock_Success` asserts every positional argument, so an omission in (5)
fails fast rather than writing shifted data.
