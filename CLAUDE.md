# CLAUDE.md

Guidance for Claude (and other AI coding assistants) working in this repo. Keep it short, keep it accurate. This file mirrors `AGENTS.md` — please keep the two in sync when editing.

## What this repo is

A Go monorepo that indexes and processes Avalanche blockchain data.

- Ingests EVM blocks, transactions, logs, debug traces, and ICM (Teleporter cross-chain) events.
- Publishes to Kafka; persists to ClickHouse; checkpoints to ClickHouse or DynamoDB.
- Two runnable services live under `cmd/`; shared libraries under `pkg/`.

Module path: `github.com/ava-labs/avalanche-indexer`. Requires **Go 1.24+**.

## Repo layout

```
cmd/
  blockfetcher/       RPC → sliding-window scheduler → Kafka
  consumerindexer/    Kafka → parse → ClickHouse (blocks / traces / ICM modes)
pkg/
  batchwriter/        Generic batched writer used by consumerindexer
  checkpointer/       Shared checkpoint abstraction (ClickHouse or DynamoDB backend)
  clickhouse/         ClickHouse client (config, connection pool, health check)
  data/
    clickhouse/
      checkpoint/     Checkpoint schema + queries
      evmrepo/        Blocks, transactions, logs, internal_transactions repos (+ *.sql)
      icmrepo/        ICM message + event repos (+ *.sql)
    dynamodb/         DynamoDB checkpoint client
  dynamodb/           Generic DynamoDB client
  kafka/              Consumer, producer, admin, offset manager, DLQ
    messages/         EVM/ICM JSON wire format + custom (un)marshallers
    processor/        Per-message processors (coreth, coreth_traces, icm) + retry semantics
  metrics/            Prometheus instrumentation (namespace: `indexer`)
  slidingwindow/      Concurrent block-height scheduler w/ realtime + backfill
  utils/              Hex parsing, backoff, logger, etc.
test/e2e/             End-to-end tests (docker compose required)
```

SQL for every ClickHouse table lives next to its repo under `queries/<entity>/`, one file per statement (create-table, batch-insert, write, delete, migrations).

## Common commands

```bash
make build-all                 # build every cmd/*/main.go into ./bin/
APP=blockfetcher make build-app

make unit-test                 # go test -v -cover -race ./...
make coverage-test             # coverage with cmd/ and testutils excluded
make integration-test          # go test -tags=integration ...  (requires docker compose)
make e2e-test                  # go test -tags=e2e ./test/e2e   (requires docker compose)
make fuzz-test                 # 30s per fuzz target (utils + kafka/messages)

make lint                      # golangci-lint run --fix && gofumpt -w .

docker compose up -d           # Kafka + Kafka UI + ClickHouse + Tabix (local dev)
```

CI (`.github/workflows/ci.yml`) runs Unit + Integration + E2E + Lint + Fuzz + `go mod tidy` check, and enforces a **70% coverage threshold** on the combined profile. Run `make lint` and `make unit-test` before pushing.

## Coding conventions

### Formatting & imports
- `gofumpt` is authoritative — do not fight it.
- `gci` import order (see `.golangci.yml`): `standard` → default → blank → `prefix(github.com/ava-labs/avalanche-indexer)` → alias → dot. Local imports must be grouped last, before aliases.
- Tag alignment: struct tags must be sorted and aligned (`tagalign` linter is strict).

### Linting hard rules (enforced by golangci-lint)
- **Testing:** never use `t.Fatal[f]`, `t.Error[f]`, `assert.Error`, `assert.ErrorContains`, `assert.EqualValues`, `require.Error`, `require.ErrorContains`, `require.EqualValues`, or `assert.NotEqualValues`. Use `require`/`assert` from `testify` with `ErrorIs`, `Equal`, etc.
- **Slices:** don't use `sort.Slice` / `sort.Strings`; use the `slices` package.
- **Format directives:** don't pass a format string to non-`f` variants (e.g., `errors.New` instead of `fmt.Errorf("no verb")`).
- **Forbidden packages** (depguard): `container/list`, `github.com/golang/mock/gomock`, `io/ioutil`.
- **Tests must not use** `context.Background()`, `context.TODO()`, `os.CreateTemp`, `os.MkdirTemp`, `os.TempDir`, `os.Chdir`, or `os.Setenv` (use `t.Context()`, `t.TempDir()`, `t.Setenv()`).
- Prefer early returns; no bool literals in conditions; no useless breaks; no unused params/receivers (revive rules on).

### Error handling
- Return sentinel errors from packages (e.g., `evmrepo.ErrBlockChainIDRequired`, `processor.ErrNilMessage`, `evmrepo.ErrTransactionChainIDRequired`); wrap with `fmt.Errorf("%w: %w", sentinel, cause)`.
- **Kafka processor errors** must be tagged with retry semantics — see `pkg/kafka/processor/errors.go`:
  - `NonRetryable(err)` — permanent per-message failure → routes to DLQ.
  - `Fatal(err)` — systemic failure → consumer shuts down, no DLQ.
  - Plain `error` → retry loop.
  - Test with `IsNonRetryable(err)` / `IsFatal(err)`.
- Never swallow errors from `Close()`, `Cancel()`, span `End()`, or HTTP `Body.Close()` (bodyclose + spancheck enabled).

### JSON handling for Kafka messages
- Use `jsonIter` (`json-iterator/go`) — not `encoding/json` — for hot-path decoding of Kafka payloads.
- For `*big.Int` fields on the wire, define an alias struct with `json.RawMessage` fields and parse via `parseBigIntFromRaw`. This is what lets us tolerate legacy scientific-notation payloads (e.g., `"1e+21"`). Look at `evmBlockJSON` / `evmTransactionJSON` / `evmTxReceiptJSON` in `pkg/kafka/messages/evm.go` before adding new numeric wire fields.
- Env-var integers that may arrive in scientific notation cannot use `cli.Uint64Flag` directly — always quote such values in YAML/Helm/`.env` so `strconv.ParseUint` accepts them.

### Logging
- Use `go.uber.org/zap` sugared loggers (`p.log.Debugw("...", "key", val)` style with alternating key/value pairs).
- Never `fmt.Print*`, `log.Print*`, or `panic()` in library code.

### ClickHouse conventions
- Every table has both `create-*-table-local.sql` (per-shard local table on `ReplicatedReplacingMergeTree` / `ReplicatedAggregatingMergeTree`) and `create-*-table.sql` (`Distributed` engine on top). Add both when introducing a table.
- Common column types across EVM tables:
  - `blockchain_id String`, `evm_chain_id UInt256`, `block_number UInt64`
  - `block_time DateTime64(3, 'UTC')`, `timestamp_ms UInt64`
  - Addresses: `FixedString(20)`, hashes: `FixedString(32)`
  - Values / gas prices: `UInt256`
- Nullable columns use `Nullable(...)`. Migrations live under `queries/migrations/<entity>/NNN_*.sql` (numbered).
- Repositories under `pkg/data/clickhouse/*/` follow the pattern: `<entity>_row.go` (typed row), `<entity>_repository.go` (Read/Write/BatchWrite methods), `queries/<entity>/*.sql` (loaded via `embed`).
- If you add a column: update the local table SQL, the distributed table SQL, an ordered migration file, the `Row` struct, the write/batch-insert SQL, and the mapping code in the processor.

### Testing conventions
- `testify` (`require` for setup/must-pass, `assert` for individual assertions).
- Unit tests are pure Go, no build tag. Integration tests use `//go:build integration`. E2E tests use `//go:build e2e` and live in `test/e2e/`.
- Fuzz targets live in `pkg/utils/` and `pkg/kafka/messages/`; CI runs each for 30s.
- Prefer table-driven tests with named subtests.

## Where things live — quick lookup

| I want to… | Look here |
|---|---|
| Add a new consumerindexer flag / env var | `cmd/consumerindexer/flags.go` + `config.go` + `run.go` (+ README table) |
| Change how an EVM block is decoded | `pkg/kafka/messages/evm.go` (`EVMBlock.UnmarshalJSON`) |
| Change how an EVM transaction is persisted | `pkg/kafka/processor/coreth.go` (`CorethTransactionToTransactionRow`) + `pkg/data/clickhouse/evmrepo/transaction_row.go` + `queries/transaction/*.sql` |
| Add an ICM event type | `pkg/kafka/messages/evm.go` (ABI parsing) + a new `<name>_events_repository.go` + `queries/<name>_events/*.sql` + wire it into `pkg/kafka/processor/icm.go` and the messages merge logic |
| Add a Prometheus metric | `pkg/metrics/metrics.go` (define, register, expose via method) |
| Change checkpoint schema | `pkg/data/clickhouse/checkpoint/queries/*.sql` and `pkg/checkpointer/` (interface applies to both backends) |
| Add a new fuzz target | put it in the same package as the code under test; add it to `Makefile` (`fuzz-test`, `fuzz-test-long`) and `.github/workflows/ci.yml` matrix |

## Things that commonly go wrong

- **Coverage below 70%** — CI will fail. `make coverage-test` locally to check.
- **`go.mod` not tidy** — CI runs `go mod tidy` and fails on any diff.
- **Scientific-notation numeric env vars** (e.g. `CHAIN_ID=9.80599e+08`) — `urfave/cli/v2` uses `strconv.ParseUint`, which rejects them. Quote large numbers in YAML/Helm.
- **Adding a ClickHouse column but forgetting the migration** — the local + distributed CREATE statements are for fresh installs only; existing clusters need the numbered `queries/migrations/<entity>/NNN_*.sql` file.
- **Mixing `String` and `LowCardinality(String)` for the same conceptual column** across tables — causes silent casts on joins. Pick one per column and stick to it.
- **Kafka DLQ vs shutdown confusion** — a bare `error` from a processor retries forever; if the message is structurally invalid, wrap with `NonRetryable(...)`, and if the whole consumer must die, wrap with `Fatal(...)`.
- **Using `encoding/json` for Kafka payloads** — the wire format supports scientific-notation big ints via `jsonIter` + `json.RawMessage` aliases; `encoding/json` will silently mis-decode them.

## Contributing checklist

Before opening a PR:

1. `make lint` (fixes formatting + import order).
2. `make unit-test` — must pass with `-race`.
3. If you touched anything under `pkg/data/clickhouse/**` or added SQL: `make integration-test` locally against `docker compose up -d`.
4. If you touched end-to-end flows: `make e2e-test`.
5. `go mod tidy` — commit any `go.mod` / `go.sum` changes.
6. Update the relevant `README.md` (repo root, `cmd/<svc>/README.md`, or `pkg/<subpkg>/README.md`) if you changed a user-visible flag, env var, or schema.
