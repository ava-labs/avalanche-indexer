# AGENTS.md

Project-level guidance for AI coding agents (and humans) working in this repo. Treat this as authoritative when it conflicts with general defaults. Mirrors `CLAUDE.md` — keep the two in sync when editing either.

## What this repo is

`avalanche-indexer` is a Go 1.25 monorepo that indexes and processes Avalanche blockchain data. Module path: `github.com/ava-labs/avalanche-indexer`.

- Ingests EVM blocks, transactions, logs, debug traces, and ICM (Teleporter cross-chain) events.
- Publishes to Kafka; persists to ClickHouse; checkpoints to ClickHouse or DynamoDB.
- Two runnable services live under `cmd/`; shared libraries under `pkg/`.

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

SQL for each ClickHouse repo lives next to it under `queries/`, one file per statement (create-table, batch-insert, write, delete, migrations). Multi-entity repos (`evmrepo`, `icmrepo`) group by entity — `queries/<entity>/*.sql` — while single-entity repos (`checkpoint`) keep the files directly under `queries/`.

## Conventions to follow

### Go code
- Go 1.25, formatted with `gofumpt`, linted with `golangci-lint` v2.10.1. Run `make lint` before handing back changes.
- `gci` import order (see `.golangci.yml`): `standard` → default → blank → `prefix(github.com/ava-labs/avalanche-indexer)` → alias → dot. Local imports grouped last, before aliases.
- Tag alignment: struct tags must be sorted and aligned (`tagalign` linter is strict).
- Prefer early returns; no bool literals in conditions; no useless breaks; no unused params/receivers (revive rules on).
- Logging: use `go.uber.org/zap` sugared loggers (`p.log.Debugw("...", "key", val)` with alternating key/value pairs). Log keys stay camelCase in this repo (e.g. `evmChainID`, `blockNumber`, `bcID`) — do not switch to snake_case. Never `fmt.Print*`, `log.Print*`, or `panic()` in library code.
- Comments: keep them tight. Package doc comments are one-liners naming the package's responsibility. Avoid multi-line block comments in source; if rationale needs more space, put it in a `README.md` next to the package.

### Linting hard rules (enforced by golangci-lint)
- **Testing:** never use `t.Fatal[f]`, `t.Error[f]`, `assert.Error`, `assert.ErrorContains`, `assert.EqualValues`, `require.Error`, `require.ErrorContains`, `require.EqualValues`, or `assert.NotEqualValues`. Use `require`/`assert` from `testify` with `ErrorIs`, `Equal`, etc.
- **Slices:** don't use `sort.Slice` / `sort.Strings`; use the `slices` package.
- **Format directives:** don't pass a format string to non-`f` variants (e.g. `errors.New` instead of `fmt.Errorf("no verb")`).
- **Forbidden packages** (depguard): `container/list`, `github.com/golang/mock/gomock`, `io/ioutil`.
- **Tests must not use** `context.Background()`, `context.TODO()`, `os.CreateTemp`, `os.MkdirTemp`, `os.TempDir`, `os.Chdir`, or `os.Setenv` (use `t.Context()`, `t.TempDir()`, `t.Setenv()`).

### Error handling
- Return sentinel errors from packages (e.g. `evmrepo.ErrBlockChainIDRequired`, `processor.ErrNilMessage`); wrap with `fmt.Errorf("%w: %w", sentinel, cause)`.
- **Kafka processor errors** must be tagged with retry semantics — see `pkg/kafka/processor/errors.go`:
  - `NonRetryable(err)` — permanent per-message failure → routes to DLQ.
  - `Fatal(err)` — systemic failure → consumer shuts down, no DLQ.
  - Plain `error` → retry loop.
  - Test with `IsNonRetryable(err)` / `IsFatal(err)`.
- Never swallow errors from `Close()`, `Cancel()`, span `End()`, or HTTP `Body.Close()` (bodyclose + spancheck enabled).

### JSON handling for Kafka messages
- Use `jsonIter` (`json-iterator/go`) — not `encoding/json` — for hot-path decoding of Kafka payloads.
- For `*big.Int` fields on the wire, define an alias struct with `json.RawMessage` fields and parse via `parseBigIntFromRaw`. This is what lets us tolerate legacy scientific-notation payloads (e.g. `"1e+21"`). Look at `evmBlockJSON` / `evmTransactionJSON` / `evmTxReceiptJSON` in `pkg/kafka/messages/evm.go` before adding new numeric wire fields.
- Env-var integers that may arrive in scientific notation cannot use `cli.Uint64Flag` directly — always quote such values in YAML/Helm/`.env` so `strconv.ParseUint` accepts them.

### SQL (ClickHouse)
- Every table has **two** create files: `create-<name>-table-local.sql` (per-shard local table on `ReplicatedReplacingMergeTree` / `ReplicatedAggregatingMergeTree`) **and** `create-<name>-table.sql` (`Distributed` engine on top). Add both when introducing a table.
- Migrations live under `pkg/data/clickhouse/<repo>/queries/migrations/<entity>/NNN_*.sql` (numbered). Never modify a historical SQL file to "fix" a deployed schema — add a new numbered migration.
- Common column types across EVM tables:
  - `blockchain_id String`, `evm_chain_id UInt256`, `block_number UInt64`.
  - `block_time DateTime64(3, 'UTC')`, `timestamp_ms UInt64`.
  - Addresses: `FixedString(20)`, hashes: `FixedString(32)`, values / gas prices: `UInt256`.
- Repositories under `pkg/data/clickhouse/*/` follow the pattern: `<entity>_row.go` (typed row), `<entity>_repository.go` (Read/Write/BatchWrite methods), `queries/<entity>/*.sql` (loaded via `embed`).
- If you add a column: update the local table SQL, the distributed table SQL, an ordered migration file, the `Row` struct, the write/batch-insert SQL, and the mapping code in the processor.
- Don't mix `String` and `LowCardinality(String)` for the same conceptual column across tables — it causes silent casts on joins. Pick one per column and stick to it.

### Testing
- `testify` (`require` for setup / must-pass, `assert` for individual assertions).
- Unit tests are pure Go, no build tag. Integration tests use `//go:build integration`. E2E tests use `//go:build e2e` and live in `test/e2e/`.
- Prefer table-driven tests with named subtests.
- Fuzz targets live in `pkg/utils/` and `pkg/kafka/messages/`; CI runs each for 30s. Add a new target to both `Makefile` (`fuzz-test`, `fuzz-test-long`) and `.github/workflows/ci.yml` matrix.
- Run `make unit-test` before claiming a change is done. If you touched `pkg/data/clickhouse/**` or added SQL, also run `make integration-test` locally against `docker compose up -d`.

### Config & flags
- Both services use `urfave/cli/v2`. When adding a new flag:
  1. Add it to `cmd/<service>/flags.go`.
  2. Wire it into `cmd/<service>/config.go` and `run.go`.
  3. Update the flag table in `cmd/<service>/README.md`.
- Never commit real secrets.

## Local dev quick reference

```bash
docker compose up -d           # Kafka + Kafka UI + ClickHouse + Tabix

make build-all                 # build every cmd/*/main.go into ./bin/
APP=blockfetcher make build-app

make unit-test                 # go test -v -cover -race ./...
make coverage-test             # coverage with cmd/ and testutils excluded (70% CI threshold)
make integration-test          # requires docker compose
make e2e-test                  # requires docker compose
make fuzz-test                 # 30s per fuzz target

make lint                      # golangci-lint run --fix && gofumpt -w .
```

CI (`.github/workflows/ci.yml`) runs Unit + Integration + E2E + Lint + Fuzz + `go mod tidy` check, and enforces a **70% coverage threshold** on the combined profile.

Local endpoints when the compose stack is up: Kafka `localhost:9092`, Kafka UI `http://localhost:8080`, ClickHouse HTTP `http://localhost:8123`, ClickHouse native `localhost:9000`, Tabix `http://localhost:8082`.

## Where things live — quick lookup

| I want to… | Look here |
|---|---|
| Add a new consumerindexer flag / env var | `cmd/consumerindexer/flags.go` + `config.go` + `run.go` (+ README table) |
| Change how an EVM block is decoded | `pkg/kafka/messages/evm.go` (`EVMBlock.UnmarshalJSON`) |
| Change how an EVM transaction is persisted | `pkg/kafka/processor/coreth.go` (`CorethTransactionToTransactionRow`) + `pkg/data/clickhouse/evmrepo/transaction_row.go` + `queries/transaction/*.sql` |
| Add an ICM event type | `pkg/kafka/messages/evm.go` (ABI parsing) + a new `<name>_events_repository.go` + `queries/<name>_events/*.sql` + wire it into `pkg/kafka/processor/icm.go` and the messages merge logic |
| Add a Prometheus metric | `pkg/metrics/metrics.go` (define, register, expose via method) |
| Change checkpoint schema | `pkg/data/clickhouse/checkpoint/queries/*.sql` and `pkg/checkpointer/` (interface applies to both backends) |
| Add a new fuzz target | put it in the same package as the code under test; add to `Makefile` (`fuzz-test`, `fuzz-test-long`) and `.github/workflows/ci.yml` matrix |

## PR workflow

- Branch naming: `<user>/<short-topic>` (e.g. `allen/add-claude-support`, `artem/create-icm-tables`).
- Title: short sentence case, no ticket prefix, no trailing period.
- Before opening a PR:
  1. `make lint` (fixes formatting + import order).
  2. `make unit-test` — must pass with `-race`.
  3. If you touched anything under `pkg/data/clickhouse/**` or added SQL: `make integration-test` locally.
  4. If you touched end-to-end flows: `make e2e-test`.
  5. `go mod tidy` — commit any `go.mod` / `go.sum` changes (CI enforces tidiness).
  6. Update the relevant `README.md` (repo root, `cmd/<svc>/README.md`, or `pkg/<subpkg>/README.md`) if you changed a user-visible flag, env var, or schema.
- GPG signing is configured for this user; use `--no-gpg-sign` only when the user explicitly asks.

## Committing

- **Never commit on the user's behalf without an explicit request.** Finish the change, run fmt/lint/tests, and stop there. Wait for the user to either run `git commit` themselves or to explicitly say "commit this" / "commit and push" / similar before staging or committing anything.
- Slash-commands or explicit phrases like "/commit", "open a PR", or "push" count as explicit requests; ambient instructions like "fix the bug" do not.
- This applies to `git add`, `git commit`, `git push`, and creating PRs.

## Things to avoid

- Don't use `encoding/json` for Kafka payloads — the wire format supports scientific-notation big ints via `jsonIter` + `json.RawMessage` aliases; `encoding/json` will silently mis-decode them.
- Don't return a bare `error` from a Kafka processor when the message is structurally invalid — it will retry forever. Wrap with `NonRetryable(...)`. If the whole consumer must die, wrap with `Fatal(...)`.
- Don't add a ClickHouse column and forget the migration — the local + distributed CREATE statements are for fresh installs only; existing clusters need a numbered `queries/migrations/<entity>/NNN_*.sql` file.
- Don't drop below the 70% CI coverage threshold. `make coverage-test` locally to check.
- Don't leave `go.mod` / `go.sum` untidy — CI runs `go mod tidy` and fails on any diff.
- Don't add new top-level directories without clear need — prefer extending `cmd/` or `pkg/`.
- Don't bypass `pkg/clickhouse` or `pkg/metrics` by re-implementing client / instrumentation logic inside a service.
- Don't commit values like `CHAIN_ID=9.80599e+08` — `urfave/cli/v2` uses `strconv.ParseUint` which rejects scientific notation. Quote large numbers in YAML/Helm/`.env`.

## Where to look next

- Repo overview, docker-compose services, example invocations: root `README.md`.
- Service-specific flag reference: `cmd/blockfetcher/README.md`, `cmd/consumerindexer/README.md`.
- ClickHouse client config (env vars): `pkg/clickhouse/README.md`.
- Sliding window architecture (backfill vs realtime scheduling): `pkg/slidingwindow/README.md`.
- Metrics catalog and Prometheus queries: `pkg/metrics/README.md`.
- Kafka processor retry semantics: `pkg/kafka/processor/errors.go`.
