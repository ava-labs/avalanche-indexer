---
name: review-pr
description: Review a pull request in avalanche-indexer against this repo's conventions (Kafka processor retry semantics, ClickHouse local/distributed table pairs and migrations, jsonIter wire-format handling, flag/config wiring, sliding-window concurrency) and general code-review quality (correctness, tests, error handling, concurrency, performance). Use when the user asks to review a PR, code-review, check a PR, look at PR #N, or evaluate changes on a branch.
---

# Review PR

Act as a senior engineer reviewing this PR. Produce a structured, grounded code review for a PR in this repo. Every finding must be concrete (file/path, and a snippet or line range) and actionable (state the specific fix or question, not a vague concern). Only cite evidence from the actual diff. Do not hallucinate files, line numbers, or behaviors.

## Input resolution

Identify the target before reviewing:

1. If user gave a PR number or URL: use `gh pr view <n> --json ...` and `gh pr diff <n>`.
2. If user gave a branch name: diff against `origin/<base>` (detect base via `gh pr view <branch> --json baseRefName` if a PR exists, else fall back to `main`).
3. If user says "this PR" / "my branch" with no identifier: use current branch; try `gh pr view --json ...`, else diff against `main`.

Ask only if all three fail.

## Workflow

### 1. Gather context (run in parallel)

```bash
gh pr view <n> --json number,title,body,baseRefName,headRefName,author,additions,deletions,changedFiles,isDraft,labels
gh pr diff <n>
gh pr view <n> --json files --jq '.files[].path'
git fetch origin "pull/<n>/head"
git log --no-merges origin/<base>..FETCH_HEAD --pretty=format:'%h %s%n%b%n---'
```

`git fetch` pulls the PR head into `FETCH_HEAD` without checking it out or disturbing the current branch — required since a PR reviewed by number is usually not checked out locally.

For large diffs (>1500 lines), also fetch per-area slices: `gh pr diff <n> -- <path>`.

### 2. Categorize the change

Tag the PR as one or more of: `kafka-processor`, `clickhouse-schema`, `wire-format`, `flags-config`, `metrics`, `sliding-window`, `tests`, `docs`, `chore`. Which repo-specific checks apply depends on these tags (see §3).

### 3. Repo-specific checks

Apply every check whose tag matches the PR's category tags. Cite a file path (and line range when helpful) for each finding.

#### `kafka-processor` — `pkg/kafka/processor/**`
- Errors from a processor are tagged with retry semantics (see `pkg/kafka/processor/errors.go`): `NonRetryable(err)` for a structurally invalid message (routes to DLQ), `Fatal(err)` for a systemic failure (consumer shuts down, no DLQ), plain `error` only for genuinely retryable/transient failures. Flag a bare `return err` on a permanently-invalid message — it will retry forever.
- Sentinel errors are declared per package (e.g. `evmrepo.ErrBlockChainIDRequired`, `processor.ErrNilMessage`) and wrapped with `fmt.Errorf("%w: %w", sentinel, cause)`.
- Errors from `Close()`, `Cancel()`, span `End()`, or HTTP `Body.Close()` are not swallowed (bodyclose + spancheck are enforced by golangci-lint — flag any that would slip past, e.g. inside a helper not covered by the linter's default scope).
- New processor logic is tested with `IsNonRetryable(err)` / `IsFatal(err)`, not by inspecting error strings.

#### `clickhouse-schema` — `pkg/data/clickhouse/**`, `*.sql`
- A new table has **both** `create-<name>-table-local.sql` (per-shard local table on `ReplicatedReplacingMergeTree` / `ReplicatedAggregatingMergeTree`) and `create-<name>-table.sql` (`Distributed` engine on top). Flag if only one exists.
- A schema change to an *existing* table adds a new numbered migration under `queries/migrations/<entity>/NNN_*.sql`. A diff that edits a historical `create-*.sql` file or an existing migration in place (rather than adding a new one) is a blocker.
- Column type conventions held across tables: `blockchain_id String` (or `LowCardinality(String)` — flag if the PR mixes the two for the same conceptual column across tables, since that causes silent casts on joins), `evm_chain_id UInt256`, `block_number UInt64`, `block_time DateTime64(3, 'UTC')`, `timestamp_ms UInt64`, addresses `FixedString(20)`, hashes `FixedString(32)`.
- Repository pattern followed: `<entity>_row.go` (typed row) + `<entity>_repository.go` (Read/Write/BatchWrite) + `queries/<entity>/*.sql` (loaded via `embed`).
- If a column was added: local SQL, distributed SQL, an ordered migration file, the `Row` struct, the write/batch-insert SQL, and the processor mapping code are all updated together — flag whichever piece is missing.

#### `wire-format` — `pkg/kafka/messages/**`
- A new `*big.Int` wire field uses the alias-struct + `json.RawMessage` + `parseBigIntFromRaw` pattern (see `evmBlockJSON` / `evmTransactionJSON` / `evmTxReceiptJSON`), not a raw `json.Number`/`float64` — otherwise legacy scientific-notation payloads (e.g. `"1e+21"`) will silently mis-decode.
- Hot-path Kafka payload decode/encode uses `jsonIter` (`json-iterator/go`), not `encoding/json`.
- New fuzz coverage added for any new parser in this package (and wired into `Makefile`'s `fuzz-test`/`fuzz-test-long` and the `.github/workflows/ci.yml` Fuzz matrix) if the change touches unmarshal logic on untrusted input.

#### `flags-config` — `cmd/*/flags.go`, `config.go`, `run.go`
- A new flag is wired in all three places: `flags.go` (`cli.Flag` definition + `EnvVars`), `config.go` (struct field + `c.<Type>(...)` read), and `run.go` (actually consumed) — plus the flag table in `cmd/<service>/README.md`. Flag any place missing.
- Numeric env vars that could realistically be large (chain IDs) are documented as needing to be quoted in YAML/Helm/`.env`, since `urfave/cli/v2` uses `strconv.ParseUint` and rejects scientific notation (e.g. `9.80599e+08`).

#### `metrics` — `pkg/metrics/**`
- A new metric is defined with `Namespace: metrics.Namespace`, registered in `New()` via `reg.Register(...)`, and exposed through a dedicated method — not accessed as a raw exported struct field from callers.
- New metric names/labels/help text are added to `pkg/metrics/README.md` if user-facing (dashboards, alerts).

#### `sliding-window` — `pkg/slidingwindow/**`
- Every `Acquire`/`TryAcquire` on `workerSem`/`backfillSem` has a matching `Release`, including on early-return/error paths (check `defer` blocks specifically).
- Backfill work always acquires `backfillSem` **and** `workerSem` together (never just one) so a backfill task can't strand a slot.
- Goroutines spawned for subscription/backfill have a bounded lifetime tied to the passed `context.Context`; reconnect loops (`subscriber.go`) use capped backoff, not an unbounded tight loop.

#### `tests`
- Unit tests are pure Go with no build tag; integration tests use `//go:build integration`; e2e tests use `//go:build e2e` under `test/e2e/`.
- None of the forbidden testify/stdlib patterns from `.golangci.yml`'s `forbidigo` rules appear: `t.Fatal[f]`, `t.Error[f]`, `assert.Error`, `assert.ErrorContains`, `assert.EqualValues`, `require.Error`, `require.ErrorContains`, `require.EqualValues`, `assert.NotEqualValues`. These should be `ErrorIs`/`Equal` instead.
- Tests don't use `context.Background()`, `context.TODO()`, `os.CreateTemp`, `os.MkdirTemp`, `os.TempDir`, `os.Chdir`, or `os.Setenv` — should be `t.Context()`, `t.TempDir()`, `t.Setenv()`.
- Table-driven tests with named subtests for new branching logic.

#### `docs`
- `README.md` (repo root, `cmd/<svc>/README.md`, or `pkg/<subpkg>/README.md`) is updated if the PR changes a user-visible flag, env var, or schema.

### 4. Senior-engineer review checks (always apply)

Walk the diff and evaluate it against these axes. Surface a finding only when there's a concrete, actionable fix — not a generic "consider improving X". For each finding, cite the file (and line or snippet) and state the fix or the precise question.

#### 4.1 Bugs and edge cases
- Off-by-one on block heights / window bounds (`lowest`/`highest` semantics in `pkg/slidingwindow`), nil deref on `tx.Receipt` or similar optional fields, shadowed variables (`err :=` inside a nested block).
- Numeric edge cases: `UInt256` columns backed by `*big.Int`, not `int64`/`uint64` — flag any place a chain ID, value, or gas price could silently truncate.
- Nil-vs-zero-value handling: `EffectiveGasPrice`/`MaxFeePerGas`/etc. defaulting incorrectly when a receipt or optional field is absent.
- Address/hash normalization: `FixedString(20)`/`FixedString(32)` conversions from hex strings — check for length/format validation before conversion.
- Time handling: `block_time` stored as UTC `DateTime64(3, 'UTC')`; flag any local-time assumption.
- Context checks missing at long-running steps (no early return on `ctx.Done()`), especially in `slidingwindow` workers and Kafka consumer loops.

#### 4.2 Concurrency
- Goroutines without bounded lifetime: no parent context, no `errgroup`/`sync.WaitGroup`, no cancellation path.
- Data races: shared maps/slices (e.g. `slidingwindow.State`'s `processed`/`failCounts`) written from multiple goroutines without the existing mutex.
- Missing `defer` for semaphore release / mutex unlock on early returns (see `sliding-window` checks above).
- Channels never closed, or closed by the receiver; send on closed channel; unbuffered channel deadlocks in subscriber/producer paths.
- `errgroup` failures swallowed, or context not propagated to spawned workers.
- Batched ClickHouse writes (`pkg/batchwriter`) executed per-row where a single batch insert would do.

#### 4.3 Error handling
- Swallowed errors: `_ = fn()`, empty `if err != nil {}`, `err` reassigned before being checked.
- Missing `%w` wrapping across a layer boundary (processor → repository → SQL).
- Kafka processor errors not classified per the `kafka-processor` checks above — this is the most repo-specific error-handling concern and should be checked even outside the `pkg/kafka/processor` package if a new consumer entry point is added.
- Resource leaks on error path: rows/cursors, `context.CancelFunc`, file handles, RPC client connections. Check every early return in the affected function.
- Retries without backoff/jitter/cap (compare against `pkg/utils/backoff.go`'s pattern); retries on non-idempotent Kafka produces without idempotency configured.

#### 4.4 Security
- Input passed directly into a ClickHouse query as a string (SQL injection) — flag `fmt.Sprintf`/string concat into a query where a parameterized batch insert or prepared statement should be used.
- Secrets or credentials in the diff (API keys, private keys, RPC auth tokens, Kafka SASL credentials). Never committed per `AGENTS.md`/`CLAUDE.md`.
- TLS disabled (`InsecureSkipVerify`, `CLICKHOUSE_INSECURE_SKIP_VERIFY=true` outside local dev) without justification.
- Logs leaking sensitive data: RPC URLs with embedded credentials, full transaction input data at a verbose level without a documented reason.

#### 4.5 Performance risks
- N+1 RPC calls: a per-transaction `eth_getTransactionReceipt` loop where the existing batched `eth_getBlockReceipts` should be used instead (see `pkg/slidingwindow/worker/coreth.go` for the established pattern).
- ClickHouse writes issued per-row in a loop instead of using `pkg/batchwriter` or an existing batch-insert query.
- Large allocations in hot paths (`make([]T, 0)` when size is known; repeated `append` without pre-sizing) — `prealloc` linter should already catch some of this, but check loops it might miss.
- Per-call construction of clients that should be reused (RPC client, ClickHouse connection, Kafka producer) inside a request/message-processing loop.
- Goroutine-per-message without a bounded worker pool / semaphore for calls to rate-limited RPC providers.

#### 4.6 Data-pipeline design quality
- Kafka topic/DLQ semantics preserved: NonRetryable messages actually route to DLQ; Fatal errors actually stop the consumer without DLQ publish.
- Checkpoint `mode` field usage: blocks vs traces checkpoints don't collide for the same chain (see root `README.md`'s note on parallel modes).
- New ClickHouse columns/tables don't reintroduce a `String`/`LowCardinality(String)` mismatch for a column that already exists elsewhere with the other type.
- Idempotency: replays of the same Kafka message (at-least-once delivery) don't produce duplicate or inconsistent ClickHouse rows — check `ReplicatedReplacingMergeTree` version/dedup columns are set correctly for new tables.

#### 4.7 Missing or weak tests
- Every new exported function/processor branch has at least one test; new Kafka processor logic has a test asserting `IsNonRetryable`/`IsFatal` where relevant.
- Table-driven tests for input validation — not just the happy path.
- Error paths asserted with `ErrorIs`/`Equal` (per the forbidigo rules above), not `assert.Error`.
- Integration tests (`//go:build integration`) added if a ClickHouse repository or SQL file changed; run `make integration-test` locally to confirm before approving.
- Fuzz targets considered for new parsers on untrusted Kafka payloads (see `wire-format` checks above).
- Coverage of the *diff*, not coverage in general: every new `if`/`switch` branch should be exercised. Don't drop the combined coverage below the CI-enforced 70% threshold (`make coverage-test` to check).

#### Also note (lower priority, still report if present)
- **Observability**: new code paths without zap logs/metrics at meaningful boundaries (RPC call, Kafka produce/consume, ClickHouse write).
- **Dead code / TODOs**: unused imports, commented-out code, `TODO:` without an owner or ticket.
- **Dependencies**: `go.mod`/`go.sum` left untidy (CI runs `go mod tidy` and fails on any diff).

### 5. PR body & description

- Body follows `.github/pull_request_template.md` sections in order (`Why this should be merged`, `How this works`, `How this was tested`, `Need to be documented in RELEASES.md?`).
- If the body is missing a section or is empty/placeholder, flag it as a Nit (not a Blocker) unless the PR is non-trivial, in which case flag as a Suggestion.

### 6. Output format

Emit the review as a single markdown block the user can paste into a PR comment. Use these sections. Omit any section with no findings — do not emit empty sections or placeholder "LGTM" bullets.

Every finding bullet must contain: **(a)** a file path (and line/snippet when possible), **(b)** what is wrong, **(c)** the concrete fix or the specific question. If you cannot state (c), do not include the bullet.

```
## Review summary
<2–4 sentences: scope, overall impression, top concern. Call out the severity counts, e.g. "2 blockers, 3 suggestions".>

## Axis scan
A one-line verdict per axis so the author sees what was considered. Use ✅ clean / ⚠️ issues / — N/A.
- Bugs & edge cases: <verdict>
- Concurrency: <verdict>
- Error handling: <verdict>
- Security: <verdict>
- Performance: <verdict>
- Data-pipeline design: <verdict>
- Tests: <verdict>

## Blockers
- **[file/path:line]** <issue> — <concrete fix>

## Suggestions
- **[file/path:line]** <issue> — <concrete fix and rationale>

## Nits
- **[file/path]** <small stylistic/naming/typo> — <fix>

## Questions
- **[file/path:line]** <clarifying question whose answer would change the review>

## Nice touches
- <genuine, concrete callouts only — omit this section if there are none>

## Checklist against repo conventions
- [x|-] Migration added (if ClickHouse column/table changed)
- [x|-] Local + distributed table SQL both present (if new table)
- [x|-] Kafka errors tagged NonRetryable/Fatal appropriately (if processor changed)
- [x|-] New flag wired in flags.go + config.go + run.go + README (if config changed)
- [x|-] go.mod/go.sum tidy
- [x|-] PR body matches `.github/pull_request_template.md`
```

Severity definitions:
- **Blocker**: correctness bug, security issue, data loss/duplication risk, missing mandatory step (e.g. unwired flag, missing migration breaking a deployed cluster), or violates a hard repo convention.
- **Suggestion**: improvement that materially helps maintainability, testability, performance, or clarity; author should consider but can defer with justification.
- **Nit**: cosmetic / naming / minor style. Author may ignore.
- **Question**: something the reviewer genuinely can't tell from the diff alone AND whose answer would change the review outcome.

## Anti-patterns to avoid

- Don't post a review that's just a file listing or diff summary — every bullet must be actionable.
- Don't use vague phrasing like "consider improving", "might want to", "could be cleaner". State the specific change.
- Don't invent line numbers. If unsure of exact line, cite the file and quote the relevant snippet instead.
- Don't re-describe what the PR does in a "Changes" section — the author already knows; focus on *findings*.
- Don't flag something as a blocker unless you can state precisely why it would break, leak, corrupt data, or violate a rule.
- Don't claim tests are missing without grepping the diff for `*_test.go` in the affected package first.
- Don't pad the review with obvious observations or generic best-practice lectures.
- Don't suggest sweeping refactors unrelated to the diff. Scope feedback to the PR.

## Delivery

Default: print the review in chat as a single markdown block for the user to review before posting.

Only run `gh pr review` / `gh pr comment` when the user explicitly asks ("post the review", "submit as comment", etc.). When asked to post:

```bash
gh pr review <n> --comment --body "$(cat <<'EOF'
<review>
EOF
)"
```

Use `--request-changes` only when there is at least one Blocker *and* the user explicitly asks to block. Use `--approve` only when the user explicitly asks to approve AND there are no Blockers or Suggestions of material weight.
