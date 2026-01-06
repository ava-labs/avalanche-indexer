## blockfetcher

Fetches blocks from an RPC endpoint and processes them concurrently using a sliding window scheduler.

### Features
- Realtime subscription for new heads
- Backfill of historical gaps within a bounded window
- Concurrency control and backfill prioritization
- Failure thresholds and graceful shutdown

### Usage

Run locally (from repo root after build):

```bash
bin/blockfetcher run \
  --rpc-url wss://api.avax-test.network/ext/bc/C/ws \
  --start-height 0 \
  --concurrency 16 \
  --backfill-priority 4 \
  --blocks-ch-capacity 200 \
  --max-failures 5 \
  --verbose
```

All flags have environment variable equivalents:
- `--rpc-url` → `RPC_URL`
- `--start-height` → `START_HEIGHT`
- `--end-height` → `END_HEIGHT` (optional; if unset the latest is used)
- `--concurrency` → `CONCURRENCY`
- `--backfill-priority` → `BACKFILL_PRIORITY`
- `--blocks-ch-capacity` → `BLOCKS_CH_CAPACITY`
- `--max-failures` → `MAX_FAILURES`
- `--verbose` → none (pass `--verbose`)

### Docker

Build the multi-binary image (all services):

```bash
docker build -t indexer:latest .
```

Run `blockfetcher` with environment variables (ENTRYPOINT selects the binary by `APP`):

```bash
docker run --rm \
  -e APP=blockfetcher \
  -e RPC_URL=wss://api.avax-test.network/ext/bc/C/ws \
  -e START_HEIGHT=0 \
  -e CONCURRENCY=16 \
  -e BACKFILL_PRIORITY=4 \
  -e BLOCKS_CH_CAPACITY=200 \
  -e MAX_FAILURES=5 \
  indexer:latest --verbose
```

Notes:
- Use the WebSocket path `/ws` with the `wss` scheme for Coreth (e.g., `wss://.../ws`).
- If you prefer building a single-service image for faster builds:

```bash
docker build -t indexer:blockfetcher --build-arg APP=blockfetcher .
docker run --rm \
  -e APP=blockfetcher \
  -e RPC_URL=wss://api.avax-test.network/ext/bc/C/ws \
  -e START_HEIGHT=0 \
  -e CONCURRENCY=16 \
  -e BACKFILL_PRIORITY=4 \
  indexer:blockfetcher --verbose
```

### Configuration tips
- `BACKFILL_PRIORITY` must be less than `CONCURRENCY`.
- For heavy realtime load, tune `BLOCKS_CH_CAPACITY` to absorb bursts.
- Ensure `RPC_URL` is reachable from within your container environment.

### Exit behavior
- Returns a non-zero exit code on unrecoverable errors (e.g., RPC dial failure, failure threshold exceeded).
- Gracefully exits on `SIGTERM`/`SIGINT`.


