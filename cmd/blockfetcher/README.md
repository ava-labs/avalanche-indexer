## blockfetcher

Fetches blocks from an RPC endpoint, processes them concurrently using a sliding window scheduler, and publishes them to Kafka.

### Features
- Realtime subscription for new heads
- Backfill of historical gaps within a bounded window
- Concurrency control and backfill prioritization
- Failure thresholds and graceful shutdown
- Kafka integration for block publishing

### Usage

Run locally (from repo root):

```bash
make build-all 

bin/blockfetcher run \
  --rpc-url wss://api.avax-test.network/ext/bc/C/ws \
  --start-height 0 \
  --concurrency 16 \
  --backfill-priority 4 \
  --blocks-ch-capacity 200 \
  --max-failures 5 \
  --kafka-brokers localhost:9092 \
  --kafka-topic blocks \
  --verbose
```

All flags have environment variable equivalents:
- `--chain-id` → `CHAIN_ID` 
- `--rpc-url` / `-r` → `RPC_URL` (required)
- `--start-height` / `-s` → `START_HEIGHT` (required)
- `--end-height` / `-e` → `END_HEIGHT` (optional; if unset the latest is used)
- `--concurrency` / `-c` → `CONCURRENCY` (required)
- `--backfill-priority` / `-b` → `BACKFILL_PRIORITY` (required)
- `--blocks-ch-capacity` / `-B` → `BLOCKS_CH_CAPACITY` (default: 100)
- `--max-failures` / `-f` → `MAX_FAILURES` (default: 3)
- `--kafka-brokers` → `KAFKA_BROKERS` (required, default: localhost:9092)
- `--kafka-topic` / `-t` → `KAFKA_TOPIC` (required)
- `--kafka-enable-logs` / `-l` → `KAFKA_ENABLE_LOGS` (default: false)
- `--kafka-client-id` → `KAFKA_CLIENT_ID` (default: blockfetcher)
- `--verbose` / `-v` → none (pass `--verbose`)


### Docker

Build the multi-binary image (all services):

```bash
docker build -t indexer:latest .
```

Run `blockfetcher` with environment variables (ENTRYPOINT selects the binary by `APP`):

```bash
docker run --rm \
  -e APP=blockfetcher \
  -e CHAIN_ID=43113 \
  -e RPC_URL=wss://api.avax-test.network/ext/bc/C/ws \
  -e START_HEIGHT=0 \
  -e CONCURRENCY=16 \
  -e BACKFILL_PRIORITY=4 \
  -e BLOCKS_CH_CAPACITY=200 \
  -e MAX_FAILURES=5 \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPIC=blocks \
  indexer:latest run --verbose
```

Notes:
- Use the WebSocket path `/ws` with the `wss` scheme for Coreth (e.g., `wss://.../ws`).
- If you prefer building a single-service image for faster builds:

```bash
docker build -t indexer:blockfetcher --build-arg APP=blockfetcher .
docker run --rm \
  -e APP=blockfetcher \
  -e CHAIN_ID=43113 \
  -e RPC_URL=wss://api.avax-test.network/ext/bc/C/ws \
  -e START_HEIGHT=0 \
  -e CONCURRENCY=16 \
  -e BACKFILL_PRIORITY=4 \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPIC=blocks \
  indexer:blockfetcher run --verbose
```

### Configuration tips
- `BACKFILL_PRIORITY` must be less than `CONCURRENCY`.
- For heavy realtime load, tune `BLOCKS_CH_CAPACITY` to absorb bursts.
- Ensure `RPC_URL` is reachable from within your container environment.
- `KAFKA_BROKERS` can be a comma-separated list (e.g., `broker1:9092,broker2:9092`).
- Enable `KAFKA_ENABLE_LOGS=true` for debugging Kafka connectivity issues.

### Exit behavior
- Returns a non-zero exit code on unrecoverable errors (e.g., RPC dial failure, failure threshold exceeded, Kafka fatal errors).
- Gracefully exits on `SIGTERM`/`SIGINT`.

