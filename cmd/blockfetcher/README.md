# blockfetcher

Fetches blocks from an RPC endpoint, processes them concurrently using a sliding window scheduler, and publishes them to Kafka.

## Features

- **Realtime subscription** for new heads via WebSocket
- **Backfill** of historical gaps within a bounded window
- **Concurrency control** and backfill prioritization
- **Automatic topic creation** with configurable partitions and replication
- **Checkpoint persistence** to ClickHouse for recovery
- **Gap watchdog** that warns if the gap grows beyond the threshold
- **Prometheus metrics** for monitoring
- **Graceful shutdown** with proper resource cleanup

## Architecture

```
RPC Endpoint (WebSocket)
    ↓
Sliding Window Scheduler
    ├── Realtime Subscriber (new blocks)
    └── Backfill Workers (historical blocks)
    ↓
Kafka Producer
    ↓
Kafka Topic (blocks)
```

## Usage

### Prerequisites

1. **Start infrastructure services** (Kafka, ClickHouse):
   ```bash
   docker compose up -d
   ```

2. **Optionally set environment variables**:
   ```bash
   export CLICKHOUSE_HOSTS="localhost:9000"
   export CLICKHOUSE_USERNAME="default"
   export CLICKHOUSE_PASSWORD=""
   ...
   ```

3. **Build the application**:
   ```bash
   make build-all
   ```

### Run Locally

```bash
bin/blockfetcher run \
  --evm-chain-id 43114 \
  --bc-id "11111111111111111111111111111111LpoYY" \
  --rpc-url wss://api.avax-test.network/ext/bc/C/ws \
  --start-height 0 \
  --concurrency 16 \
  --backfill-priority 4 \
  --blocks-ch-capacity 200 \
  --max-failures 5 \
  --kafka-brokers localhost:9092 \
  --kafka-topic blocks \
  --kafka-topic-num-partitions 3 \
  --kafka-topic-replication-factor 3 \
  --verbose
```

### Run with Docker

Build the multi-binary image:

```bash
docker build -t indexer:latest .
```

Run with environment variables:

```bash
docker run --rm \
  --network avalanche-indexer_app-network \
  -e APP=blockfetcher \
  -e EVM_CHAIN_ID=43114 \
  -e BLOCKCHAIN_ID=11111111111111111111111111111111LpoYY \
  -e RPC_URL=wss://api.avax-test.network/ext/bc/C/ws \
  -e START_HEIGHT=0 \
  -e CONCURRENCY=16 \
  -e BACKFILL_PRIORITY=4 \
  -e BLOCKS_CH_CAPACITY=200 \
  -e MAX_FAILURES=5 \
  -e KAFKA_BROKERS=kafka:9093 \
  -e KAFKA_TOPIC=blocks \
  -e KAFKA_TOPIC_NUM_PARTITIONS=3 \
  -e KAFKA_TOPIC_REPLICATION_FACTOR=3 \
  -e CLICKHOUSE_HOSTS=clickhouse:9000 \
  -e CLICKHOUSE_DATABASE=test_db \
  -e CLICKHOUSE_USERNAME=default \
  -e CLICKHOUSE_PASSWORD= \
  indexer:latest run --verbose
```

Notes:
- Use the WebSocket path `/ws` with the `wss` scheme for Coreth (e.g., `wss://.../ws`).
- If you prefer building a single-service image for faster builds:

```bash
docker build -t indexer:blockfetcher --build-arg APP=blockfetcher .
```

### Flags

All flags have environment variable equivalents:

**Required flags:**
- `--evm-chain-id` / `-C` → `EVM_CHAIN_ID` (EVM chain ID)
- `--bc-id` → `BLOCKCHAIN_ID` (blockchain ID)
- `--rpc-url` / `-r` → `RPC_URL` (WebSocket RPC URL)
- `--concurrency` / `-c` → `CONCURRENCY` (number of concurrent workers)
- `--backfill-priority` / `-b` → `BACKFILL_PRIORITY` (backfill worker priority, must be < concurrency)
- `--kafka-brokers` → `KAFKA_BROKERS` (Kafka brokers, comma-separated, default: localhost:9092)
- `--kafka-topic` / `-t` → `KAFKA_TOPIC` (Kafka topic for blocks)

**Optional flags:**
- `--start-height` / `-s` → `START_HEIGHT` (default: 0, fetches from checkpoint if 0)
- `--end-height` / `-e` → `END_HEIGHT` (optional; if unset the latest is used)
- `--blocks-ch-capacity` / `-B` → `BLOCKS_CH_CAPACITY` (default: 100, subscription channel capacity)
- `--max-failures` / `-f` → `MAX_FAILURES` (default: 3, max failures before stopping)
- `--kafka-enable-logs` / `-l` → `KAFKA_ENABLE_LOGS` (default: false, enable Kafka client logs)
- `--kafka-client-id` → `KAFKA_CLIENT_ID` (default: blockfetcher)
- `--kafka-topic-num-partitions` → `KAFKA_TOPIC_NUM_PARTITIONS` (default: 1, automatically creates/validates topic with this partition count)
- `--kafka-topic-replication-factor` → `KAFKA_TOPIC_REPLICATION_FACTOR` (default: 1, automatically creates/validates topic with this replication factor)
- `--checkpoint-table-name` / `-T` → `CHECKPOINT_TABLE_NAME` (default: test_db.checkpoints, ClickHouse table for checkpoints)
- `--checkpoint-interval` / `-i` → `CHECKPOINT_INTERVAL` (default: 1m, checkpoint write interval)
- `--gap-watchdog-interval` / `-g` → `GAP_WATCHDOG_INTERVAL` (default: 15m, gap check interval)
- `--gap-watchdog-max-gap` / `-G` → `GAP_WATCHDOG_MAX_GAP` (default: 100, max gap before warning)
- `--metrics-host` → `METRICS_HOST` (default: empty, metrics server host)
- `--metrics-port` / `-m` → `METRICS_PORT` (default: 9090, metrics server port)
- `--verbose` / `-v` → none (enable verbose logging)

### Configuration tips
- `BACKFILL_PRIORITY` must be less than `CONCURRENCY`.
- For heavy realtime load, tune `BLOCKS_CH_CAPACITY` to absorb bursts.
- Ensure `RPC_URL` is reachable from within your container environment.
- `KAFKA_BROKERS` can be a comma-separated list (e.g., `broker1:9092,broker2:9092`).
- Enable `KAFKA_ENABLE_LOGS=true` for debugging Kafka connectivity issues.
- **Topic management**: The blockfetcher automatically ensures the Kafka topic exists with the specified `--kafka-topic-num-partitions` and `--kafka-topic-replication-factor`. It will create the topic if it doesn't exist, or increase partitions if needed. Note: partitions cannot be decreased and replication factor cannot be changed after creation.

### Exit behavior
- Returns a non-zero exit code on unrecoverable errors (e.g., RPC dial failure, failure threshold exceeded, Kafka fatal errors).
- Gracefully exits on `SIGTERM`/`SIGINT`.

