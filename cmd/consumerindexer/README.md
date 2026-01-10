## consumerindexer

Consumes blocks from Kafka pipeline

### Usage

Run locally (from repo root after build):

```bash
bin/consumerindexer run \
  --bootstrap-servers localhost:9092 \
  --group-id hello-avalanche-group \
  --topics blocks,transactions \
  --clickhouse-addresses localhost:9000 \
  --clickhouse-username default
```

Or using environment variables:

```bash
export CLICKHOUSE_ADDRESSES="localhost:9000"
export CLICKHOUSE_USERNAME="default"
bin/consumerindexer run \
  --bootstrap-servers localhost:9092 \
  --group-id hello-avalanche-group \
  --topics blocks,transactions
```

All flags have environment variable equivalents:

**Kafka flags:**
- `--bootstrap-servers` / `-b` → `KAFKA_BOOTSTRAP_SERVERS` (required)
- `--group-id` / `-g` → `KAFKA_GROUP_ID` (required)
- `--topics` / `-t` → `KAFKA_TOPICS` (required, comma-separated)
- `--auto-offset-reset` / `-o` → `KAFKA_AUTO_OFFSET_RESET` (optional, default: "earliest")
- `--verbose` / `-v` → none (pass `--verbose`)

**ClickHouse flags:**
- `--clickhouse-addresses` → `CLICKHOUSE_ADDRESSES` (default: "localhost:9000", comma-separated)
- `--clickhouse-database` → `CLICKHOUSE_DATABASE` (default: "default")
- `--clickhouse-username` → `CLICKHOUSE_USERNAME` (default: "default")
- `--clickhouse-password` → `CLICKHOUSE_PASSWORD` (default: "")
- `--clickhouse-debug` → `CLICKHOUSE_DEBUG` (default: false)
- `--clickhouse-insecure-skip-verify` → `CLICKHOUSE_INSECURE_SKIP_VERIFY` (default: true)
- `--clickhouse-max-execution-time` → `CLICKHOUSE_MAX_EXECUTION_TIME` (default: 60, seconds)
- `--clickhouse-dial-timeout` → `CLICKHOUSE_DIAL_TIMEOUT` (default: 30, seconds)
- `--clickhouse-max-open-conns` → `CLICKHOUSE_MAX_OPEN_CONNS` (default: 5)
- `--clickhouse-max-idle-conns` → `CLICKHOUSE_MAX_IDLE_CONNS` (default: 5)
- `--clickhouse-conn-max-lifetime` → `CLICKHOUSE_CONN_MAX_LIFETIME` (default: 10, minutes)
- `--clickhouse-block-buffer-size` → `CLICKHOUSE_BLOCK_BUFFER_SIZE` (default: 10)
- `--clickhouse-max-block-size` → `CLICKHOUSE_MAX_BLOCK_SIZE` (default: 1000, recommended maximum number of rows in a single block)
- `--clickhouse-max-compression-buffer` → `CLICKHOUSE_MAX_COMPRESSION_BUFFER` (default: 10240, bytes)
- `--clickhouse-client-name` → `CLICKHOUSE_CLIENT_NAME` (default: "ac-client-name")
- `--clickhouse-client-version` → `CLICKHOUSE_CLIENT_VERSION` (default: "1.0")

### Docker

Build the multi-binary image (all services):

```bash
docker build -t indexer:latest .
```

Run `consumerindexer` with environment variables (ENTRYPOINT selects the binary by `APP`):

```bash
docker run --rm \
  -e APP=consumerindexer \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e KAFKA_GROUP_ID=my-consumer-group \
  -e KAFKA_TOPICS=blocks,transactions \
  -e KAFKA_AUTO_OFFSET_RESET=earliest \
  indexer:latest run --verbose
```

If you prefer building a single-service image for faster builds:

```bash
docker build -t indexer:consumerindexer --build-arg APP=consumerindexer .
docker run --rm \
  -e APP=consumerindexer \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e KAFKA_GROUP_ID=my-consumer-group \
  -e KAFKA_TOPICS=blocks,transactions \
  indexer:consumerindexer run --verbose
```

### Configuration tips
- `KAFKA_BOOTSTRAP_SERVERS` can be a comma-separated list of brokers (e.g., `broker1:9092,broker2:9092`).
- `KAFKA_TOPICS` should be a comma-separated list of topic names.
- `KAFKA_AUTO_OFFSET_RESET` options: `earliest` (from beginning), `latest` (from now), or `none` (fail if no offset).
- Ensure Kafka brokers are reachable from within your container environment.

### Exit behavior
- Returns a non-zero exit code on unrecoverable errors (e.g., Kafka connection failure, consumer errors).
- Gracefully exits on `SIGTERM`/`SIGINT`.


