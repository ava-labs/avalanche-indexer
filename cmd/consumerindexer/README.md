## consumerindexer

Consumes blocks from Kafka pipeline with concurrent processing, automatic offset management, and DLQ support.

### Features
- **Concurrent Processing**: Configurable concurrency with semaphore-based throttling
- **At-Least-Once Delivery**: Sliding window offset commits ensure no data loss
- **Dead Letter Queue**: Failed messages automatically sent to DLQ topic
- **Graceful Shutdown**: Waits for in-flight messages before terminating

### Usage

Run locally (from repo root after build):

```bash
bin/consumerindexer run \
  --bootstrap-servers localhost:9092 \
  --group-id my-consumer-group \
  --topic blocks \
  --dlq-topic blocks-dlq \
  --max-concurrency 10 \
  --clickhouse-hosts localhost:9000 \
  --clickhouse-username default
```

Or using environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_GROUP_ID="my-consumer-group"
export KAFKA_TOPIC="blocks"
export KAFKA_DLQ_TOPIC="blocks-dlq"
export CLICKHOUSE_HOSTS="localhost:9000"
export CLICKHOUSE_USERNAME="default"
bin/consumerindexer run --verbose
```

### Flags

All flags have environment variable equivalents:

**Kafka flags:**
- `--bootstrap-servers` / `-b` → `KAFKA_BOOTSTRAP_SERVERS` (required)
- `--group-id` / `-g` → `KAFKA_GROUP_ID` (required)
- `--topic` / `-t` → `KAFKA_TOPIC` (required, single topic)
- `--dlq-topic` → `KAFKA_DLQ_TOPIC` (optional, dead letter queue topic)
- `--auto-offset-reset` / `-o` → `KAFKA_AUTO_OFFSET_RESET` (default: "earliest")
- `--max-concurrency` → `KAFKA_MAX_CONCURRENCY` (default: 10, concurrent processors)
- `--offset-commit-interval` → `KAFKA_OFFSET_COMMIT_INTERVAL` (default: 10s)
- `--enable-kafka-logs` → `KAFKA_ENABLE_LOGS` (default: false, enable librdkafka logs)
- `--verbose` / `-v` → none (enable verbose application logging)

**ClickHouse flags:**
- `--clickhouse-hosts` → `CLICKHOUSE_HOSTS` (default: "localhost:9000")
- `--clickhouse-database` → `CLICKHOUSE_DATABASE` (default: "default")
- `--clickhouse-username` → `CLICKHOUSE_USERNAME` (default: "default")
- `--clickhouse-password` → `CLICKHOUSE_PASSWORD` (default: "")
- `--clickhouse-debug` → `CLICKHOUSE_DEBUG` (default: false)
- `--clickhouse-insecure-skip-verify` → `CLICKHOUSE_INSECURE_SKIP_VERIFY` (default: true)
- `--raw-blocks-table-name` → `CLICKHOUSE_RAW_BLOCKS_TABLE_NAME` (default: "default.raw_blocks")

See `--help` for additional ClickHouse connection tuning parameters.

### Docker

Build the multi-binary image:

```bash
docker build -t indexer:latest .
```

Run with environment variables (ENTRYPOINT selects binary by `APP`):

```bash
docker run --rm \
  --network avalanche-indexer_app-network \
  -e APP=consumerindexer \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9093 \
  -e KAFKA_GROUP_ID=my-consumer-group \
  -e KAFKA_TOPIC=blocks \
  -e KAFKA_DLQ_TOPIC=blocks-dlq \
  -e KAFKA_MAX_CONCURRENCY=20 \
  -e CLICKHOUSE_HOSTS=clickhouse:9000 \
  -e CLICKHOUSE_USERNAME=default \
  indexer:latest run --verbose
```

### Configuration Tips

**Concurrency:**
- `--max-concurrency` controls parallel message processing
- Higher values increase throughput but use more resources
- Recommended: 10-50 depending on workload and resources

**Dead Letter Queue:**
- `--dlq-topic` captures messages that fail processing after retries
- Create DLQ topic before starting: `kafka-topics --create --topic blocks_dlq ...`
- Monitor DLQ for parsing/validation errors

**Offset Management:**
- `--offset-commit-interval` balances commit frequency vs. reprocessing on restart
- Shorter intervals (5s) = less reprocessing, more broker load
- Longer intervals (30s) = more reprocessing, less broker load

**Auto Offset Reset:**
- `earliest`: Process from beginning (backfill)
- `latest`: Process only new messages (real-time)
- `none`: Fail if no committed offset exists

### Exit Behavior
- Gracefully handles `SIGTERM`/`SIGINT`
- Waits up to 30s for in-flight messages to complete
- Returns non-zero exit code on fatal errors
- DLQ producer flushes pending messages before shutdown


