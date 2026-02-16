## consumerindexer

Consumes blocks from Kafka pipeline with concurrent processing, automatic offset management, and DLQ support.

### Features
- **Concurrent Processing**: Configurable concurrency with semaphore-based throttling
- **At-Least-Once Delivery**: Sliding window offset commits ensure no data loss
- **Dead Letter Queue**: Failed messages automatically sent to DLQ topic
- **Graceful Shutdown**: Waits for in-flight messages before terminating

### Usage

### Run Locally (Development)

**Note:** The example below uses minimal Kafka configuration (1 partition, replication factor 1) suitable for **local development and testing** with a single-broker setup. Local Kafka (docker-compose) doesn't require SASL authentication.

```bash
bin/consumerindexer run \
  --bootstrap-servers localhost:9092 \
  --group-id my-consumer-group \
  --topic blocks \
  --dlq-topic blocks-dlq \
  --publish-to-dlq \
  --concurrency 10 \
  --kafka-topic-num-partitions 1 \
  --kafka-topic-replication-factor 1 \
  --kafka-dlq-topic-num-partitions 1 \
  --kafka-dlq-topic-replication-factor 1 \
  --clickhouse-hosts localhost:9000 \
  --clickhouse-cluster default \
  --clickhouse-database default \
  --clickhouse-username default
```

### Run with SASL Authentication (OCI Kafka, etc.)

For authenticated Kafka clusters (e.g., Oracle Cloud Infrastructure Kafka):

```bash
bin/consumerindexer run \
  --bootstrap-servers "your-kafka-broker.example.com:9092" \
  --group-id my-consumer-group \
  --topic blocks \
  --dlq-topic blocks-dlq \
  --publish-to-dlq \
  --concurrency 10 \
  --kafka-sasl-username "YOUR_SASL_USERNAME" \
  --kafka-sasl-password "YOUR_SASL_PASSWORD" \
  --kafka-sasl-mechanism "SCRAM-SHA-512" \
  --kafka-security-protocol "SASL_SSL" \
  --kafka-topic-num-partitions 3 \
  --kafka-topic-replication-factor 3 \
  --kafka-dlq-topic-num-partitions 3 \
  --kafka-dlq-topic-replication-factor 3 \
  --clickhouse-hosts localhost:9000 \
  --clickhouse-cluster default \
  --clickhouse-database default \
  --clickhouse-username default
```

Or using environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS="your-kafka-broker.example.com:9092"
export KAFKA_GROUP_ID="my-consumer-group"
export KAFKA_TOPIC="blocks"
export KAFKA_DLQ_TOPIC="blocks-dlq"
export KAFKA_PUBLISH_TO_DLQ="true"
export KAFKA_SASL_USERNAME="YOUR_SASL_USERNAME"
export KAFKA_SASL_PASSWORD="YOUR_SASL_PASSWORD"
export KAFKA_SASL_MECHANISM="SCRAM-SHA-512"
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
export KAFKA_TOPIC_NUM_PARTITIONS="3"
export KAFKA_TOPIC_REPLICATION_FACTOR="3"
export CLICKHOUSE_HOSTS="localhost:9000"
export CLICKHOUSE_USERNAME="default"
export CLICKHOUSE_CLUSTER="default"
export CLICKHOUSE_DATABASE="default"
bin/consumerindexer run --verbose
```

### Flags

All flags have environment variable equivalents:

**Kafka flags:**
- `--bootstrap-servers` / `-b` → `KAFKA_BOOTSTRAP_SERVERS` (required)
- `--group-id` / `-g` → `KAFKA_GROUP_ID` (required)
- `--topic` / `-t` → `KAFKA_TOPIC` (required, single topic)
- `--dlq-topic` → `KAFKA_DLQ_TOPIC` (optional, dead letter queue topic)
- `--publish-to-dlq` → `KAFKA_PUBLISH_TO_DLQ` (default: false, enable publishing failed messages to DLQ)
- `--auto-offset-reset` / `-o` → `KAFKA_AUTO_OFFSET_RESET` (default: "earliest")
- `--concurrency` → `KAFKA_CONCURRENCY` (default: 10, concurrent processors)
- `--offset-commit-interval` → `KAFKA_OFFSET_COMMIT_INTERVAL` (default: 10s)
- `--enable-kafka-logs` → `KAFKA_ENABLE_LOGS` (default: false, enable librdkafka logs)
- `--session-timeout` → `KAFKA_SESSION_TIMEOUT` (default: 240s, session timeout)
- `--max-poll-interval` → `KAFKA_MAX_POLL_INTERVAL` (default: 3400s, max poll interval)
- `--flush-timeout` → `KAFKA_FLUSH_TIMEOUT` (default: 15s, producer flush timeout on close)
- `--goroutine-wait-timeout` → `KAFKA_GOROUTINE_WAIT_TIMEOUT` (default: 30s, wait timeout for in-flight messages)
- `--poll-interval` → `KAFKA_POLL_INTERVAL` (default: 100ms, consumer poll interval)
- `--kafka-topic-num-partitions` → `KAFKA_TOPIC_NUM_PARTITIONS` (default: 1, automatically ensures topic has this partition count)
- `--kafka-topic-replication-factor` → `KAFKA_TOPIC_REPLICATION_FACTOR` (default: 1, automatically ensures topic has this replication factor)
- `--kafka-topic-retention-ms` → `KAFKA_TOPIC_RETENTION_MS` (default: 604800000 / 7 days, main topic retention time in milliseconds)
- `--kafka-topic-retention-bytes` → `KAFKA_TOPIC_RETENTION_BYTES` (default: 161061273600 / 150GB, main topic retention size in bytes)
- `--kafka-dlq-topic-num-partitions` → `KAFKA_DLQ_TOPIC_NUM_PARTITIONS` (default: 1, DLQ topic partition count)
- `--kafka-dlq-topic-replication-factor` → `KAFKA_DLQ_TOPIC_REPLICATION_FACTOR` (default: 1, DLQ topic replication factor)
- `--kafka-dlq-topic-retention-ms` → `KAFKA_DLQ_TOPIC_RETENTION_MS` (default: 604800000 / 7 days, DLQ topic retention time in milliseconds)
- `--kafka-dlq-topic-retention-bytes` → `KAFKA_DLQ_TOPIC_RETENTION_BYTES` (default: 161061273600 / 150GB, DLQ topic retention size in bytes)
- `--kafka-sasl-username` → `KAFKA_SASL_USERNAME` (optional, SASL username for authenticated Kafka)
- `--kafka-sasl-password` → `KAFKA_SASL_PASSWORD` (optional, SASL password for authenticated Kafka)
- `--kafka-sasl-mechanism` → `KAFKA_SASL_MECHANISM` (default: SCRAM-SHA-512, SASL mechanism: SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN)
- `--kafka-security-protocol` → `KAFKA_SECURITY_PROTOCOL` (default: SASL_SSL, security protocol: SASL_SSL or SASL_PLAINTEXT)
- `--verbose` / `-v` → none (enable verbose application logging)

**ClickHouse flags:**
- `--clickhouse-hosts` → `CLICKHOUSE_HOSTS` (default: "localhost:9000", comma-separated)
- `--clickhouse-cluster` → `CLICKHOUSE_CLUSTER` (default: "default")
- `--clickhouse-database` → `CLICKHOUSE_DATABASE` (default: "default")
- `--clickhouse-username` → `CLICKHOUSE_USERNAME` (default: "default")
- `--clickhouse-password` → `CLICKHOUSE_PASSWORD` (default: "")
- `--clickhouse-debug` → `CLICKHOUSE_DEBUG` (default: false)
- `--clickhouse-insecure-skip-verify` → `CLICKHOUSE_INSECURE_SKIP_VERIFY` (default: true)
- `--raw-blocks-table-name` → `CLICKHOUSE_RAW_BLOCKS_TABLE_NAME` (default: "default.raw_blocks")
- `--raw-transactions-table-name` → `CLICKHOUSE_RAW_TRANSACTIONS_TABLE_NAME` (default: "default.raw_transactions")

Tables are automatically created if they don't exist. See `--help` for additional ClickHouse connection tuning parameters.

**Metrics flags:**
- `--metrics-host` → `METRICS_HOST` (default: "" for all interfaces)
- `--metrics-port` / `-m` → `METRICS_PORT` (default: 9090)
- `--chain-id` / `-C` → `CHAIN_ID` (optional, metrics label e.g., 43114 for C-Chain mainnet)
- `--environment` / `-E` → `ENVIRONMENT` (optional, metrics label e.g., "production", "staging")
- `--region` / `-R` → `REGION` (optional, metrics label e.g., "us-east-1")
- `--cloud-provider` / `-P` → `CLOUD_PROVIDER` (optional, metrics label e.g., "aws", "oci", "gcp")

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
  -e KAFKA_PUBLISH_TO_DLQ=true \
  -e KAFKA_CONCURRENCY=20 \
  -e KAFKA_TOPIC_NUM_PARTITIONS=1 \
  -e KAFKA_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_DLQ_TOPIC_NUM_PARTITIONS=1 \
  -e KAFKA_DLQ_TOPIC_REPLICATION_FACTOR=1 \
  -e CLICKHOUSE_HOSTS=clickhouse:9000 \
  -e CLICKHOUSE_USERNAME=default \
  -e CLICKHOUSE_CLUSTER=default \
  -e CLICKHOUSE_DATABASE=default \
  -e METRICS_PORT=9090 \
  -e CHAIN_ID=43114 \
  -e ENVIRONMENT=production \
  -e REGION=us-east-1 \
  -e CLOUD_PROVIDER=aws \
  indexer:latest run --verbose
```

### Configuration Tips

**Concurrency:**
- `--concurrency` controls parallel message processing
- Higher values increase throughput but use more resources
- Recommended: 10-50 depending on workload and resources

**Dead Letter Queue:**
- Set `--publish-to-dlq` to enable automatic publishing of failed messages to DLQ
- `--dlq-topic` specifies the DLQ topic name
- The consumerindexer automatically ensures both main topic and DLQ topic exist with the specified partition counts and replication factors
- Monitor DLQ for parsing/validation errors
- Note: Topics are created automatically if they don't exist, or partitions are increased if needed. Partitions cannot be decreased and replication factor cannot be changed after creation.

**Retention Settings:**
- Configure `--kafka-topic-retention-ms` and `--kafka-topic-retention-bytes` for the main topic (defaults: 604800000 ms / 7 days, 161061273600 bytes / 150GB)
- Configure `--kafka-dlq-topic-retention-ms` and `--kafka-dlq-topic-retention-bytes` for the DLQ topic separately (same defaults)
- The consumerindexer automatically applies these settings when creating or updating topics
- Use `-1` for infinite retention on either setting

**SASL Authentication:**
- For authenticated Kafka clusters (e.g., OCI Kafka), provide `--kafka-sasl-username` and `--kafka-sasl-password`
- SASL is automatically applied to consumer, DLQ producer, and admin clients
- Supported mechanisms: `SCRAM-SHA-256`, `SCRAM-SHA-512`, `PLAIN`
- Supported protocols: `SASL_SSL` (default), `SASL_PLAINTEXT`
- Local Kafka (docker-compose) typically doesn't require SASL unless explicitly configured

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
- Waits up to `--goroutine-wait-timeout` (default: 30s) for in-flight messages to complete
- DLQ producer flushes pending messages with `--flush-timeout` (default: 15s) before shutdown
- Returns non-zero exit code on fatal errors

### Delete Resources 
As a clean up it might be needed to delete all blocks, transactions and logs for specific chain. Use `remove` in this case:
```bash
./bin/consumerindexer remove --evm-chain-id 43114
```
