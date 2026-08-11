## consumerindexer

Consumes blockchain data from Kafka pipeline with concurrent processing, automatic offset management, and DLQ support.

### Features
- **Three Operating Modes**: Blocks mode (default), Traces mode for debug traces, and ICM mode for Teleporter/ICM events
- **Concurrent Processing**: Configurable concurrency with semaphore-based throttling
- **At-Least-Once Delivery**: Sliding window offset commits ensure no data loss
- **Dead Letter Queue**: Failed messages automatically sent to DLQ topic
- **Graceful Shutdown**: Waits for in-flight messages before terminating

### Modes

#### Blocks Mode (default)
Processes EVM blocks and persists:
- Raw blocks to `raw_blocks` table
- Transactions to `raw_transactions` table
- Event logs to `raw_logs` table

#### Traces Mode
Processes debug traces (internal transactions) and persists to `internal_transactions` table:
- Flattens nested call traces into individual records
- Captures CALL, DELEGATECALL, STATICCALL, CREATE operations
- Tracks gas usage, reverts, and errors
- Maintains hierarchical call indices (e.g., `call_0`, `call_0_0`, `call_0_0_1`)

**Internal Transactions Schema:**
- `blockchain_id` - Avalanche blockchain ID
- `evm_chain_id` - EVM chain ID (UInt256)
- `block_number` - Block number
- `transaction_hash` - Parent transaction hash
- `type` - Call type (CALL, DELEGATECALL, STATICCALL, CREATE, etc.)
- `from` - Caller address
- `to` - Callee address
- `value` - Value transferred (wei as string)
- `gas` - Gas limit provided
- `gas_used` - Actual gas used
- `revert` - Whether the call reverted
- `error` - Error message if reverted
- `revert_reason` - Revert reason data
- `input` - Call input data
- `output` - Call output data
- `call_index` - Hierarchical index (e.g., `call_0_1_2`)

#### ICM Mode
Processes Avalanche Teleporter/ICM cross-chain messages. Filters logs by Teleporter contract address and topic0, ABI-decodes seven event types, and persists to:
- `icm_messages` — AggregatingMergeTree that merges partial rows from multiple event types into one merged message row per (source_chain, destination_chain, message_id)
- `icm_send_events` — `SendCrossChainMessage` events
- `icm_receive_events` — `ReceiveCrossChainMessage` events
- `icm_message_executed_events` — `MessageExecuted` events
- `icm_message_execution_failed_events` — `MessageExecutionFailed` events
- `icm_receipt_events` — `ReceiptReceived` events
- `icm_add_fee_events` — `AddFeeAmount` events
- `icm_relayer_reward_redeemed_events` — `RelayerRewardsRedeemed` events

Requires `--teleporter-contract-addresses` (at least one). Supports `--enable-clickhouse-batch-writes` to batch event table writes (partial writes to `icm_messages` are always immediate regardless of batch mode).

### Usage

### Run Locally (Development)

**Note:** The example below uses minimal Kafka configuration (1 partition, replication factor 1) suitable for **local development and testing** with a single-broker setup. Local Kafka (docker-compose) doesn't require SASL authentication.

#### Blocks Mode (default)
```bash
bin/consumerindexer run \
  --mode blocks \
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
  --clickhouse-username default \
  --metrics-port 9099
```

#### Traces Mode
```bash
bin/consumerindexer run \
  --mode traces \
  --bootstrap-servers localhost:9092 \
  --group-id my-trace-consumer-group \
  --topic traces \
  --dlq-topic traces-dlq \
  --publish-to-dlq \
  --concurrency 10 \
  --kafka-topic-num-partitions 1 \
  --kafka-topic-replication-factor 1 \
  --kafka-dlq-topic-num-partitions 1 \
  --kafka-dlq-topic-replication-factor 1 \
  --clickhouse-hosts localhost:9000 \
  --clickhouse-cluster default \
  --clickhouse-database default \
  --clickhouse-username default \
  --internal-transactions-table-name internal_transactions
```

#### ICM Mode
```bash
bin/consumerindexer run \
  --mode icm \
  --bootstrap-servers localhost:9092 \
  --group-id my-icm-consumer-group \
  --topic icm-blocks \
  --dlq-topic icm-blocks-dlq \
  --publish-to-dlq \
  --concurrency 10 \
  --kafka-topic-num-partitions 1 \
  --kafka-topic-replication-factor 1 \
  --kafka-dlq-topic-num-partitions 1 \
  --kafka-dlq-topic-replication-factor 1 \
  --clickhouse-hosts localhost:9000 \
  --clickhouse-cluster default \
  --clickhouse-database default \
  --clickhouse-username default \
  --teleporter-contract-addresses 0xD820f95Bd8A5b7a0E3E01f5fC08Ed8D17E8E1E0 \
  --metrics-port 9099
```

### Run with SASL Authentication (OCI Kafka, etc.)

For authenticated Kafka clusters (e.g., Oracle Cloud Infrastructure Kafka):

```bash
bin/consumerindexer run \
  --mode blocks \
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

**Note:** Change `--mode` to `traces` and `--topic` to your traces topic when processing debug traces.

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

**Application flags:**
- `--mode` → `MODE` (default: "blocks", options: "blocks", "traces", or "icm")
- `--verbose` / `-v` → none (enable verbose application logging)

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
- `--kafka-topic-message-max-bytes` → `KAFKA_TOPIC_MESSAGE_MAX_BYTES` (optional, max message size for main and DLQ topics when creating/updating; unset uses broker default)

**Primary consumer retry policy:**
- `--consumer-retry-max-retries` → `CONSUMER_RETRY_MAX_RETRIES` (default: 3; `-1` = infinite, `0` = disabled)
- `--consumer-retry-base-delay` → `CONSUMER_RETRY_BASE_DELAY` (default: 500ms, initial backoff between retries)
- `--consumer-retry-max-delay` → `CONSUMER_RETRY_MAX_DELAY` (default: 2s, cap on backoff delay)

**Kafka SASL flags:**
- `--kafka-sasl-username` → `KAFKA_SASL_USERNAME` (optional, SASL username for authenticated Kafka)
- `--kafka-sasl-password` → `KAFKA_SASL_PASSWORD` (optional, SASL password for authenticated Kafka)
- `--kafka-sasl-mechanism` → `KAFKA_SASL_MECHANISM` (default: SCRAM-SHA-512, SASL mechanism: SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN)
- `--kafka-security-protocol` → `KAFKA_SECURITY_PROTOCOL` (default: SASL_SSL, security protocol: SASL_SSL or SASL_PLAINTEXT)

**DLQ consumer (secondary consumer on the DLQ topic):**
- `--enable-dlq-consumer` → `ENABLE_DLQ_CONSUMER` (default: false, run a second consumer group that reads the DLQ and retries indefinitely)
- `--dlq-consumer-group-id` → `KAFKA_DLQ_CONSUMER_GROUP_ID` (required when DLQ consumer is enabled; must differ from `--group-id`)
- `--dlq-consumer-concurrency` → `KAFKA_DLQ_CONSUMER_CONCURRENCY` (default: 1, keep low; DLQ path retries without cap)
- `--dlq-consumer-offset-commit-interval` → `KAFKA_DLQ_CONSUMER_OFFSET_COMMIT_INTERVAL` (default: 10s)
- `--dlq-consumer-session-timeout` → `KAFKA_DLQ_CONSUMER_SESSION_TIMEOUT` (default: 240s)
- `--dlq-consumer-max-poll-interval` → `KAFKA_DLQ_CONSUMER_MAX_POLL_INTERVAL` (default: 3400s)
- `--dlq-consumer-goroutine-wait-timeout` → `KAFKA_DLQ_CONSUMER_GOROUTINE_WAIT_TIMEOUT` (default: 30s, in-flight wait on shutdown)
- `--dlq-consumer-poll-interval` → `KAFKA_DLQ_CONSUMER_POLL_INTERVAL` (default: 100ms)
- `--dlq-consumer-retry-base-delay` → `DLQ_CONSUMER_RETRY_BASE_DELAY` (default: 1s)
- `--dlq-consumer-retry-max-delay` → `DLQ_CONSUMER_RETRY_MAX_DELAY` (default: 5m)

**ClickHouse flags:**
- `--clickhouse-hosts` → `CLICKHOUSE_HOSTS` (default: "localhost:9000", comma-separated)
- `--clickhouse-cluster` → `CLICKHOUSE_CLUSTER` (default: "default")
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
- `--clickhouse-max-block-size` → `CLICKHOUSE_MAX_BLOCK_SIZE` (default: 1000, recommended max rows per block)
- `--clickhouse-max-compression-buffer` → `CLICKHOUSE_MAX_COMPRESSION_BUFFER` (default: 10240, bytes)
- `--clickhouse-client-name` → `CLICKHOUSE_CLIENT_NAME` (default: "ac-client-name", ClientInfo)
- `--clickhouse-client-version` → `CLICKHOUSE_CLIENT_VERSION` (default: "1.0", ClientInfo)
- `--clickhouse-use-http` → `CLICKHOUSE_USE_HTTP` (default: false, use HTTP instead of native protocol)

**Table name flags:**
- `--raw-blocks-table-name` → `CLICKHOUSE_RAW_BLOCKS_TABLE_NAME` (default: "raw_blocks", used in blocks mode)
- `--raw-transactions-table-name` → `CLICKHOUSE_RAW_TRANSACTIONS_TABLE_NAME` (default: "raw_transactions", used in blocks mode)
- `--raw-logs-table-name` → `CLICKHOUSE_RAW_LOGS_TABLE_NAME` (default: "raw_logs", used in blocks mode)
- `--internal-transactions-table-name` → `CLICKHOUSE_INTERNAL_TRANSACTIONS_TABLE_NAME` (default: "internal_transactions", used in traces mode)

Tables are automatically created if they don't exist.

**ICM / Teleporter flags (used in icm mode):**
- `--teleporter-contract-addresses` → `TELEPORTER_CONTRACT_ADDRESSES` (**required** when mode=icm; one or more 0x-prefixed addresses, repeated flag or comma-separated)
- `--icm-messages-table-name` → `ICM_MESSAGES_TABLE_NAME` (default: "icm_messages")
- `--icm-send-events-table-name` → `ICM_SEND_EVENTS_TABLE_NAME` (default: "icm_send_events")
- `--icm-receive-events-table-name` → `ICM_RECEIVE_EVENTS_TABLE_NAME` (default: "icm_receive_events")
- `--icm-message-executed-events-table-name` → `ICM_MESSAGE_EXECUTED_EVENTS_TABLE_NAME` (default: "icm_message_executed_events")
- `--icm-message-execution-failed-events-table-name` → `ICM_MESSAGE_EXECUTION_FAILED_EVENTS_TABLE_NAME` (default: "icm_message_execution_failed_events")
- `--icm-receipts-events-table-name` → `ICM_RECEIPTS_EVENTS_TABLE_NAME` (default: "icm_receipts_events")
- `--icm-fee-info-events-table-name` → `ICM_FEE_INFO_EVENTS_TABLE_NAME` (default: "icm_fee_info_events")
- `--icm-fee-redemptions-events-table-name` → `ICM_FEE_REDEMPTIONS_EVENTS_TABLE_NAME` (default: "icm_fee_redemptions_events")

**Batch writer (optional, aggregates block writes to ClickHouse):**
- `--enable-clickhouse-batch-writes` → `ENABLE_CLICKHOUSE_BATCH_WRITES` (default: false; when true, processors enqueue rows and a background writer batches inserts)
- `--batch-writer-workers` → `BATCH_WRITER_WORKERS` (default: 3, max concurrent flush goroutines to ClickHouse)
- `--batch-writer-max-blocks` → `BATCH_WRITER_MAX_BLOCKS` (default: 1000, max blocks per batch before flush)
- `--batch-writer-flush-timeout` → `BATCH_WRITER_FLUSH_TIMEOUT` (default: 120s, max wait after first block in a batch before flushing)

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

**Blocks mode:**
```bash
docker run --rm \
  --network avalanche-indexer_app-network \
  -e APP=consumerindexer \
  -e MODE=blocks \
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

**Traces mode:**
```bash
docker run --rm \
  --network avalanche-indexer_app-network \
  -e APP=consumerindexer \
  -e MODE=traces \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9093 \
  -e KAFKA_GROUP_ID=my-trace-consumer-group \
  -e KAFKA_TOPIC=traces \
  -e KAFKA_DLQ_TOPIC=traces-dlq \
  -e KAFKA_PUBLISH_TO_DLQ=true \
  -e KAFKA_CONCURRENCY=10 \
  -e CLICKHOUSE_HOSTS=clickhouse:9000 \
  -e CLICKHOUSE_USERNAME=default \
  -e CLICKHOUSE_CLUSTER=default \
  -e CLICKHOUSE_DATABASE=default \
  -e METRICS_PORT=9090 \
  -e CHAIN_ID=43114 \
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

**Primary consumer retries:**
- Tune `--consumer-retry-max-retries`, `--consumer-retry-base-delay`, and `--consumer-retry-max-delay` for transient ClickHouse or parsing failures before DLQ or permanent failure.

**ClickHouse batch writes:**
- Enable with `--enable-clickhouse-batch-writes` to batch inserts across blocks (see `--batch-writer-*` flags). Useful for higher throughput; ensure `--batch-writer-max-blocks` and `--batch-writer-flush-timeout` match latency and memory goals.

**DLQ consumer:**
- Requires `--publish-to-dlq`, a `--dlq-topic`, and `--enable-dlq-consumer` with a distinct `--dlq-consumer-group-id`. The DLQ consumer reprocesses failed messages with infinite retry semantics (separate from primary consumer retries).

### Exit Behavior
- Gracefully handles `SIGTERM`/`SIGINT`
- Waits up to `--goroutine-wait-timeout` (default: 30s) for in-flight messages to complete
- DLQ producer flushes pending messages with `--flush-timeout` (default: 15s) before shutdown
- Returns non-zero exit code on fatal errors

### Delete Resources 
To clean up all indexed data for a specific chain, use `remove`:
```bash
./bin/consumerindexer remove --evm-chain-id 43114
```

This deletes data by `evm_chain_id` from all tables:
- `raw_blocks`
- `raw_transactions`
- `raw_logs`
- `internal_transactions`
- `icm_send_events`
- `icm_receive_events`
- `icm_message_executed_events`
- `icm_message_execution_failed_events`
- `icm_receipt_events`
- `icm_add_fee_events`
- `icm_relayer_reward_redeemed_events`

Note: `icm_messages` is not deleted because it is keyed on `(source_blockchain_id, destination_blockchain_id, message_id)` and has no `evm_chain_id` column.

**`remove` flags** (same environment variable names as `run` where applicable):

- `--evm-chain-id` / `-C` → `EVM_CHAIN_ID` (**required**, chain ID to delete)
- `--clickhouse-hosts` → `CLICKHOUSE_HOSTS` (default: "localhost:9000")
- `--clickhouse-cluster` → `CLICKHOUSE_CLUSTER` (default: "default")
- `--clickhouse-database` → `CLICKHOUSE_DATABASE` (default: "default")
- `--clickhouse-username` → `CLICKHOUSE_USERNAME` (default: "default")
- `--clickhouse-password` → `CLICKHOUSE_PASSWORD` (default: "")
- `--clickhouse-debug` → `CLICKHOUSE_DEBUG`
- `--clickhouse-insecure-skip-verify` → `CLICKHOUSE_INSECURE_SKIP_VERIFY` (default: true)
- `--clickhouse-max-execution-time` → `CLICKHOUSE_MAX_EXECUTION_TIME` (default: 60)
- `--clickhouse-dial-timeout` → `CLICKHOUSE_DIAL_TIMEOUT` (default: 30)
- `--clickhouse-max-open-conns` → `CLICKHOUSE_MAX_OPEN_CONNS` (default: 5)
- `--clickhouse-max-idle-conns` → `CLICKHOUSE_MAX_IDLE_CONNS` (default: 5)
- `--clickhouse-conn-max-lifetime` → `CLICKHOUSE_CONN_MAX_LIFETIME` (default: 10)
- `--clickhouse-block-buffer-size` → `CLICKHOUSE_BLOCK_BUFFER_SIZE` (default: 10)
- `--clickhouse-max-block-size` → `CLICKHOUSE_MAX_BLOCK_SIZE` (default: 1000)
- `--clickhouse-max-compression-buffer` → `CLICKHOUSE_MAX_COMPRESSION_BUFFER` (default: 10240)
- `--clickhouse-client-name` → `CLICKHOUSE_CLIENT_NAME` (default: "ac-client-name")
- `--clickhouse-client-version` → `CLICKHOUSE_CLIENT_VERSION` (default: "1.0")
- `--clickhouse-use-http` → `CLICKHOUSE_USE_HTTP` (default: false)
- `--raw-blocks-table-name` → `CLICKHOUSE_RAW_BLOCKS_TABLE_NAME`
- `--raw-transactions-table-name` → `CLICKHOUSE_RAW_TRANSACTIONS_TABLE_NAME`
- `--raw-logs-table-name` → `CLICKHOUSE_RAW_LOGS_TABLE_NAME`
- `--internal-transactions-table-name` → `CLICKHOUSE_INTERNAL_TRANSACTIONS_TABLE_NAME` (default: "internal_transactions")
- `--icm-send-events-table-name` → `ICM_SEND_EVENTS_TABLE_NAME` (default: "icm_send_events")
- `--icm-receive-events-table-name` → `ICM_RECEIVE_EVENTS_TABLE_NAME` (default: "icm_receive_events")
- `--icm-message-executed-events-table-name` → `ICM_MESSAGE_EXECUTED_EVENTS_TABLE_NAME` (default: "icm_message_executed_events")
- `--icm-message-execution-failed-events-table-name` → `ICM_MESSAGE_EXECUTION_FAILED_EVENTS_TABLE_NAME` (default: "icm_message_execution_failed_events")
- `--icm-receipts-events-table-name` → `ICM_RECEIPTS_EVENTS_TABLE_NAME` (default: "icm_receipts_events")
- `--icm-fee-info-events-table-name` → `ICM_FEE_INFO_EVENTS_TABLE_NAME` (default: "icm_fee_info_events")
- `--icm-fee-redemptions-events-table-name` → `ICM_FEE_REDEMPTIONS_EVENTS_TABLE_NAME` (default: "icm_fee_redemptions_events")
