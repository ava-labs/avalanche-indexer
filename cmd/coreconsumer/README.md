# coreconsumer

Consumes EVM blocks from Kafka and persists them to DynamoDB in the format that glacier-api expects, replacing the legacy `analytics` EVM indexer pipeline.

## Features

- **Glacier-API compatible writes**: Produces DynamoDB items with the exact key patterns, GSIs, and attribute names that glacier-api queries
- **ERC transfer parsing**: Detects and indexes ERC-20, ERC-721, and ERC-1155 transfers from receipt logs
- **Two DynamoDB tables**: History table (blocks, transactions, interactions, receivables, logs, topics) and ERC table (contract metadata, token existence, ownership)
- **Concurrent processing**: Configurable concurrency with semaphore-based throttling
- **At-least-once delivery**: Sliding window offset commits ensure no data loss
- **Dead Letter Queue**: Failed messages automatically sent to DLQ topic
- **Prometheus metrics**: Monitoring via `/metrics` endpoint
- **Graceful shutdown**: Waits for in-flight messages before terminating

## DynamoDB Schema

### History Table

The history table uses a composite key (`pk`, `sk`) with 7 Global Secondary Indexes:

| GSI Name | Partition Key | Sort Key | Purpose |
|---|---|---|---|
| `latest-blocks-index` | `isBlock` | `blockSk` | Query latest blocks |
| `block-hash-index` | `blockHashKey` | `blockSk` | Query blocks/txs by hash |
| `block-number-index` | `blockNumberKey` | `blockSk` | Query blocks/txs by number |
| `deployed-contract-address-index` | `deployedContractAddressKey` | `sk` | Find contract deployments |
| `contract-address-index` | `contractAddressKey` | `sk` | Query ERC transfers by contract |
| `contract-address-token-id-index` | `contractAddress#tokenId` | `sk` | Query specific NFT transfers |
| `contract-deployer-address-index` | `contractDeployerAddress` | `sk` | Find contracts by deployer |

### ERC Table

The ERC table uses a composite key (`pk`, `sk`) with 5 Global Secondary Indexes for token metadata, ownership tracking, and interaction history.

## Usage

### Prerequisites

1. **Start infrastructure services** (Kafka and optionally LocalStack for local DynamoDB):

   ```bash
   docker compose up -d
   ```

2. **Build the application**:
   ```bash
   make build-all
   ```

### Run Locally (Development)

**Note:** The example below uses minimal Kafka configuration suitable for **local development and testing** with a single-broker setup. For local DynamoDB, use the `--dynamodb-endpoint` flag pointing to LocalStack.

```bash
bin/coreconsumer run \
  --bootstrap-servers localhost:9092 \
  --group-id coreconsumer-cchain \
  --topic blocks \
  --concurrency 10 \
  --kafka-topic-num-partitions 1 \
  --kafka-topic-replication-factor 1 \
  --dynamodb-region us-east-1 \
  --dynamodb-endpoint http://localhost:4566 \
  --dynamodb-history-table history_c-chain-mainnet \
  --dynamodb-erc-table erc_c-chain-mainnet \
  --chain-id 43114 \
  --verbose
```

### Run with DLQ

```bash
bin/coreconsumer run \
  --bootstrap-servers localhost:9092 \
  --group-id coreconsumer-cchain \
  --topic blocks \
  --dlq-topic blocks-coreconsumer-dlq \
  --publish-to-dlq \
  --concurrency 10 \
  --dynamodb-region us-east-1 \
  --dynamodb-endpoint http://localhost:4566 \
  --dynamodb-history-table history_c-chain-mainnet \
  --dynamodb-erc-table erc_c-chain-mainnet \
  --chain-id 43114 \
  --verbose
```

### Run with SASL Authentication (Production Kafka)

```bash
bin/coreconsumer run \
  --bootstrap-servers "your-kafka-broker.example.com:9092" \
  --group-id coreconsumer-cchain \
  --topic blocks \
  --dlq-topic blocks-coreconsumer-dlq \
  --publish-to-dlq \
  --concurrency 10 \
  --kafka-sasl-username "YOUR_SASL_USERNAME" \
  --kafka-sasl-password "YOUR_SASL_PASSWORD" \
  --kafka-sasl-mechanism "SCRAM-SHA-512" \
  --kafka-security-protocol "SASL_SSL" \
  --kafka-topic-num-partitions 3 \
  --kafka-topic-replication-factor 3 \
  --dynamodb-region us-east-1 \
  --dynamodb-history-table history_c-chain-mainnet \
  --dynamodb-erc-table erc_c-chain-mainnet \
  --chain-id 43114 \
  --verbose
```

Or using environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS="your-kafka-broker.example.com:9092"
export KAFKA_GROUP_ID="coreconsumer-cchain"
export KAFKA_TOPIC="blocks"
export KAFKA_DLQ_TOPIC="blocks-coreconsumer-dlq"
export KAFKA_PUBLISH_TO_DLQ="true"
export KAFKA_SASL_USERNAME="YOUR_SASL_USERNAME"
export KAFKA_SASL_PASSWORD="YOUR_SASL_PASSWORD"
export KAFKA_SASL_MECHANISM="SCRAM-SHA-512"
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
export DYNAMODB_REGION="us-east-1"
export DYNAMODB_HISTORY_TABLE="history_c-chain-mainnet"
export DYNAMODB_ERC_TABLE="erc_c-chain-mainnet"
export CHAIN_ID="43114"
bin/coreconsumer run --verbose
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
  -e APP=coreconsumer \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9093 \
  -e KAFKA_GROUP_ID=coreconsumer-cchain \
  -e KAFKA_TOPIC=blocks \
  -e KAFKA_DLQ_TOPIC=blocks-coreconsumer-dlq \
  -e KAFKA_PUBLISH_TO_DLQ=true \
  -e KAFKA_CONCURRENCY=10 \
  -e KAFKA_TOPIC_NUM_PARTITIONS=1 \
  -e KAFKA_TOPIC_REPLICATION_FACTOR=1 \
  -e DYNAMODB_REGION=us-east-1 \
  -e DYNAMODB_ENDPOINT=http://localstack:4566 \
  -e DYNAMODB_HISTORY_TABLE=history_c-chain-mainnet \
  -e DYNAMODB_ERC_TABLE=erc_c-chain-mainnet \
  -e CHAIN_ID=43114 \
  -e METRICS_PORT=9090 \
  -e ENVIRONMENT=production \
  -e REGION=us-east-1 \
  -e CLOUD_PROVIDER=aws \
  indexer:latest run --verbose
```

Or build a single-service image:

```bash
docker build -t indexer:coreconsumer --build-arg APP=coreconsumer .
```

### Flags

All flags have environment variable equivalents:

**Application flags:**
- `--verbose` / `-v` → none (enable verbose logging)

**Kafka flags:**
- `--bootstrap-servers` / `-b` → `KAFKA_BOOTSTRAP_SERVERS` (required)
- `--group-id` / `-g` → `KAFKA_GROUP_ID` (required)
- `--topic` / `-t` → `KAFKA_TOPIC` (required)
- `--dlq-topic` → `KAFKA_DLQ_TOPIC` (optional, dead letter queue topic)
- `--publish-to-dlq` → `KAFKA_PUBLISH_TO_DLQ` (default: false)
- `--auto-offset-reset` / `-o` → `KAFKA_AUTO_OFFSET_RESET` (default: "earliest")
- `--concurrency` → `KAFKA_CONCURRENCY` (default: 10)
- `--offset-commit-interval` → `KAFKA_OFFSET_COMMIT_INTERVAL` (default: 10s)
- `--enable-kafka-logs` → `KAFKA_ENABLE_LOGS` (default: false)
- `--session-timeout` → `KAFKA_SESSION_TIMEOUT` (default: 240s)
- `--max-poll-interval` → `KAFKA_MAX_POLL_INTERVAL` (default: 3400s)
- `--flush-timeout` → `KAFKA_FLUSH_TIMEOUT` (default: 15s)
- `--goroutine-wait-timeout` → `KAFKA_GOROUTINE_WAIT_TIMEOUT` (default: 30s)
- `--poll-interval` → `KAFKA_POLL_INTERVAL` (default: 100ms)
- `--kafka-topic-num-partitions` → `KAFKA_TOPIC_NUM_PARTITIONS` (default: 1)
- `--kafka-topic-replication-factor` → `KAFKA_TOPIC_REPLICATION_FACTOR` (default: 1)
- `--kafka-topic-retention-ms` → `KAFKA_TOPIC_RETENTION_MS` (default: 604800000 / 7 days)
- `--kafka-topic-retention-bytes` → `KAFKA_TOPIC_RETENTION_BYTES` (default: 161061273600 / 150GB)
- `--kafka-dlq-topic-num-partitions` → `KAFKA_DLQ_TOPIC_NUM_PARTITIONS` (default: 1)
- `--kafka-dlq-topic-replication-factor` → `KAFKA_DLQ_TOPIC_REPLICATION_FACTOR` (default: 1)
- `--kafka-dlq-topic-retention-ms` → `KAFKA_DLQ_TOPIC_RETENTION_MS` (default: 604800000 / 7 days)
- `--kafka-dlq-topic-retention-bytes` → `KAFKA_DLQ_TOPIC_RETENTION_BYTES` (default: 161061273600 / 150GB)
- `--kafka-topic-message-max-bytes` → `KAFKA_TOPIC_MESSAGE_MAX_BYTES` (optional)
- `--kafka-sasl-username` → `KAFKA_SASL_USERNAME` (optional)
- `--kafka-sasl-password` → `KAFKA_SASL_PASSWORD` (optional)
- `--kafka-sasl-mechanism` → `KAFKA_SASL_MECHANISM` (default: SCRAM-SHA-512)
- `--kafka-security-protocol` → `KAFKA_SECURITY_PROTOCOL` (default: SASL_SSL)
- `--consumer-retry-max-retries` → `CONSUMER_RETRY_MAX_RETRIES` (default: 3, -1 for infinite)
- `--consumer-retry-base-delay` → `CONSUMER_RETRY_BASE_DELAY` (default: 500ms)
- `--consumer-retry-max-delay` → `CONSUMER_RETRY_MAX_DELAY` (default: 2s)

**DynamoDB flags:**
- `--dynamodb-region` → `DYNAMODB_REGION` (default: "us-east-1")
- `--dynamodb-endpoint` → `DYNAMODB_ENDPOINT` (optional, for LocalStack/local dev)
- `--dynamodb-history-table` → `DYNAMODB_HISTORY_TABLE` (default: "history")
- `--dynamodb-erc-table` → `DYNAMODB_ERC_TABLE` (default: "erc")
- `--dynamodb-status-table` → `DYNAMODB_STATUS_TABLE` (default: "status")
- `--dynamodb-max-retries` → `DYNAMODB_MAX_RETRIES` (default: 10)
- `--dynamodb-max-inflight` → `DYNAMODB_MAX_INFLIGHT` (default: 100)

**Metrics flags:**
- `--metrics-host` → `METRICS_HOST` (default: "" for all interfaces)
- `--metrics-port` / `-m` → `METRICS_PORT` (default: 9090)
- `--chain-id` / `-C` → `CHAIN_ID` (optional, metrics label)
- `--environment` / `-E` → `ENVIRONMENT` (optional, metrics label)
- `--region` / `-R` → `REGION` (optional, metrics label)
- `--cloud-provider` / `-P` → `CLOUD_PROVIDER` (optional, metrics label)

Tables are automatically created if they don't exist.

### Configuration Tips

**Table naming:**
- Use the same table names as the legacy analytics indexer (e.g., `history_c-chain-mainnet`, `erc_c-chain-mainnet`) to enable in-place replacement without glacier-api changes.

**Consumer group:**
- Use a **separate consumer group** from `consumerindexer` since they write to different databases and must track offsets independently.

**Concurrency:**
- `--concurrency` controls parallel message processing.
- DynamoDB is the bottleneck; start with 10 and increase based on throttling metrics.
- Higher values increase throughput but also DynamoDB write costs.

**DynamoDB costs:**
- A single block can produce 100s-1000s of DynamoDB writes (block + txs + interactions + ERC transfers + logs + topics).
- Use on-demand billing mode during backfill; consider provisioned capacity with auto-scaling for steady-state.
- Monitor `ConsumedWriteCapacityUnits` in CloudWatch.

**Backfill:**
- Set `--auto-offset-reset earliest` to process all historical blocks from Kafka.
- Consider running multiple consumer instances with partitioned topics for faster backfill.

**Dead Letter Queue:**
- Set `--publish-to-dlq` to capture messages that fail after retries.
- Non-retryable errors (unmarshal failures, missing blockchainID) go directly to DLQ.
- Retryable errors (DynamoDB throttling) are retried with exponential backoff first.

**SASL Authentication:**
- For authenticated Kafka clusters, provide `--kafka-sasl-username` and `--kafka-sasl-password`.
- SASL is automatically applied to consumer, DLQ producer, and admin clients.
- Local Kafka (docker-compose) typically doesn't require SASL.

### Exit Behavior

- Gracefully handles `SIGTERM`/`SIGINT`.
- Waits up to `--goroutine-wait-timeout` (default: 30s) for in-flight messages to complete.
- DLQ producer flushes pending messages with `--flush-timeout` (default: 15s) before shutdown.
- Returns non-zero exit code on fatal errors.

### Delete Resources

To delete the DynamoDB tables for a stream:

```bash
bin/coreconsumer remove \
  --dynamodb-region us-east-1 \
  --dynamodb-endpoint http://localhost:4566 \
  --dynamodb-history-table history_c-chain-mainnet \
  --dynamodb-erc-table erc_c-chain-mainnet
```

### Known Limitations

- **Internal transactions** are not yet indexed (requires consuming from the separate `traces` Kafka topic).
- **`blockGasCost` field** defaults to `"0"` (Avalanche-specific field not yet present in Kafka messages).
- **Token metadata** (name, symbol, decimals) is not enriched (legacy system uses async SQS-based fetching).
