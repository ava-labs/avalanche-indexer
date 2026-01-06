## consumerindexer

Consumes blocks from Kafka pipeline

### Usage

Run locally (from repo root):

```bash
go run ./cmd/consumerindexer run \
  --bootstrap-servers localhost:9092 \
  --group-id my-consumer-group \
  --topics blocks,transactions
```

All flags have environment variable equivalents:
- `--bootstrap-servers` / `-b` → `KAFKA_BOOTSTRAP_SERVERS` (required)
- `--group-id` / `-g` → `KAFKA_GROUP_ID` (required)
- `--topics` / `-t` → `KAFKA_TOPICS` (required, comma-separated)
- `--auto-offset-reset` / `-o` → `KAFKA_AUTO_OFFSET_RESET` (optional, default: "earliest")
- `--verbose` / `-v` → none (pass `--verbose`)

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


