## Avalanche Indexer

A Go monorepo for indexing and processing Avalanche blockchain data. It includes reusable packages and runnable services under `cmd/`.

### Monorepo layout
- `cmd/` — runnable services (e.g., `blockfetcher`)
- `pkg/` — shared libraries (e.g., `slidingwindow`)
- `Dockerfile` — multi-binary image build that can run any service via `APP`
- `Makefile` — build, test, lint targets

### Requirements
- Go 1.24+
- Make
- Docker (optional, for container builds)

### Build

Build all services:

```bash
make build-all
```

Build a specific service:

```bash
APP=blockfetcher make build-app
```

Artifacts are placed in `bin/`.

### Test and lint

```bash
make unit-test
make lint
```

### Docker

This repo uses a single multi-binary Docker image. At runtime, select which binary to run with the `APP` environment variable.

Build all services into one image:

```bash
docker build -t indexer:latest .
```

Optionally, build a single service for faster builds:

```bash
docker build -t indexer:blockfetcher --build-arg APP=blockfetcher .
```

Run a service (example: `blockfetcher`):

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
  --verbose
```

See each service’s README for its full flag/env reference.

### Services
- `blockfetcher` — fetches blocks from a Coreth-compatible RPC and processes them via a sliding window.
  - See `cmd/blockfetcher/README.md`

---

### Local Development Setup

This project includes a Docker Compose configuration for local development with:
- **Apache Kafka** (KRaft mode, no Zookeeper)
- **Provectus Kafka UI**
- **ClickHouse** (with Tabix UI)

##### Quick Start

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop services
docker compose down

# Stop and remove volumes
docker compose down -v
```

##### Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| Kafka | `9092` | External client access (from host) |
| Kafka | `9093` | Internal access (container-to-container) |
| Kafka UI | `8080` | Web UI for Kafka management |
| ClickHouse | `8123` | HTTP interface |
| ClickHouse | `9000` | Native protocol |
| Tabix | `8082` | ClickHouse web UI |

##### Connecting from Host Applications

```bash
# Kafka bootstrap server
localhost:9092

# Kafka UI
http://localhost:8080

# ClickHouse
http://localhost:8123

# Tabix (ClickHouse UI)
http://localhost:8082
```

##### Connecting from Other Docker Containers

Add your container to the `kafka-network` network and use:

```bash
# Kafka bootstrap server
kafka:9093

# ClickHouse
clickhouse:8123
```

##### Kafka UI Features

The Kafka UI is pre-configured with:
- **Cluster**: `local-kafka` pointing to the local Kafka broker
- **Dynamic Config**: Enabled (`DYNAMIC_CONFIG_ENABLED=true`) — allows adding/editing clusters via the UI

Access the UI at [http://localhost:8080](http://localhost:8080) to:
- View and create topics
- Browse messages
- Manage consumer groups

---

### Examples

##### Block Fetcher

Start ingestion from specific block (no checkpoint):
```
# spin up containers (checkpoints table will be created)
docker-compose up -d

# build applications
make build-all

# run block fetcher for Fuji testnet starting from block 48662238 using default checkpoints table
./bin/blockfetcher run -r wss://api.avax-test.network/ext/bc/C/ws -C 43113 -b 9 -B 5 -c 10 -s 48662238

```

The checkpoints can be checked using UI at http://localhost:8082
To login use these values:
```
name: dev (can be any other name)
host: http://127.0.0.1:8123
login: default
```

Select `default` in ClickHouse Server tab (on the left).

checkpoint records can be added manually:
```
INSERT INTO default.checkpoints (chain_id, lowest_unprocessed_block, timestamp) 
VALUES (43114, 48662238, 1767903034)
```

Start ingestion from checkpoint:
```
# run block fetcher for Fuji testnet starting from latest un-ingested 
# block in default checkpoints table
./bin/blockfetcher run -r wss://api.avax-test.network/ext/bc/C/ws -C 43113 -b 9 -B 5 -c 10
```

### Contributing
- Open an issue or pull request
- Run `make unit-test` and `make lint` before submitting

### License

See `LICENSE`.


