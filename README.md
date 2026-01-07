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
docker run --rm \
  -e APP=blockfetcher \
  -e RPC_URL=wss://api.avax-test.network/ext/bc/C/ws \
  -e START_HEIGHT=0 \
  -e CONCURRENCY=16 \
  -e BACKFILL_PRIORITY=4 \
  indexer:latest --verbose
```

See each service’s README for its full flag/env reference.

### Services
- `blockfetcher` — fetches blocks from a Coreth-compatible RPC and processes them via a sliding window.
  - See `cmd/blockfetcher/README.md`

---

### Local Kafka Development Setup

This project includes a Docker Compose configuration for local Kafka development with:
- **Apache Kafka** (KRaft mode, no Zookeeper)
- **Confluent Schema Registry**
- **Provectus Kafka UI**

![Kafka UI Topics View](docs/images/kafka-ui.png)

#### Quick Start

```bash
# Start all Kafka services
docker compose up -d

# View logs
docker compose logs -f

# Stop services
docker compose down

# Stop and remove volumes
docker compose down -v
```

#### Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| Kafka | `9092` | External client access (from host) |
| Kafka | `9093` | Internal access (container-to-container) |
| Schema Registry | `8081` | Avro/JSON/Protobuf schema management |
| Kafka UI | `8080` | Web UI for Kafka management |

#### Connecting from Host Applications

```bash
# Kafka bootstrap server
localhost:9092

# Schema Registry
http://localhost:8081

# Kafka UI
http://localhost:8080
```

#### Connecting from Other Docker Containers

Add your container to the `kafka-network` network and use:

```bash
# Kafka bootstrap server
kafka:9093

# Schema Registry
http://schema-registry:8081
```

#### Kafka UI Features

The Kafka UI is pre-configured with:
- **Cluster**: `local-kafka` pointing to the local Kafka broker
- **Schema Registry**: Already integrated
- **Dynamic Config**: Enabled (`DYNAMIC_CONFIG_ENABLED=true`) — allows adding/editing clusters via the UI

Access the UI at [http://localhost:8080](http://localhost:8080) to:
- View and create topics
- Browse messages
- Manage consumer groups
- View and register schemas

---

### Contributing
- Open an issue or pull request
- Run `make unit-test` and `make lint` before submitting

### License

See `LICENSE`.


