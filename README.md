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

### Contributing
- Open an issue or pull request
- Run `make unit-test` and `make lint` before submitting

### License

See `LICENSE`.


