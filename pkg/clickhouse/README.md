# ClickHouse Client

A ClickHouse client package for the avalanche-indexer project.

## Features

- Environment variable configuration
- Connection pooling and lifecycle management
- Health checks and error handling
- Comprehensive test coverage (unit and integration tests)

## Usage

### Basic Usage

```go
import (
    "github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
)

// Load configuration from environment variables
cfg := clickhouse.Load()

// Create a logger (using zap)
logger, _ := zap.NewProduction()
sugar := logger.Sugar()

// Create a new client
client, err := clickhouse.New(cfg, sugar)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Use the connection
conn := client.Conn()
// ... use conn for queries
```

### Configuration

The client can be configured via environment variables:

- `CLICKHOUSE_HOSTS` - Comma-separated list of ClickHouse hosts (default: `localhost:9000`)
- `CLICKHOUSE_DATABASE` - Database name (default: `default`)
- `CLICKHOUSE_USERNAME` - Username (default: `default`)
- `CLICKHOUSE_PASSWORD` - Password (default: empty)
- `CLICKHOUSE_DEBUG` - Enable debug logging (default: `false`)
- `CLICKHOUSE_INSECURE_SKIP_VERIFY` - Skip TLS verification (default: `true`)
- `CLICKHOUSE_MAX_EXECUTION_TIME` - Max execution time in seconds (default: `60`)
- `CLICKHOUSE_DIAL_TIMEOUT` - Dial timeout in seconds (default: `30`)
- `CLICKHOUSE_MAX_OPEN_CONNS` - Maximum open connections (default: `5`)
- `CLICKHOUSE_MAX_IDLE_CONNS` - Maximum idle connections (default: `5`)
- `CLICKHOUSE_CONN_MAX_LIFETIME` - Connection max lifetime in minutes (default: `10`)
- `CLICKHOUSE_BLOCK_BUFFER_SIZE` - Block buffer size (default: `10`)
- `CLICKHOUSE_MAX_BLOCK_SIZE` - Max block size in rows (default: `1000`)
- `CLICKHOUSE_MAX_COMPRESSION_BUFFER` - Max compression buffer in bytes (default: `10240`)
- `CLICKHOUSE_CLIENT_NAME` - Client name for ClickHouse ClientInfo (default: `ac-client-name`)
- `CLICKHOUSE_CLIENT_VERSION` - Client version (default: `1.0`)

## Testing

### Unit Tests

Run unit tests:

```bash
go test ./pkg/clickhouse
```

### Integration Tests

Integration tests require a running ClickHouse instance. Use the root docker-compose file:

```bash
# Start ClickHouse (from repo root)
docker-compose up -d clickhouse

# Run integration tests
go test -tags=integration ./pkg/clickhouse

# Stop ClickHouse
docker-compose stop clickhouse
```

The integration tests will automatically load `.env.test` if present, or use default values.

## Docker Compose

The root `docker-compose.yml` includes a ClickHouse service for local testing. It starts a ClickHouse server on:
- Port `8123` (HTTP interface)
- Port `9000` (Native protocol)

## Notes

- The client performs a health check (ping) on creation. If the connection fails, the client creation will fail.
- The client uses LZ4 compression by default.
- Connection pooling is configured for optimal performance.

