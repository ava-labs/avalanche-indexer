# Metrics Package

This package provides Prometheus metrics instrumentation for the Avalanche indexer, along with a local development stack for visualization.

## Quick Start

```bash
# Start the block fetcher (exposes metrics on :9090)
go run ./cmd/blockfetcher run --rpc-url=<RPC_URL> --start=<HEIGHT> --end=<HEIGHT> --concurrency=4 --backfill-priority=2

# In another terminal, start Prometheus + Grafana
docker compose up -d

# Access dashboards
# Prometheus: http://localhost:9091
# Grafana:    http://localhost:3000 (admin/admin)
```

## Available Metrics

All metrics use the `indexer` namespace.

### Sliding Window State

| Metric | Type | Description |
|--------|------|-------------|
| `indexer_lub` | Gauge | Lowest Unprocessed Block height |
| `indexer_hib` | Gauge | Highest Ingested Block height |
| `indexer_processed_set_size` | Gauge | Number of blocks in the in-memory processed set |

### Processing Counters

| Metric | Type | Description |
|--------|------|-------------|
| `indexer_blocks_processed_total` | Counter | Total blocks processed and committed |
| `indexer_lub_advances_total` | Counter | Times LUB was advanced |

### RPC Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `indexer_rpc_calls_total` | Counter | `method`, `status` | Total RPC calls |
| `indexer_rpc_duration_seconds` | Histogram | `method` | RPC call latency |
| `indexer_rpc_in_flight` | Gauge | - | Current in-progress RPC calls |

### Processing Latency

| Metric | Type | Description |
|--------|------|-------------|
| `indexer_block_processing_duration_seconds` | Histogram | End-to-end block processing time |

## Example Queries

```promql
# Current backlog (blocks behind)
indexer_hib - indexer_lub

# Processing rate (blocks/sec over 5m)
rate(indexer_blocks_processed_total[5m])

# RPC error rate
sum(rate(indexer_rpc_calls_total{status="error"}[5m]))
  / sum(rate(indexer_rpc_calls_total[5m]))

# P99 block processing latency
histogram_quantile(0.99, rate(indexer_block_processing_duration_seconds_bucket[5m]))

# P95 RPC latency by method
histogram_quantile(0.95, sum by (method, le) (rate(indexer_rpc_duration_seconds_bucket[5m])))
```

## Adding Grafana Dashboards

1. Open Grafana at http://localhost:3000
2. Add Prometheus data source: `http://prometheus:9090`
3. Create dashboards using the metrics above

## Extending Metrics

To add new metrics:

1. Define the metric in `metrics.go`:
   ```go
   myMetric: prometheus.NewCounter(prometheus.CounterOpts{
       Namespace: Namespace,
       Name:      "my_metric_total",
       Help:      "Description of what this measures",
   }),
   ```

2. Register it in `New()`:
   ```go
   reg.Register(m.myMetric),
   ```

3. Add a method to update it:
   ```go
   func (m *Metrics) RecordMyMetric() {
       m.myMetric.Inc()
   }
   ```
