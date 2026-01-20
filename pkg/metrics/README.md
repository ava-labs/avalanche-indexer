# Metrics Package

This package provides Prometheus metrics instrumentation for the Avalanche indexer, along with a local development stack for visualization.

## Quick Start

```bash
Start the blockfetcher as described in /cmd/blockfetcher/main.go, then navigate to the following pages for metrics and visibility.

# Access dashboards
# Prometheus: http://localhost:9091
# Grafana:    http://localhost:3000 (admin/admin)
# Health:     http://localhost:9090/health
```

## Available Metrics

All metrics use the `indexer` namespace.

### Sliding Window State

| Metric | Type | Description |
|--------|------|-------------|
| `indexer_lowest` | Gauge | Lowest unprocessed block height (window lower bound) |
| `indexer_highest` | Gauge | Highest ingested block height (window upper bound) |
| `indexer_processed_set_size` | Gauge | Number of blocks in the in-memory processed set |

### Processing Counters

| Metric | Type | Description |
|--------|------|-------------|
| `indexer_blocks_processed_total` | Counter | Total blocks processed and committed |
| `indexer_lowest_advances_total` | Counter | Times the lowest bound was advanced |

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
indexer_highest - indexer_lowest

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

## Kubernetes Deployment

### Prometheus Scraping Annotations

Add annotations to your pod spec based on your monitoring infrastructure. In Helm, these would be conditioned on a values flag like `prometheus.type: "standard"` or `prometheus.type: "grafana-cloud"`.

#### Self-Hosted / Cloud-Managed Prometheus

For self-hosted Prometheus or cloud-managed Prometheus (AWS Managed Prometheus, GCP Managed Prometheus, etc.):

```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "9090"      # Must match --metrics-port flag
    prometheus.io/path: "/metrics"
```

#### Grafana Cloud (Alloy)

For Grafana Cloud using Alloy:

```yaml
metadata:
  annotations:
    k8s.grafana.com/scrape: "true"
    k8s.grafana.com/port: "9090"           # Must match --metrics-port flag
    k8s.grafana.com/metrics_path: "/metrics"
```

#### Both

If your infrastructure supports both then include all annotations for maximum compatibility:

```yaml
metadata:
  annotations:
    # Standard Prometheus
    prometheus.io/scrape: "true"
    prometheus.io/port: "9090"
    prometheus.io/path: "/metrics"
    # Grafana Cloud (Alloy)
    k8s.grafana.com/scrape: "true"
    k8s.grafana.com/port: "9090"
    k8s.grafana.com/metrics_path: "/metrics"
```

### Configuration Flags

Both `blockfetcher` and `consumerindexer` support:

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--metrics-host` | `METRICS_HOST` | `""` (all interfaces) | Host to bind metrics server |
| `--metrics-port` | `METRICS_PORT` | `9090` | Port for metrics server |
| `--chain-id` | `CHAIN_ID` | `""` | Chain identifier for metrics labels |

### Chain Label for Multi-Instance Filtering

When running multiple indexer instances (e.g., different chains), set `--chain-id` to add a constant `chain` label to all metrics:

```bash
# Blockfetcher (chain-id is required, used for both data and metrics)
blockfetcher run --chain-id 43114 ...

# Consumerindexer (chain-id is optional, for metrics labeling only)
consumerindexer run --chain-id 43114 ...
```

This enables Grafana queries filtered by chain:

```promql
# Processing rate for C-Chain mainnet only
rate(indexer_blocks_processed_total{chain="43114"}[5m])

# Compare error rates across chains
sum by (chain) (rate(indexer_errors_total[5m]))
```

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
