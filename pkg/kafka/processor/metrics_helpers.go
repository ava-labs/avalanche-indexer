package processor

import (
	"time"

	metricslib "github.com/ava-labs/avalanche-indexer/pkg/metrics"
)

// recordClickHouseWrite records a ClickHouse write duration and status for a table.
func recordClickHouseWrite(m *metricslib.Metrics, table string, err error, writeStart time.Time) {
	m.RecordClickHouseWrite(table, err, time.Since(writeStart).Seconds())
}
