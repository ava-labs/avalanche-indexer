//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/checkpoint"
	"github.com/ava-labs/avalanche-indexer/pkg/utils"
	"github.com/stretchr/testify/require"
)

// verifyCheckpointFromMaxProcessed finds the max processed height and checks checkpoint lowest==max+1.
func verifyCheckpointFromMaxProcessed(t *testing.T, ctx context.Context, repo checkpoint.Repository, chainID uint64, kafkaByNumber map[uint64][]byte) {
	t.Helper()
	if len(kafkaByNumber) == 0 {
		return
	}
	var max uint64
	for n := range kafkaByNumber {
		if n > max {
			max = n
		}
	}
	verifyCheckpointLowestCorrect(t, ctx, repo, chainID, max+1)
}

// verifyCheckpointLowestCorrect polls ClickHouse checkpoint for the chain until lowest>=expected or timeout.
func verifyCheckpointLowestCorrect(t *testing.T, ctx context.Context, repo checkpoint.Repository, chainID uint64, expected uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, err := repo.ReadCheckpoint(ctx, chainID)
		if err == nil && s != nil && s.Lowest >= expected {
			return
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "read checkpoint failed")
			require.NotNil(t, s, "checkpoint nil")
			require.GreaterOrEqual(t, expected, s.Lowest, "checkpoint lowest mismatch")
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func getEnvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvUint64(key string, def uint64) uint64 {
	if v := os.Getenv(key); v != "" {
		var out uint64
		_, _ = fmt.Sscanf(v, "%d", &out)
		if out != 0 {
			return out
		}
	}
	return def
}

func queryCount(t *testing.T, ctx context.Context, ch clickhouse.Client, query string, args ...interface{}) uint64 {
	t.Helper()
	conn := ch.Conn()
	var cnt uint64
	require.NoError(t, conn.QueryRow(ctx, query, args...).Scan(&cnt))
	return cnt
}

func queryFixedString(t *testing.T, ctx context.Context, ch clickhouse.Client, query string, args ...interface{}) string {
	t.Helper()
	conn := ch.Conn()
	var s string
	require.NoError(t, conn.QueryRow(ctx, query, args...).Scan(&s))
	return s
}

func queryString(t *testing.T, ctx context.Context, ch clickhouse.Client, query string, args ...interface{}) string {
	t.Helper()
	conn := ch.Conn()
	var s string
	require.NoError(t, conn.QueryRow(ctx, query, args...).Scan(&s))
	return s
}

func mustHexToFixed32(t *testing.T, hex string) string {
	t.Helper()
	b, err := utils.HexToBytes32(hex)
	require.NoError(t, err)
	return string(b[:])
}
