package worker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
	"github.com/ava-labs/coreth/rpc"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	evmclient "github.com/ava-labs/coreth/plugin/evm/customethclient"
)

type rpcRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      interface{}   `json:"id"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
	ID      interface{}     `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// testRPCServerWithBlock returns a server that responds to:
// - eth_getBlockByNumber with a static, minimally valid block (empty txs)
// - eth_getTransactionReceipt with defaults (rarely used here)
func testRPCServerWithBlock(t *testing.T, block map[string]interface{}, rpcErr *rpcError) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		var req rpcRequest
		_ = json.Unmarshal(buf.Bytes(), &req)
		switch req.Method {
		case "eth_getBlockByNumber":
			w.Header().Set("Content-Type", "application/json")
			res := rpcResponse{JSONRPC: "2.0", ID: req.ID}
			if rpcErr != nil {
				res.Error = rpcErr
			} else {
				res.Result = mustJSONMarshal(block)
			}
			_ = json.NewEncoder(w).Encode(res)
		case "eth_getTransactionReceipt":
			// Not used in success path with empty txs; return a default if called
			def := map[string]interface{}{
				"status":            "0x1",
				"gasUsed":           "0x0",
				"cumulativeGasUsed": "0x0",
				"contractAddress":   "0x0000000000000000000000000000000000000000",
				"logs":              []interface{}{},
				"blockHash":         "0x" + strings.Repeat("0", 64),
				"blockNumber":       "0x1",
				"transactionHash":   "0x" + strings.Repeat("1", 64),
				"transactionIndex":  "0x0",
				"logsBloom":         "0x" + strings.Repeat("0", 512),
				"effectiveGasPrice": "0x0",
				"type":              "0x2",
			}
			w.Header().Set("Content-Type", "application/json")
			res := rpcResponse{
				JSONRPC: "2.0",
				Result:  mustJSONMarshal(def),
				ID:      req.ID,
			}
			_ = json.NewEncoder(w).Encode(res)
		default:
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = w.Write([]byte(`{"error":"method not implemented in test server"}`))
		}
	}))
}

func mustJSONMarshal(v interface{}) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func newTestWorker(t *testing.T, serverURL string) *CorethWorker {
	t.Helper()
	c, err := rpc.Dial(serverURL)
	if err != nil {
		require.Fail(t, "failed to dial test rpc server", err)
	}
	return &CorethWorker{
		client:         evmclient.New(c),
		log:            zap.NewNop().Sugar(),
		producer:       nil,
		topic:          "",
		receiptTimeout: 10 * time.Second,
	}
}

// testRPCServerForBlockReceipts creates a JSON-RPC server that responds to
// eth_getBlockReceipts with a configurable list of receipts or error.
// If delay is non-zero, the handler will sleep for that duration before responding.
func testRPCServerForBlockReceipts(t *testing.T, receipts []map[string]interface{}, rpcErr *rpcError, delay time.Duration) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		var req rpcRequest
		_ = json.Unmarshal(buf.Bytes(), &req)
		if req.Method != "eth_getBlockReceipts" {
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = w.Write([]byte(`{"error":"method not implemented in test server"}`))
			return
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Content-Type", "application/json")
		resp := rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
		}
		if rpcErr != nil {
			resp.Error = rpcErr
		} else {
			result, _ := json.Marshal(receipts)
			resp.Result = result
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

func TestGetBlock_BlockFetchError(t *testing.T) {
	server := testRPCServerWithBlock(t, nil, &rpcError{Code: -32000, Message: "block not found"})
	defer server.Close()

	log := zap.NewNop().Sugar()
	ctx := t.Context()
	w, err := NewCorethWorker(ctx, server.URL, nil, "", 43114, "C", log, nil, 10*time.Second)
	if err != nil {
		require.Fail(t, "failed to create worker", err)
	}
	_, err = w.GetBlock(ctx, 1)
	if err == nil {
		require.Fail(t, "expected error, got nil")
	}
}

func TestFetchBlockReceipts_SetsReceipts(t *testing.T) {
	// Prepare two minimal receipts with different fields
	receipts := []map[string]interface{}{
		{
			"status":            "0x1",
			"gasUsed":           "0x5208", // 21000
			"cumulativeGasUsed": "0x5208",
			"contractAddress":   "0x0000000000000000000000000000000000000001",
			"logs":              []interface{}{},
			"type":              "0x2",
			"transactionHash":   "0x" + strings.Repeat("1", 64),
			"transactionIndex":  "0x0",
			"blockHash":         "0x" + strings.Repeat("a", 64),
			"blockNumber":       "0x1",
			"logsBloom":         "0x" + strings.Repeat("0", 512),
		},
		{
			"status":            "0x0",
			"gasUsed":           "0x0",
			"cumulativeGasUsed": "0x0",
			"contractAddress":   "0x0000000000000000000000000000000000000002",
			"logs":              []interface{}{},
			"type":              "0x2",
			"transactionHash":   "0x" + strings.Repeat("2", 64),
			"transactionIndex":  "0x1",
			"blockHash":         "0x" + strings.Repeat("b", 64),
			"blockNumber":       "0x1",
			"logsBloom":         "0x" + strings.Repeat("0", 512),
		},
	}
	server := testRPCServerForBlockReceipts(t, receipts, nil, 0)
	defer server.Close()

	w := newTestWorker(t, server.URL)
	// Ensure a small timeout so the test is fast if something goes wrong
	w.receiptTimeout = 2 * time.Second

	// Prepare transactions slice with matching length
	txs := []*messages.CorethTransaction{
		{Hash: "0x" + strings.Repeat("1", 64)},
		{Hash: "0x" + strings.Repeat("2", 64)},
	}
	ctx := t.Context()
	err := w.FetchBlockReceipts(ctx, txs, 1)
	require.NoError(t, err)
	require.Len(t, txs, 2)
	require.NotNil(t, txs[0].Receipt)
	require.NotNil(t, txs[1].Receipt)

	require.Equal(t, uint64(1), txs[0].Receipt.Status)
	require.Equal(t, uint64(0), txs[1].Receipt.Status)

	require.Equal(t, "0x0000000000000000000000000000000000000001", txs[0].Receipt.ContractAddress.Hex())
	require.Equal(t, "0x0000000000000000000000000000000000000002", txs[1].Receipt.ContractAddress.Hex())

	// 0x5208 == 21000
	require.Equal(t, uint64(21000), txs[0].Receipt.GasUsed)
	require.Equal(t, uint64(0), txs[1].Receipt.GasUsed)
}

func TestFetchBlockReceipts_ErrorFromRPC(t *testing.T) {
	server := testRPCServerForBlockReceipts(t, nil, &rpcError{Code: -32000, Message: "bad block"}, 0)
	defer server.Close()

	w := newTestWorker(t, server.URL)
	w.receiptTimeout = 2 * time.Second

	txs := []*messages.CorethTransaction{
		{Hash: "0x" + strings.Repeat("1", 64)},
	}
	ctx := t.Context()
	err := w.FetchBlockReceipts(ctx, txs, 123)
	require.Contains(t, err.Error(), "fetch block receipts failed for block")
}

func TestFetchBlockReceipts_Timeout(t *testing.T) {
	// Server delays longer than receiptTimeout; expect deadline exceeded.
	delayMs := 200
	server := testRPCServerForBlockReceipts(t, []map[string]interface{}{
		{"status": "0x1", "gasUsed": "0x0", "logs": []interface{}{}},
	}, nil, time.Duration(delayMs)*time.Millisecond)
	defer server.Close()

	w := newTestWorker(t, server.URL)
	w.receiptTimeout = 50 * time.Millisecond

	txs := []*messages.CorethTransaction{{Hash: "0x" + strings.Repeat("1", 64)}}
	ctx := t.Context()
	err := w.FetchBlockReceipts(ctx, txs, 1)
	// Error message might be wrapped; check it mentions timeout/deadline or contains our method
	require.True(t, strings.Contains(strings.ToLower(err.Error()), "deadline") ||
		strings.Contains(strings.ToLower(err.Error()), "timeout") ||
		strings.Contains(err.Error(), strconv.Itoa(1)))
}

func TestFetchBlockReceipts_MetricsSuccess(t *testing.T) {
	receipts := []map[string]interface{}{
		{
			"status":            "0x1",
			"gasUsed":           "0x5208", // 21000
			"cumulativeGasUsed": "0x5208",
			"contractAddress":   "0x0000000000000000000000000000000000000001",
			"logs": []interface{}{
				map[string]interface{}{
					"address":          "0x0000000000000000000000000000000000000001",
					"topics":           []interface{}{"0x" + strings.Repeat("0", 64)},
					"data":             "0x01",
					"blockNumber":      "0x1",
					"transactionHash":  "0x" + strings.Repeat("1", 64),
					"transactionIndex": "0x0",
					"blockHash":        "0x" + strings.Repeat("a", 64),
					"logIndex":         "0x0",
					"removed":          false,
				},
				map[string]interface{}{
					"address":          "0x0000000000000000000000000000000000000001",
					"topics":           []interface{}{"0x" + strings.Repeat("0", 64)},
					"data":             "0x02",
					"blockNumber":      "0x1",
					"transactionHash":  "0x" + strings.Repeat("1", 64),
					"transactionIndex": "0x0",
					"blockHash":        "0x" + strings.Repeat("a", 64),
					"logIndex":         "0x1",
					"removed":          false,
				},
			},
			"type":             "0x2",
			"transactionHash":  "0x" + strings.Repeat("1", 64),
			"transactionIndex": "0x0",
			"blockHash":        "0x" + strings.Repeat("a", 64),
			"blockNumber":      "0x1",
			"logsBloom":        "0x" + strings.Repeat("0", 512),
		},
		{
			"status":            "0x1",
			"gasUsed":           "0x5208",
			"cumulativeGasUsed": "0x5208",
			"contractAddress":   "0x0000000000000000000000000000000000000002",
			"logs":              []interface{}{},
			"type":              "0x2",
			"transactionHash":   "0x" + strings.Repeat("2", 64),
			"transactionIndex":  "0x1",
			"blockHash":         "0x" + strings.Repeat("b", 64),
			"blockNumber":       "0x1",
			"logsBloom":         "0x" + strings.Repeat("0", 512),
		},
	}
	server := testRPCServerForBlockReceipts(t, receipts, nil, 0)
	defer server.Close()

	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	require.NoError(t, err)

	w := newTestWorker(t, server.URL)
	w.metrics = m
	w.receiptTimeout = 2 * time.Second

	txs := []*messages.CorethTransaction{
		{Hash: "0x" + strings.Repeat("1", 64)},
		{Hash: "0x" + strings.Repeat("2", 64)},
	}
	ctx := t.Context()
	err = w.FetchBlockReceipts(ctx, txs, 1)
	require.NoError(t, err)

	require.Equal(t, 0.0, getGaugeValue(t, reg, "indexer_receipts_fetches_in_flight", nil))
	require.Equal(t, 1.0, getCounterValue(t, reg, "indexer_receipts_fetched_total", map[string]string{"status": "success"}))
	require.Equal(t, uint64(1), getHistogramCount(t, reg, "indexer_receipts_fetch_duration_seconds", nil))
	require.Equal(t, 2.0, getCounterValue(t, reg, "indexer_logs_fetched_total", nil))
}

func TestFetchBlockReceipts_MetricsError(t *testing.T) {
	server := testRPCServerForBlockReceipts(t, nil, &rpcError{Code: -32000, Message: "bad block"}, 0)
	defer server.Close()

	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	require.NoError(t, err)

	w := newTestWorker(t, server.URL)
	w.metrics = m
	w.receiptTimeout = 2 * time.Second

	txs := []*messages.CorethTransaction{
		{Hash: "0x" + strings.Repeat("1", 64)},
	}
	ctx := t.Context()
	err = w.FetchBlockReceipts(ctx, txs, 123)
	require.Error(t, err)

	require.Equal(t, 0.0, getGaugeValue(t, reg, "indexer_receipts_fetches_in_flight", nil))
	require.Equal(t, 1.0, getCounterValue(t, reg, "indexer_receipts_fetched_total", map[string]string{"status": "error"}))
	require.Equal(t, uint64(1), getHistogramCount(t, reg, "indexer_receipts_fetch_duration_seconds", nil))
	require.Equal(t, 0.0, getCounterValue(t, reg, "indexer_logs_fetched_total", nil))
}

func TestFetchBlockReceipts_CountMismatch(t *testing.T) {
	// Server returns 1 receipt but we have 2 transactions - this is a data integrity error
	receipts := []map[string]interface{}{
		{
			"status":            "0x1",
			"gasUsed":           "0x5208",
			"cumulativeGasUsed": "0x5208",
			"contractAddress":   "0x0000000000000000000000000000000000000001",
			"logs":              []interface{}{},
			"type":              "0x2",
			"transactionHash":   "0x" + strings.Repeat("1", 64),
			"transactionIndex":  "0x0",
			"blockHash":         "0x" + strings.Repeat("a", 64),
			"blockNumber":       "0x1",
			"logsBloom":         "0x" + strings.Repeat("0", 512),
		},
	}
	server := testRPCServerForBlockReceipts(t, receipts, nil, 0)
	defer server.Close()

	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	require.NoError(t, err)

	w := newTestWorker(t, server.URL)
	w.metrics = m
	w.receiptTimeout = 2 * time.Second

	// 2 transactions but only 1 receipt returned
	txs := []*messages.CorethTransaction{
		{Hash: "0x" + strings.Repeat("1", 64)},
		{Hash: "0x" + strings.Repeat("2", 64)},
	}
	ctx := t.Context()
	err = w.FetchBlockReceipts(ctx, txs, 1)

	// Should fail with count mismatch error
	require.Error(t, err)
	require.Contains(t, err.Error(), "receipt count mismatch")
	require.Contains(t, err.Error(), "got 1 receipts")
	require.Contains(t, err.Error(), "expected 2 transactions")

	// Verify receipts were NOT assigned (data integrity preserved)
	require.Nil(t, txs[0].Receipt, "receipt should not be assigned on mismatch")
	require.Nil(t, txs[1].Receipt, "receipt should not be assigned on mismatch")

	// Verify error metrics were recorded
	require.Equal(t, 1.0, getCounterValue(t, reg, "indexer_receipts_fetched_total", map[string]string{"status": "error"}))
	require.Equal(t, 0.0, getCounterValue(t, reg, "indexer_logs_fetched_total", nil))
}

func getCounterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	metric := findMetric(t, reg, name, labels)
	if metric == nil || metric.Counter == nil {
		return 0
	}
	return metric.Counter.GetValue()
}

func getGaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	metric := findMetric(t, reg, name, labels)
	if metric == nil || metric.Gauge == nil {
		return 0
	}
	return metric.Gauge.GetValue()
}

func getHistogramCount(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) uint64 {
	t.Helper()
	metric := findMetric(t, reg, name, labels)
	if metric == nil || metric.Histogram == nil {
		return 0
	}
	return metric.Histogram.GetSampleCount()
}

func findMetric(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			if labelsMatch(m, labels) {
				return m
			}
		}
	}
	return nil
}

func labelsMatch(metric *dto.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return len(metric.Label) == 0
	}
	if len(metric.Label) != len(labels) {
		return false
	}
	for _, l := range metric.Label {
		if val, ok := labels[l.GetName()]; !ok || val != l.GetValue() {
			return false
		}
	}
	return true
}
