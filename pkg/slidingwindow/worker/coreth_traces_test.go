package worker

import (
	"bytes"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ava-labs/coreth/rpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
)

func testRPCServerForTraces(t *testing.T, traces []json.RawMessage, tracesErr *rpcError, delay time.Duration) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		var req rpcRequest
		_ = json.Unmarshal(buf.Bytes(), &req)

		switch req.Method {
		case "debug_traceBlockByNumber":
			if delay > 0 {
				time.Sleep(delay)
			}
			w.Header().Set("Content-Type", "application/json")
			res := rpcResponse{JSONRPC: "2.0", ID: req.ID}
			if tracesErr != nil {
				res.Error = tracesErr
			} else {
				res.Result = mustJSONMarshal(traces)
			}
			_ = json.NewEncoder(w).Encode(res)
		default:
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = w.Write([]byte(`{"error":"method not implemented in test server"}`))
		}
	}))
}

func newTestTracesWorker(t *testing.T, serverURL string) *CorethTracesWorker {
	t.Helper()
	c, err := rpc.Dial(serverURL)
	if err != nil {
		require.Fail(t, "failed to dial test rpc server", err)
	}
	bcID := "test-blockchain-id-1"
	return &CorethTracesWorker{
		rpc:          c,
		log:          zap.NewNop().Sugar(),
		producer:     nil,
		topic:        "",
		evmChainID:   big.NewInt(43114),
		blockchainID: &bcID,
		traceTimeout: 10 * time.Second,
	}
}

func TestCorethFetchBlockTraces_Success(t *testing.T) {
	traces := []json.RawMessage{
		json.RawMessage(`{"type":"CALL","from":"0x1111111111111111111111111111111111111111","to":"0x2222222222222222222222222222222222222222","value":"0x0","gas":"0x5208","gasUsed":"0x5208","input":"0x","output":"0x"}`),
		json.RawMessage(`{"type":"CREATE","from":"0x3333333333333333333333333333333333333333","value":"0x0","gas":"0x10000","gasUsed":"0x10000","input":"0x6060","output":"0x"}`),
	}

	server := testRPCServerForTraces(t, traces, nil, 0)
	defer server.Close()

	w := newTestTracesWorker(t, server.URL)
	w.traceTimeout = 2 * time.Second

	ctx := t.Context()
	fetchedTraces, err := w.FetchBlockTraces(ctx, 1)
	require.NoError(t, err)
	require.Len(t, fetchedTraces, 2)
}

func TestCorethFetchBlockTraces_FetchError(t *testing.T) {
	server := testRPCServerForTraces(t, nil, &rpcError{Code: -32000, Message: "trace failed"}, 0)
	defer server.Close()

	w := newTestTracesWorker(t, server.URL)
	w.traceTimeout = 2 * time.Second

	ctx := t.Context()
	_, err := w.FetchBlockTraces(ctx, 1)
	require.ErrorIs(t, err, ErrTracesFetchFailed)
	require.Contains(t, err.Error(), "for block 1")
}

func TestCorethFetchBlockTraces_Timeout(t *testing.T) {
	traces := []json.RawMessage{
		json.RawMessage(`{"type":"CALL"}`),
	}

	server := testRPCServerForTraces(t, traces, nil, 200*time.Millisecond)
	defer server.Close()

	w := newTestTracesWorker(t, server.URL)
	w.traceTimeout = 50 * time.Millisecond

	ctx := t.Context()
	_, err := w.FetchBlockTraces(ctx, 1)
	require.ErrorIs(t, err, ErrTracesFetchFailed)
	require.Contains(t, strings.ToLower(err.Error()), "deadline")
}

func TestCorethFetchBlockTraces_MetricsSuccess(t *testing.T) {
	traces := []json.RawMessage{
		json.RawMessage(`{"type":"CALL","from":"0x1111111111111111111111111111111111111111","to":"0x2222222222222222222222222222222222222222"}`),
	}

	server := testRPCServerForTraces(t, traces, nil, 0)
	defer server.Close()

	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	require.NoError(t, err)

	w := newTestTracesWorker(t, server.URL)
	w.metrics = m
	w.traceTimeout = 2 * time.Second

	ctx := t.Context()
	fetchedTraces, err := w.FetchBlockTraces(ctx, 1)
	require.NoError(t, err)
	require.Len(t, fetchedTraces, 1)

	require.Equal(t, 0.0, getGaugeValue(t, reg))
	require.Equal(t, 1.0, getCounterValue(t, reg, "indexer_rpc_calls_total", map[string]string{"method": "debug_traceBlockByNumber", "status": "success"}))
	require.Positive(t, uint64(1))
}

func TestCorethFetchBlockTraces_MetricsError(t *testing.T) {
	server := testRPCServerForTraces(t, nil, &rpcError{Code: -32000, Message: "trace failed"}, 0)
	defer server.Close()

	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	require.NoError(t, err)

	w := newTestTracesWorker(t, server.URL)
	w.metrics = m
	w.traceTimeout = 2 * time.Second

	ctx := t.Context()
	_, err = w.FetchBlockTraces(ctx, 1)
	require.ErrorIs(t, err, ErrTracesFetchFailed)

	require.Equal(t, 0.0, getGaugeValue(t, reg))
	require.Equal(t, 1.0, getCounterValue(t, reg, "indexer_rpc_calls_total", map[string]string{"method": "debug_traceBlockByNumber", "status": "error"}))
}

func TestCorethMarshalBlockTrace(t *testing.T) {
	traces := []json.RawMessage{
		json.RawMessage(`{"type":"CALL","from":"0x1111111111111111111111111111111111111111"}`),
	}

	evmChainID := big.NewInt(43114)
	bcID := "test-blockchain-id-2"
	blockTimestamp := uint64(1640000000)

	bytes, err := messages.MarshalEVMBlockTrace(123, blockTimestamp, traces, evmChainID, &bcID)
	require.NoError(t, err)
	require.NotEmpty(t, bytes)

	var result messages.EVMBlockTrace
	err = json.Unmarshal(bytes, &result)
	require.NoError(t, err)
	require.Equal(t, uint64(123), result.BlockNumber)
	require.Equal(t, blockTimestamp, result.BlockTimestamp)
	require.Equal(t, evmChainID, result.EVMChainID)
	require.Equal(t, bcID, *result.BlockchainID)
	require.Len(t, result.Traces, 1)
}

func TestCorethFetchBlockTraces_EmptyTraces(t *testing.T) {
	traces := []json.RawMessage{}

	server := testRPCServerForTraces(t, traces, nil, 0)
	defer server.Close()

	w := newTestTracesWorker(t, server.URL)
	w.traceTimeout = 2 * time.Second

	ctx := t.Context()
	fetchedTraces, err := w.FetchBlockTraces(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, fetchedTraces)
}
