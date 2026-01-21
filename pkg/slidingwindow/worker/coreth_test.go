package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/kafka/messages"
	"github.com/ava-labs/coreth/rpc"
	"github.com/ava-labs/libevm/common"
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

// testRPCServer creates a JSON-RPC server that responds to eth_getTransactionReceipt with
// configurable results or errors keyed by tx hash.
func testRPCServer(t *testing.T, txHashToResp map[string]rpcResponse) *httptest.Server {
	t.Helper()
	var mu sync.RWMutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		var req rpcRequest
		if err := json.Unmarshal(buf.Bytes(), &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, `{"error":"bad request: %v"}`, err)
			return
		}
		switch req.Method {
		case "eth_getTransactionReceipt":
			if len(req.Params) < 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"missing params"}`))
				return
			}
			txHash, _ := req.Params[0].(string)
			mu.RLock()
			resp, ok := txHashToResp[txHash]
			mu.RUnlock()
			if !ok {
				// default: success empty logs, success status, zero address, gasUsed 0x0
				def := map[string]interface{}{
					"status":            "0x1",
					"gasUsed":           "0x0",
					"cumulativeGasUsed": "0x0",
					"contractAddress":   "0x0000000000000000000000000000000000000000",
					"logs":              []interface{}{},
					"blockHash":         "0x" + strings.Repeat("0", 64),
					"blockNumber":       "0x1",
					"transactionHash":   txHash,
					"transactionIndex":  "0x0",
					"logsBloom":         "0x" + strings.Repeat("0", 512),
					"effectiveGasPrice": "0x0",
					"type":              "0x2",
				}
				result, _ := json.Marshal(def)
				resp = rpcResponse{JSONRPC: "2.0", Result: result, ID: req.ID}
			} else {
				// Ensure the ID matches the request
				resp.ID = req.ID
			}
			w.Header().Set("Content-Type", "application/json")
			enc := json.NewEncoder(w)
			_ = enc.Encode(resp)
		default:
			// method not supported in tests
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = w.Write([]byte(`{"error":"method not implemented in test server"}`))
		}
	}))
}

// testRPCServerWithBlock returns a server that responds to:
// - eth_getBlockByNumber with a static, minimally valid block (empty txs)
// - eth_getTransactionReceipt with defaults (rarely used here)
func testRPCServerWithBlock(t *testing.T, block map[string]interface{}) *httptest.Server {
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
			res := rpcResponse{
				JSONRPC: "2.0",
				Result:  mustJSONMarshal(block),
				ID:      req.ID,
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
		client:                  evmclient.New(c),
		log:                     zap.NewNop().Sugar(),
		producer:                nil,
		topic:                   "",
		receiptConcurrencyLimit: 10,
		receiptTimeout:          10 * time.Second,
	}
}

func TestFetchReceipts_PopulatesTxReceipts(t *testing.T) {
	// Prepare three tx hashes with deterministic responses
	tx1 := "0x399728677b726558e4aa89169771b2b9eebcf66fa4271c0ab29f75c4e0bb7e17"
	tx2 := "0x399728677b726558e4aa89169771b2b9eebcf66fa4271c0ab29f75c4e0bb7e18"
	tx3 := "0x399728677b726558e4aa89169771b2b9eebcf66fa4271c0ab29f75c4e0bb7e19"

	makeReceipt := func(txHash string, contract string) json.RawMessage {
		m := map[string]interface{}{
			"status":            "0x1",
			"gasUsed":           "0x5208", // 21000
			"cumulativeGasUsed": "0x5208",
			"contractAddress":   contract,
			"logs":              []interface{}{},
			"blockHash":         "0x4bde6898d74de6592c3a3158c782a37e8179e1c8568b390597144ba5a1e35d77",
			"blockNumber":       "0x1",
			"transactionHash":   txHash,
			"transactionIndex":  "0x0",
			"logsBloom":         "0x" + strings.Repeat("0", 512),
			"effectiveGasPrice": "0x0",
			"type":              "0x2",
		}
		b, _ := json.Marshal(m)
		return b
	}

	server := testRPCServer(t, map[string]rpcResponse{
		tx1: {JSONRPC: "2.0", Result: makeReceipt(tx1, "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E")},
		tx2: {JSONRPC: "2.0", Result: makeReceipt(tx2, "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E")},
		tx3: {JSONRPC: "2.0", Result: makeReceipt(tx3, "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E")},
	})
	defer server.Close()

	w := newTestWorker(t, server.URL)

	txs := []*messages.CorethTransaction{
		{Hash: tx1},
		{Hash: tx2},
		{Hash: tx3},
	}

	ctx := t.Context()
	if err := w.FetchReceipts(ctx, txs); err != nil {
		require.Fail(t, "FetchReceipts returned error", err)
	}

	for i, tx := range txs {
		if tx.Receipt == nil {
			require.Failf(t, "Receipt", "tx %d receipt is nil", i)
		}
		if tx.Receipt.Status != 1 {
			require.Failf(t, "Status", "tx %d expected status=1, got %d", i, tx.Receipt.Status)
		}
		if tx.Receipt.GasUsed != 21000 {
			require.Failf(t, "GasUsed", "tx %d expected gasUsed=21000, got %d", i, tx.Receipt.GasUsed)
		}
		if tx.Receipt.ContractAddress != common.HexToAddress("0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E") {
			require.Failf(t, "ContractAddress", "tx %d contract address mismatch: got %s", i, tx.Receipt.ContractAddress.Hex())
		}
	}
}

func TestFetchReceipts_PropagatesError(t *testing.T) {
	makeReceipt := func(txHash string) json.RawMessage {
		m := map[string]interface{}{
			"status":            "0x1",
			"gasUsed":           "0x5208",
			"cumulativeGasUsed": "0x5208",
			"contractAddress":   "0x0000000000000000000000000000000000000001",
			"logs":              []interface{}{},
			"blockHash":         "0x0000000000000000000000000000000000000000000000000000000000000000",
			"blockNumber":       "0x1",
			"transactionHash":   txHash,
			"transactionIndex":  "0x0",
			"logsBloom":         "0x" + strings.Repeat("0", 512),
			"effectiveGasPrice": "0x0",
			"type":              "0x2",
		}
		b, _ := json.Marshal(m)
		return b
	}

	okTx := "0x1111111111111111111111111111111111111111111111111111111111111111"
	errTx := "0x2222222222222222222222222222222222222222222222222222222222222222"

	server := testRPCServer(t, map[string]rpcResponse{
		okTx:  {JSONRPC: "2.0", Result: makeReceipt(okTx)},
		errTx: {JSONRPC: "2.0", Error: &rpcError{Code: -32000, Message: "tx not found"}},
	})
	defer server.Close()

	w := newTestWorker(t, server.URL)
	txs := []*messages.CorethTransaction{
		{Hash: okTx},
		{Hash: errTx},
	}

	err := w.FetchReceipts(t.Context(), txs)
	if err == nil {
		require.Fail(t, "expected error, got nil")
	}
	// The successful one may or may not be set depending on scheduling; ensure it can be set without panics.
	if txs[0].Receipt != nil && txs[0].Receipt.Status != 1 {
		require.Fail(t, "unexpected receipt content for ok tx")
	}
}

func TestTryFetchReceipt_ReturnsParsedReceipt(t *testing.T) {
	tx := "0x399728677b726558e4aa89169771b2b9eebcf66fa4271c0ab29f75c4e0bb7e17"
	m := map[string]interface{}{
		"status":            "0x1",
		"gasUsed":           "0x5208",
		"cumulativeGasUsed": "0x5208",
		"contractAddress":   "0x0000000000000000000000000000000000000002",
		"logs":              []interface{}{},
		"blockHash":         "0x0000000000000000000000000000000000000000000000000000000000000000",
		"blockNumber":       "0x1",
		"transactionHash":   tx,
		"transactionIndex":  "0x0",
		"logsBloom":         "0x" + strings.Repeat("0", 512),
		"effectiveGasPrice": "0x0",
		"type":              "0x2",
	}
	result, _ := json.Marshal(m)
	server := testRPCServer(t, map[string]rpcResponse{
		tx: {JSONRPC: "2.0", Result: result},
	})
	defer server.Close()

	w := newTestWorker(t, server.URL)

	rcpt, err := w.tryFetchReceipt(t.Context(), tx)
	if err != nil {
		require.Fail(t, "tryFetchReceipt returned error", err)
	}
	if rcpt.Status != 1 {
		require.Failf(t, "Status", "expected status=1, got %d", rcpt.Status)
	}
	if rcpt.GasUsed != 21000 {
		require.Failf(t, "GasUsed", "expected gasUsed=21000, got %d", rcpt.GasUsed)
	}
	if rcpt.ContractAddress != common.HexToAddress("0x0000000000000000000000000000000000000002") {
		require.Failf(t, "ContractAddress", "contract address mismatch: got %s", rcpt.ContractAddress.Hex())
	}
}

func TestGetBlock_Success_NoTransactions(t *testing.T) {
	const emptyTrieRootHash = "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421" // empty trie root

	block := map[string]interface{}{
		"number":           "0x1",
		"hash":             "0x" + strings.Repeat("1", 64),
		"parentHash":       "0x" + strings.Repeat("0", 64),
		"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		"transactionsRoot": emptyTrieRootHash,
		"receiptsRoot":     emptyTrieRootHash,
		"logsBloom":        "0x" + strings.Repeat("0", 512),
		"stateRoot":        "0x" + strings.Repeat("0", 64),
		"miner":            "0x" + strings.Repeat("0", 40),
		"difficulty":       "0x1",
		"size":             "0x1",
		"extraData":        "0x",
		"extDataHash":      "0x" + strings.Repeat("0", 64),
		"gasLimit":         "0x5208",
		"gasUsed":          "0x0",
		"baseFeePerGas":    "0x0",
		"timestamp":        "0x1",
		"nonce":            "0x0000000000000000",
		"transactions":     []interface{}{},
		"uncles":           []interface{}{},
	}
	server := testRPCServerWithBlock(t, block)
	defer server.Close()

	log := zap.NewNop().Sugar()
	ctx := t.Context()
	w, err := NewCorethWorker(ctx, server.URL, nil, "", 43114, "C", log, nil, 10, 10*time.Second)
	if err != nil {
		require.Fail(t, "failed to create worker", err)
	}
	cb, err := w.GetBlock(ctx, 1)
	if err != nil {
		require.Fail(t, "GetBlock returned error", err)
	}
	if cb == nil || cb.Number == nil || cb.Number.Uint64() != 1 {
		require.Fail(t, "unexpected block number")
	}
	if cb.Hash == "" {
		require.Fail(t, "expected non-empty block hash")
	}
	if len(cb.Transactions) != 0 {
		require.Failf(t, "Transactions", "expected 0 transactions, got %d", len(cb.Transactions))
	}
}

func TestGetBlock_BlockFetchError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		var req rpcRequest
		_ = json.Unmarshal(buf.Bytes(), &req)
		switch req.Method {
		case "eth_getBlockByNumber":
			res := rpcResponse{
				JSONRPC: "2.0",
				Error:   &rpcError{Code: -32000, Message: "block not found"},
				ID:      req.ID,
			}
			_ = json.NewEncoder(w).Encode(res)
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	}))
	defer server.Close()

	log := zap.NewNop().Sugar()
	ctx := t.Context()
	w, err := NewCorethWorker(ctx, server.URL, nil, "", 43114, "C", log, nil, 10, 10*time.Second)
	if err != nil {
		require.Fail(t, "failed to create worker", err)
	}
	_, err = w.GetBlock(ctx, 1)
	if err == nil {
		require.Fail(t, "expected error, got nil")
	}
}
