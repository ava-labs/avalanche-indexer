package processor

import (
	"encoding/json"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// GetTracesForTransaction Tests
// ============================================================================

func TestGetTracesForTransaction_Success(t *testing.T) {
	t.Parallel()

	trace := map[string]interface{}{
		"txHash": "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
		"result": map[string]interface{}{
			"type":    "CALL",
			"from":    "0x4142434445464748494a4b4c4d4e4f5051525354",
			"to":      "0x55565758595a5b5c5d5e5f6061626364656667",
			"value":   "0xde0b6b3a7640000",
			"gas":     "0x5208",
			"gasUsed": "0x5208",
			"input":   "0x",
			"output":  "0x",
		},
	}

	traceBytes, err := json.Marshal(trace)
	require.NoError(t, err)

	txHash, traces, err := GetTracesForTransaction(traceBytes)
	require.NoError(t, err)
	require.NotNil(t, traces)

	assert.Equal(t, "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273", txHash)
	assert.Len(t, traces, 1)
	assert.Equal(t, "CALL", traces[0].Type)
	assert.Equal(t, "call_0", traces[0].CallIndex)
}

func TestGetTracesForTransaction_MissingTxHash(t *testing.T) {
	t.Parallel()

	trace := map[string]interface{}{
		"result": map[string]interface{}{
			"type": "CALL",
			"from": "0x4142434445464748494a4b4c4d4e4f5051525354",
			"to":   "0x55565758595a5b5c5d5e5f6061626364656667",
		},
	}

	traceBytes, err := json.Marshal(trace)
	require.NoError(t, err)

	_, _, err = GetTracesForTransaction(traceBytes)
	require.ErrorIs(t, err, ErrMissingTxHash)
}

func TestGetTracesForTransaction_EmptyTxHash(t *testing.T) {
	t.Parallel()

	trace := map[string]interface{}{
		"txHash": "",
		"result": map[string]interface{}{
			"type": "CALL",
			"from": "0x4142434445464748494a4b4c4d4e4f5051525354",
			"to":   "0x55565758595a5b5c5d5e5f6061626364656667",
		},
	}

	traceBytes, err := json.Marshal(trace)
	require.NoError(t, err)

	_, _, err = GetTracesForTransaction(traceBytes)
	require.ErrorIs(t, err, ErrMissingTxHash)
}

func TestGetTracesForTransaction_InvalidTxHashType(t *testing.T) {
	t.Parallel()

	trace := map[string]interface{}{
		"txHash": 12345, // Not a string
		"result": map[string]interface{}{
			"type": "CALL",
			"from": "0x4142434445464748494a4b4c4d4e4f5051525354",
			"to":   "0x55565758595a5b5c5d5e5f6061626364656667",
		},
	}

	traceBytes, err := json.Marshal(trace)
	require.NoError(t, err)

	_, _, err = GetTracesForTransaction(traceBytes)
	require.ErrorIs(t, err, ErrTraceUnmarshal)
}

func TestGetTracesForTransaction_MissingResult(t *testing.T) {
	t.Parallel()

	trace := map[string]interface{}{
		"txHash": "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273",
	}

	traceBytes, err := json.Marshal(trace)
	require.NoError(t, err)

	txHash, traces, err := GetTracesForTransaction(traceBytes)
	require.NoError(t, err)
	assert.Equal(t, "0x55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70717273", txHash)
	// Result will be zero-value CallFrame, which produces a single root call
	assert.Len(t, traces, 1)
}

func TestGetTracesForTransaction_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, _, err := GetTracesForTransaction([]byte(`{invalid json}`))
	require.ErrorIs(t, err, ErrTraceUnmarshal)
}

// ============================================================================
// TransformTrace Tests
// ============================================================================

func TestCallFrame_TransformTrace_SingleCall(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:    "CALL",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:   "0xde0b6b3a7640000",
		Gas:     "0x5208",
		GasUsed: "0x5208",
		Input:   "0x",
		Output:  "0x",
	}

	traces := cf.TransformTrace("call", 0)
	require.Len(t, traces, 1)

	assert.Equal(t, "call_0", traces[0].CallIndex)
	assert.Equal(t, "CALL", traces[0].Type)
	assert.Equal(t, common.HexToAddress("0x4142434445464748494a4b4c4d4e4f5051525354"), traces[0].From)
	assert.Equal(t, common.HexToAddress("0x55565758595a5b5c5d5e5f6061626364656667"), traces[0].To)
	assert.Equal(t, "1000000000000000000", traces[0].Value)
	assert.Equal(t, "21000", traces[0].Gas)
	assert.Equal(t, "21000", traces[0].GasUsed)
	assert.False(t, traces[0].Revert)
}

func TestCallFrame_TransformTrace_WithChildren(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:    "CALL",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:   "0x0",
		Gas:     "0x10000",
		GasUsed: "0x5000",
		Input:   "0xabcd",
		Output:  "0x1234",
		Calls: []CallFrame{
			{
				Type:    "CALL",
				From:    "0x55565758595a5b5c5d5e5f6061626364656667",
				To:      "0x6162636465666768696a6b6c6d6e6f7071727374",
				Value:   "0x100",
				Gas:     "0x1000",
				GasUsed: "0x500",
				Input:   "0xef",
				Output:  "0xfeed",
			},
			{
				Type:    "DELEGATECALL",
				From:    "0x55565758595a5b5c5d5e5f6061626364656667",
				To:      "0x7172737475767778797a7b7c7d7e7f8081828384",
				Value:   "0x0",
				Gas:     "0x2000",
				GasUsed: "0x1000",
				Input:   "0xaa",
				Output:  "0xbb",
			},
		},
	}

	traces := cf.TransformTrace("call", 0)
	require.Len(t, traces, 3)

	// Root call
	assert.Equal(t, "call_0", traces[0].CallIndex)
	assert.Equal(t, "CALL", traces[0].Type)

	// First child
	assert.Equal(t, "call_0_0", traces[1].CallIndex)
	assert.Equal(t, "CALL", traces[1].Type)
	assert.Equal(t, "256", traces[1].Value)

	// Second child
	assert.Equal(t, "call_0_1", traces[2].CallIndex)
	assert.Equal(t, "DELEGATECALL", traces[2].Type)
}

func TestCallFrame_TransformTrace_RevertPropagation(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:    "CALL",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:   "0x0",
		Gas:     "0x10000",
		GasUsed: "0x5000",
		Input:   "0xabcd",
		Output:  "0x",
		Error:   "execution reverted",
		Calls: []CallFrame{
			{
				Type:    "CALL",
				From:    "0x55565758595a5b5c5d5e5f6061626364656667",
				To:      "0x6162636465666768696a6b6c6d6e6f7071727374",
				Value:   "0x0",
				Gas:     "0x1000",
				GasUsed: "0x500",
				Input:   "0xef",
				Output:  "0x",
			},
		},
	}

	traces := cf.TransformTrace("call", 0)
	require.Len(t, traces, 2)

	// Root should be reverted
	assert.True(t, traces[0].Revert)
	assert.Equal(t, "execution reverted", traces[0].Error)

	// Child should also be reverted and inherit parent error
	assert.True(t, traces[1].Revert)
	assert.Equal(t, "execution reverted", traces[1].Error)
}

func TestCallFrame_TransformTrace_ChildWithOwnError(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:    "CALL",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:   "0x0",
		Gas:     "0x10000",
		GasUsed: "0x5000",
		Input:   "0xabcd",
		Output:  "0x",
		Error:   "parent error",
		Calls: []CallFrame{
			{
				Type:    "CALL",
				From:    "0x55565758595a5b5c5d5e5f6061626364656667",
				To:      "0x6162636465666768696a6b6c6d6e6f7071727374",
				Value:   "0x0",
				Gas:     "0x1000",
				GasUsed: "0x500",
				Input:   "0xef",
				Output:  "0x",
				Error:   "child error",
			},
		},
	}

	traces := cf.TransformTrace("call", 0)
	require.Len(t, traces, 2)

	// Parent error
	assert.Equal(t, "parent error", traces[0].Error)

	// Child keeps its own error
	assert.Equal(t, "child error", traces[1].Error)
}

func TestCallFrame_TransformTrace_NestedCalls(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:    "CALL",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:   "0x0",
		Gas:     "0x10000",
		GasUsed: "0x5000",
		Input:   "0xabcd",
		Output:  "0x1234",
		Calls: []CallFrame{
			{
				Type:    "CALL",
				From:    "0x55565758595a5b5c5d5e5f6061626364656667",
				To:      "0x6162636465666768696a6b6c6d6e6f7071727374",
				Value:   "0x0",
				Gas:     "0x5000",
				GasUsed: "0x2000",
				Input:   "0xef",
				Output:  "0xfeed",
				Calls: []CallFrame{
					{
						Type:    "STATICCALL",
						From:    "0x6162636465666768696a6b6c6d6e6f7071727374",
						To:      "0x7172737475767778797a7b7c7d7e7f8081828384",
						Value:   "0x0",
						Gas:     "0x1000",
						GasUsed: "0x500",
						Input:   "0xaa",
						Output:  "0xbb",
					},
				},
			},
		},
	}

	traces := cf.TransformTrace("call", 0)
	require.Len(t, traces, 3)

	assert.Equal(t, "call_0", traces[0].CallIndex)
	assert.Equal(t, "call_0_0", traces[1].CallIndex)
	assert.Equal(t, "call_0_0_0", traces[2].CallIndex)
	assert.Equal(t, "STATICCALL", traces[2].Type)
}

// ============================================================================
// TransformCall Tests
// ============================================================================

func TestCallFrame_TransformCall_AllFieldsPopulated(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:         "CALL",
		From:         "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:           "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:        "0xde0b6b3a7640000",
		Gas:          "0x5208",
		GasUsed:      "0x5208",
		Input:        "0xabcd",
		Output:       "0x1234",
		Error:        "execution reverted",
		RevertReason: "insufficient balance",
	}

	call := cf.TransformCall("call_0")
	require.NotNil(t, call)

	assert.Equal(t, "call_0", call.CallIndex)
	assert.Equal(t, "CALL", call.Type)
	assert.Equal(t, common.HexToAddress("0x4142434445464748494a4b4c4d4e4f5051525354"), call.From)
	assert.Equal(t, common.HexToAddress("0x55565758595a5b5c5d5e5f6061626364656667"), call.To)
	assert.Equal(t, "1000000000000000000", call.Value)
	assert.Equal(t, "21000", call.Gas)
	assert.Equal(t, "21000", call.GasUsed)
	assert.True(t, call.Revert)
	assert.Equal(t, "execution reverted", call.Error)
	assert.Equal(t, "insufficient balance", call.RevertReason)
	assert.Equal(t, "0xabcd", call.Input)
	assert.Equal(t, "0x1234", call.Output)
}

func TestCallFrame_TransformCall_EmptyValues(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:    "CALL",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:   "",
		Gas:     "",
		GasUsed: "",
		Input:   "",
		Output:  "",
	}

	call := cf.TransformCall("call_1")
	require.NotNil(t, call)

	// Empty strings should result in default "0" values
	assert.Equal(t, "0", call.Value)
	assert.Equal(t, "0", call.Gas)
	assert.Equal(t, "0", call.GasUsed)
	assert.False(t, call.Revert)
	assert.Empty(t, call.Error)
}

func TestCallFrame_TransformCall_ErrorSetsRevert(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:  "CALL",
		From:  "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:    "0x55565758595a5b5c5d5e5f6061626364656667",
		Value: "0x0",
		Gas:   "0x5208",
		Error: "out of gas",
	}

	call := cf.TransformCall("call_0")
	require.NotNil(t, call)

	assert.True(t, call.Revert)
	assert.Equal(t, "out of gas", call.Error)
}

func TestCallFrame_TransformCall_InvalidHexValue(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:    "CALL",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:   "invalid_hex",
		Gas:     "0x5208",
		GasUsed: "0x5208",
		Input:   "0x",
		Output:  "0x",
	}

	// MustDecodeBig panics on invalid hex, so we expect a panic
	assert.Panics(t, func() {
		cf.TransformCall("call_0")
	}, "Expected panic with invalid hex value")
}

func TestCallFrame_TransformCall_ZeroAddresses(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:    "CREATE",
		From:    "0x0000000000000000000000000000000000000000",
		To:      "0x0000000000000000000000000000000000000000",
		Value:   "0x0",
		Gas:     "0x0",
		GasUsed: "0x0",
	}

	call := cf.TransformCall("call_0")
	require.NotNil(t, call)

	assert.Equal(t, common.HexToAddress("0x0000000000000000000000000000000000000000"), call.From)
	assert.Equal(t, common.HexToAddress("0x0000000000000000000000000000000000000000"), call.To)
}

func TestCallFrame_TransformTrace_ComplexHierarchy(t *testing.T) {
	t.Parallel()

	// Create a complex call hierarchy:
	// call_0
	//   ├─ call_0_0
	//   │   └─ call_0_0_0
	//   └─ call_0_1
	cf := CallFrame{
		Type:    "CALL",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:   "0x0",
		Gas:     "0x100000",
		GasUsed: "0x50000",
		Calls: []CallFrame{
			{
				Type:    "CALL",
				From:    "0x55565758595a5b5c5d5e5f6061626364656667",
				To:      "0x6162636465666768696a6b6c6d6e6f7071727374",
				Value:   "0x0",
				Gas:     "0x10000",
				GasUsed: "0x5000",
				Calls: []CallFrame{
					{
						Type:    "STATICCALL",
						From:    "0x6162636465666768696a6b6c6d6e6f7071727374",
						To:      "0x7172737475767778797a7b7c7d7e7f8081828384",
						Value:   "0x0",
						Gas:     "0x1000",
						GasUsed: "0x500",
					},
				},
			},
			{
				Type:    "DELEGATECALL",
				From:    "0x55565758595a5b5c5d5e5f6061626364656667",
				To:      "0x8182838485868788898a8b8c8d8e8f9091929394",
				Value:   "0x0",
				Gas:     "0x20000",
				GasUsed: "0x10000",
			},
		},
	}

	traces := cf.TransformTrace("call", 0)
	require.Len(t, traces, 4)

	assert.Equal(t, "call_0", traces[0].CallIndex)
	assert.Equal(t, "call_0_0", traces[1].CallIndex)
	assert.Equal(t, "call_0_0_0", traces[2].CallIndex)
	assert.Equal(t, "call_0_1", traces[3].CallIndex)

	assert.Equal(t, "STATICCALL", traces[2].Type)
	assert.Equal(t, "DELEGATECALL", traces[3].Type)
}

func TestCallFrame_TransformTrace_RevertWithRevertReason(t *testing.T) {
	t.Parallel()

	cf := CallFrame{
		Type:         "CALL",
		From:         "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:           "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:        "0x0",
		Gas:          "0x10000",
		GasUsed:      "0x5000",
		Input:        "0xabcd",
		Output:       "0x",
		Error:        "execution reverted",
		RevertReason: "0x08c379a00000000000000000000000000000000000000000000000000000000000000020",
	}

	call := cf.TransformCall("call_0")
	require.NotNil(t, call)

	assert.True(t, call.Revert)
	assert.Equal(t, "execution reverted", call.Error)
	assert.Equal(t, "0x08c379a00000000000000000000000000000000000000000000000000000000000000020", call.RevertReason)
}

func TestCallFrame_TransformTrace_LargeValues(t *testing.T) {
	t.Parallel()

	// Test with very large values (beyond uint64)
	cf := CallFrame{
		Type:    "CALL",
		From:    "0x4142434445464748494a4b4c4d4e4f5051525354",
		To:      "0x55565758595a5b5c5d5e5f6061626364656667",
		Value:   "0xffffffffffffffffffffffffff",
		Gas:     "0xffffffff",
		GasUsed: "0xeeeeeeee",
		Input:   "0x",
		Output:  "0x",
	}

	call := cf.TransformCall("call_0")
	require.NotNil(t, call)

	// Verify large values are properly converted
	assert.NotEqual(t, "0", call.Value)
	assert.NotEqual(t, "0", call.Gas)
	assert.NotEqual(t, "0", call.GasUsed)
}
