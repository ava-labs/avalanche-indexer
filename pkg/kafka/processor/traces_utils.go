package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
)

var (
	ErrMissingTxHash     = errors.New("txHash field is missing in trace")
	ErrInvalidTxHashType = errors.New("txHash field is not a string")
	ErrMissingResult     = errors.New("key 'result' does not exist in traces")
	ErrTraceUnmarshal    = errors.New("trace unmarshall error")
	ErrMalformedTraces   = errors.New("malformed traces")
)

type FlatCall struct {
	Type         string         `json:"type"`
	From         common.Address `json:"from"`
	To           common.Address `json:"to"`
	Value        string         `json:"value"`
	Gas          string         `json:"gas"`
	GasUsed      string         `json:"gasUsed"`
	Revert       bool           `json:"revert"`
	Error        string         `json:"error"`
	RevertReason string         `json:"revertReason"`
	Input        string         `json:"input"`
	Output       string         `json:"output"`
	CallIndex    string         `json:"callIndex"`
}

type CallFrame struct {
	Type         string      `json:"type"`
	From         string      `json:"from"`
	To           string      `json:"to"`
	Value        string      `json:"value"`
	Gas          string      `json:"gas"` // Aka gas limit
	GasUsed      string      `json:"gasUsed"`
	Input        string      `json:"input"`
	Output       string      `json:"output"`
	Error        string      `json:"error"`
	RevertReason string      `json:"revertReason"`
	Calls        []CallFrame `json:"calls"`
}

// GetTracesForTransaction extracts the transaction hash from the traces and returns the transformed traces.
func GetTracesForTransaction(traces json.RawMessage) (string, []*FlatCall, error) {
	var tr map[string]CallFrame
	// this process will take out the txHash in the Ethereum trace call, our CallFrame struct is on a
	// trace level so this will take out the tx hash to be able to align to the struct
	var jsonMap map[string]interface{}
	err := json.Unmarshal(traces, &jsonMap)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", ErrTraceUnmarshal, err)
	}
	txHashRaw, exists := jsonMap["txHash"]
	if !exists {
		return "", nil, ErrMissingTxHash
	}
	txHash, ok := txHashRaw.(string)
	if !ok {
		return "", nil, fmt.Errorf("%w: got type %T", ErrInvalidTxHashType, txHashRaw)
	}
	// Remove the key
	delete(jsonMap, "txHash")
	// Marshal back into a json.RawMessage
	jsonRaw, err := json.Marshal(jsonMap)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", ErrMalformedTraces, err)
	}
	err = json.Unmarshal(jsonRaw, &tr)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", ErrMalformedTraces, err)
	}
	cf, exists := tr["result"]
	if !exists {
		return "", nil, ErrMissingResult
	}
	return txHash, cf.TransformTrace("call", 0), nil
}

func (c *CallFrame) TransformTrace(indexPrefix string, index int64) []*FlatCall {
	callIndex := indexPrefix + "_" + strconv.FormatInt(index, 10)
	root := c.TransformCall(callIndex)
	results := make([]*FlatCall, 1, 1+len(c.Calls))
	results[0] = root
	for idx, calls := range c.Calls {
		// Ensure all children of a reverted call
		// are also reverted!
		if root.Revert {
			// Copy error message from parent
			// if child does not have one
			if len(calls.Error) == 0 {
				calls.Error = c.Error
			}
		}
		children := calls.TransformTrace(callIndex, int64(idx))
		results = append(results, children...)
	}
	return results
}

func (c *CallFrame) TransformCall(callIndex string) *FlatCall {
	call := FlatCall{
		Type:         c.Type,
		From:         common.HexToAddress(c.From),
		To:           common.HexToAddress(c.To),
		Value:        "0",
		Gas:          "0",
		GasUsed:      "0",
		Revert:       false,
		Error:        c.Error,
		RevertReason: c.RevertReason,
		Input:        c.Input,
		Output:       c.Output,
		CallIndex:    callIndex,
	}

	if len(c.Value) > 0 {
		call.Value = (hexutil.MustDecodeBig(c.Value)).String()
	}
	if len(c.Gas) > 0 {
		call.Gas = (hexutil.MustDecodeBig(c.Gas)).String()
	}
	if len(c.GasUsed) > 0 {
		call.GasUsed = (hexutil.MustDecodeBig(c.GasUsed)).String()
	}
	if len(c.Error) > 0 {
		call.Revert = true
	}

	return &call
}
