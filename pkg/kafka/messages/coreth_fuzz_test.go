package messages

import (
	"testing"
)

// FuzzCorethBlockUnmarshal tests CorethBlock.Unmarshal with random JSON inputs.
// This is critical for security as it processes external Kafka messages.
// Run with: go test -fuzz=FuzzCorethBlockUnmarshal -fuzztime=30s ./pkg/kafka/messages/
func FuzzCorethBlockUnmarshal(f *testing.F) {
	// Seed corpus with valid JSON structures
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"number": "123"}`))
	f.Add([]byte(`{"number": "123", "hash": "0xabc", "parentHash": "0xdef"}`))
	f.Add([]byte(`{"evmChainId": "43114", "number": "1000000", "gasLimit": 8000000}`))
	f.Add([]byte(`{"transactions": []}`))
	f.Add([]byte(`{"transactions": [{"hash": "0x123", "from": "0xabc"}]}`))
	f.Add([]byte(`{"withdrawals": [{"index": 1, "validatorIndex": 2, "address": "0x123", "amount": 1000}]}`))

	// Edge cases
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`"string"`))
	f.Add([]byte(`123`))
	f.Add([]byte(`{"number": "not_a_number"}`))
	f.Add([]byte(`{"number": "-999999999999999999999999999999"}`))
	f.Add([]byte(`{"gasLimit": -1}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		block := &CorethBlock{}
		// Should never panic, only return errors for invalid input
		_ = block.Unmarshal(data)
	})
}

// FuzzCorethTransactionUnmarshal tests CorethTransaction.Unmarshal with random inputs.
// Run with: go test -fuzz=FuzzCorethTransactionUnmarshal -fuzztime=30s ./pkg/kafka/messages/
func FuzzCorethTransactionUnmarshal(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"hash": "0x123", "from": "0xabc", "to": "0xdef"}`))
	f.Add([]byte(`{"nonce": 0, "value": 1000000000000000000, "gas": 21000}`))
	f.Add([]byte(`{"type": 2, "maxFeePerGas": 100, "maxPriorityFeePerGas": 2}`))

	// Edge cases
	f.Add([]byte(`null`))
	f.Add([]byte(`{"nonce": -1}`))
	f.Add([]byte(`{"gas": 999999999999999999}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		tx := &CorethTransaction{}
		_ = tx.Unmarshal(data)
	})
}

// FuzzCorethWithdrawalUnmarshal tests CorethWithdrawal.Unmarshal with random inputs.
// Run with: go test -fuzz=FuzzCorethWithdrawalUnmarshal -fuzztime=30s ./pkg/kafka/messages/
func FuzzCorethWithdrawalUnmarshal(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"index": 1, "validatorIndex": 100, "address": "0x123", "amount": 32000000000}`))

	// Edge cases
	f.Add([]byte(`null`))
	f.Add([]byte(`{"index": -1}`))
	f.Add([]byte(`{"amount": 999999999999999999999999}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		w := &CorethWithdrawal{}
		_ = w.Unmarshal(data)
	})
}

// FuzzCorethBlockMarshalRoundtrip tests that Marshal/Unmarshal are consistent.
// If Unmarshal succeeds, Marshal should not panic.
func FuzzCorethBlockMarshalRoundtrip(f *testing.F) {
	f.Add([]byte(`{"number": "123", "hash": "0xabc"}`))
	f.Add([]byte(`{"evmChainId": "43114", "number": "1000000"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		block := &CorethBlock{}
		if err := block.Unmarshal(data); err == nil {
			// If unmarshal succeeded, marshal should not panic
			_, _ = block.Marshal()
		}
	})
}
