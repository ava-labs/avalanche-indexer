package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// FuzzHexToBytes32 tests HexToBytes32 with random inputs to find panics or crashes.
// Run with: go test -fuzz=FuzzHexToBytes32 -fuzztime=30s ./pkg/utils/
func FuzzHexToBytes32(f *testing.F) {
	// Seed corpus with interesting edge cases
	f.Add("")
	f.Add("0x")
	f.Add("0x0")
	f.Add("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	f.Add("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	f.Add("0xGGGG")                          // invalid hex
	f.Add("0x" + string(make([]byte, 1000))) // very long input

	f.Fuzz(func(t *testing.T, input string) {
		// The function should never panic, only return errors for invalid input
		result, err := HexToBytes32(input)
		if err == nil {
			// If no error, result should be a valid 32-byte array
			require.Len(t, result, 32)
		}
	})
}

// FuzzHexToBytes20 tests HexToBytes20 with random inputs.
// Run with: go test -fuzz=FuzzHexToBytes20 -fuzztime=30s ./pkg/utils/
func FuzzHexToBytes20(f *testing.F) {
	// Seed corpus with interesting edge cases
	f.Add("")
	f.Add("0x")
	f.Add("0x0")
	f.Add("0x1234567890abcdef12345678901234567890abcd")
	f.Add("1234567890abcdef12345678901234567890abcd")
	f.Add("0xGGGG")                          // invalid hex
	f.Add("0x" + string(make([]byte, 1000))) // very long input

	f.Fuzz(func(t *testing.T, input string) {
		result, err := HexToBytes20(input)
		if err == nil {
			require.Len(t, result, 20)
		}
	})
}

// FuzzHexToBytes8 tests HexToBytes8 with random inputs.
// Run with: go test -fuzz=FuzzHexToBytes8 -fuzztime=30s ./pkg/utils/
func FuzzHexToBytes8(f *testing.F) {
	// Seed corpus with interesting edge cases
	f.Add("")
	f.Add("0x")
	f.Add("0x0")
	f.Add("0x1234567890abcdef")
	f.Add("1234567890abcdef")
	f.Add("0xGGGG")                          // invalid hex
	f.Add("0x" + string(make([]byte, 1000))) // very long input

	f.Fuzz(func(t *testing.T, input string) {
		result, err := HexToBytes8(input)
		if err == nil {
			require.Len(t, result, 8)
		}
	})
}
