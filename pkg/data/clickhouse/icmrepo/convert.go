package icmrepo

import (
	"fmt"
	"math/big"

	"github.com/ava-labs/avalanche-indexer/pkg/utils"
)

// hexToFixed32 converts a hex string to a 32-byte binary string for FixedString(32).
func hexToFixed32(s string) (string, error) {
	b, err := utils.HexToBytes32(s)
	if err != nil {
		return "", fmt.Errorf("hexToFixed32(%q): %w", s, err)
	}
	return string(b[:]), nil
}

// hexToFixed20 converts a hex string to a 20-byte binary string for FixedString(20).
func hexToFixed20(s string) (string, error) {
	b, err := utils.HexToBytes20(s)
	if err != nil {
		return "", fmt.Errorf("hexToFixed20(%q): %w", s, err)
	}
	return string(b[:]), nil
}

// bigIntStr returns the decimal string of v, or "0" if v is nil.
// Used when passing non-nullable UInt256 values via Exec.
func bigIntStr(v *big.Int) string {
	if v == nil {
		return "0"
	}
	return v.String()
}

// bigIntPtrStr returns a pointer to the decimal string of v for Nullable(UInt256) Exec args,
// or nil (SQL NULL) when v is nil.
func bigIntPtrStr(v *big.Int) *string {
	if v == nil {
		return nil
	}
	s := v.String()
	return &s
}

// bigIntStrs converts a slice of *big.Int to decimal strings for Array(UInt256) Exec args.
// Returns an empty non-nil slice when vs is nil or empty.
func bigIntStrs(vs []*big.Int) []string {
	result := make([]string, len(vs))
	for i, v := range vs {
		result[i] = bigIntStr(v)
	}
	return result
}

// bigIntOrZero returns v if non-nil, or a new zero-value big.Int.
// Used to guarantee non-nil *big.Int values in ch structs for AppendStruct.
func bigIntOrZero(v *big.Int) *big.Int {
	if v == nil {
		return new(big.Int)
	}
	return v
}

// bigIntsOrZero replaces nil elements in vs with zero-value big.Ints and
// returns an empty slice (not nil) when vs is nil.
func bigIntsOrZero(vs []*big.Int) []*big.Int {
	if vs == nil {
		return []*big.Int{}
	}
	result := make([]*big.Int, len(vs))
	for i, v := range vs {
		if v == nil {
			result[i] = new(big.Int)
		} else {
			result[i] = v
		}
	}
	return result
}

// hexAddressesToBinary converts hex address strings to 20-byte binary strings
// for Array(FixedString(20)) columns.
func hexAddressesToBinary(addrs []string) ([]string, error) {
	result := make([]string, len(addrs))
	for i, addr := range addrs {
		b, err := hexToFixed20(addr)
		if err != nil {
			return nil, fmt.Errorf("address %d: %w", i, err)
		}
		result[i] = b
	}
	return result, nil
}
