package utils

import (
	"encoding/hex"
	"time"
)

// HexToBytes32 converts a hex string (with or without 0x prefix) to a 32-byte array
func HexToBytes32(hexStr string) ([32]byte, error) {
	// Remove 0x prefix if present
	if len(hexStr) >= 2 && hexStr[0:2] == "0x" {
		hexStr = hexStr[2:]
	}
	// Pad to 64 hex characters (32 bytes)
	for len(hexStr) < 64 {
		hexStr = "0" + hexStr
	}
	// Trim if too long
	if len(hexStr) > 64 {
		hexStr = hexStr[:64]
	}
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return [32]byte{}, err
	}
	var result [32]byte
	copy(result[:], bytes)
	return result, nil
}

// HexToBytes20 converts a hex string (with or without 0x prefix) to a 20-byte array
func HexToBytes20(hexStr string) ([20]byte, error) {
	// Remove 0x prefix if present
	if len(hexStr) >= 2 && hexStr[0:2] == "0x" {
		hexStr = hexStr[2:]
	}
	// Pad to 40 hex characters (20 bytes)
	for len(hexStr) < 40 {
		hexStr = "0" + hexStr
	}
	// Trim if too long
	if len(hexStr) > 40 {
		hexStr = hexStr[:40]
	}
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return [20]byte{}, err
	}
	var result [20]byte
	copy(result[:], bytes)
	return result, nil
}

// HexToBytes8 converts a hex string (with or without 0x prefix) to an 8-byte array
func HexToBytes8(hexStr string) ([8]byte, error) {
	// Remove 0x prefix if present
	if len(hexStr) >= 2 && hexStr[0:2] == "0x" {
		hexStr = hexStr[2:]
	}
	// Pad to 16 hex characters (8 bytes)
	for len(hexStr) < 16 {
		hexStr = "0" + hexStr
	}
	// Trim if too long
	if len(hexStr) > 16 {
		hexStr = hexStr[:16]
	}
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return [8]byte{}, err
	}
	var result [8]byte
	copy(result[:], bytes)
	return result, nil
}

// MonthFromTime calculates the month as YYYYMM from a time.Time value.
// For example, January 2026 returns 202601.
// Returns 0 if the time is the zero value or if the year is before 1970 (Unix epoch).
func MonthFromTime(t time.Time) int {
	// Guard against zero time value (year 1) or invalid timestamps
	if t.IsZero() || t.Year() < 1970 {
		return 0
	}
	return t.Year()*100 + int(t.Month())
}
