package utils

import (
	"encoding/hex"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHexToBytes32(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    [32]byte
		wantErr bool
	}{
		{
			name:    "with 0x prefix",
			input:   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			want:    [32]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
			wantErr: false,
		},
		{
			name:    "without 0x prefix",
			input:   "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			want:    [32]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
			wantErr: false,
		},
		{
			name:    "empty string pads with zeros",
			input:   "",
			want:    [32]byte{},
			wantErr: false,
		},
		{
			name:    "short string pads with zeros",
			input:   "0x1234",
			want:    [32]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34},
			wantErr: false,
		},
		{
			name:    "too long string trims",
			input:   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2021222324",
			want:    [32]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
			wantErr: false,
		},
		{
			name:    "invalid hex string",
			input:   "0xghijklmnopqrstuvwxyz",
			want:    [32]byte{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := HexToBytes32(tt.input)
			if tt.wantErr {
				// Verify it's a hex decoding error using ErrorIs pattern
				var invalidByteErr hex.InvalidByteError
				require.True(t, errors.As(err, &invalidByteErr), "expected hex.InvalidByteError, got: %v", err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestHexToBytes20(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    [20]byte
		wantErr bool
	}{
		{
			name:    "with 0x prefix",
			input:   "0x4142434445464748494a4b4c4d4e4f5051525354",
			want:    [20]byte{0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53, 0x54},
			wantErr: false,
		},
		{
			name:    "without 0x prefix",
			input:   "4142434445464748494a4b4c4d4e4f5051525354",
			want:    [20]byte{0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53, 0x54},
			wantErr: false,
		},
		{
			name:    "empty string pads with zeros",
			input:   "",
			want:    [20]byte{},
			wantErr: false,
		},
		{
			name:    "short string pads with zeros",
			input:   "0x1234",
			want:    [20]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34},
			wantErr: false,
		},
		{
			name:    "too long string trims",
			input:   "0x4142434445464748494a4b4c4d4e4f505152535455565758",
			want:    [20]byte{0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53, 0x54},
			wantErr: false,
		},
		{
			name:    "invalid hex string",
			input:   "0xghijklmnop",
			want:    [20]byte{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := HexToBytes20(tt.input)
			if tt.wantErr {
				// Verify it's a hex decoding error using ErrorIs pattern
				var invalidByteErr hex.InvalidByteError
				require.True(t, errors.As(err, &invalidByteErr), "expected hex.InvalidByteError, got: %v", err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestHexToBytes8(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    [8]byte
		wantErr bool
	}{
		{
			name:    "with 0x prefix",
			input:   "0x55565758595a5b5c",
			want:    [8]byte{0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c},
			wantErr: false,
		},
		{
			name:    "without 0x prefix",
			input:   "55565758595a5b5c",
			want:    [8]byte{0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c},
			wantErr: false,
		},
		{
			name:    "empty string pads with zeros",
			input:   "",
			want:    [8]byte{},
			wantErr: false,
		},
		{
			name:    "short string pads with zeros",
			input:   "0x1234",
			want:    [8]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34},
			wantErr: false,
		},
		{
			name:    "too long string trims",
			input:   "0x55565758595a5b5c5d5e5f",
			want:    [8]byte{0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c},
			wantErr: false,
		},
		{
			name:    "invalid hex string",
			input:   "0xghij",
			want:    [8]byte{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := HexToBytes8(tt.input)
			if tt.wantErr {
				// Verify it's a hex decoding error using ErrorIs pattern
				var invalidByteErr hex.InvalidByteError
				require.True(t, errors.As(err, &invalidByteErr), "expected hex.InvalidByteError, got: %v", err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
