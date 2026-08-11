package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackoffNextDoublesAndCaps(t *testing.T) {
	b := NewBackoff(1*time.Second, 8*time.Second)

	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		8 * time.Second, // capped
		8 * time.Second,
	}
	for i, w := range want {
		require.Equalf(t, w, b.Next(), "Next() call %d", i+1)
	}
}

func TestBackoffReset(t *testing.T) {
	b := NewBackoff(1*time.Second, 30*time.Second)

	require.Equal(t, 1*time.Second, b.Next())
	require.Equal(t, 2*time.Second, b.Next())

	b.Reset()
	require.Equal(t, 1*time.Second, b.Next(), "Next() should return initial after Reset")
}
