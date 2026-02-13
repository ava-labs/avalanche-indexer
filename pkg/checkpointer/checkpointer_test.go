package checkpointer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
)

type mockCheckpointer struct {
	mock.Mock
}

func (m *mockCheckpointer) Initialize(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockCheckpointer) Write(ctx context.Context, evmChainID uint64, lowestUnprocessed uint64) error {
	args := m.Called(ctx, evmChainID, lowestUnprocessed)
	return args.Error(0)
}

func (m *mockCheckpointer) Read(ctx context.Context, evmChainID uint64) (uint64, bool, error) {
	args := m.Called(ctx, evmChainID)
	return args.Get(0).(uint64), args.Bool(1), args.Error(2)
}

func TestStart_WritesAndCancels(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(5, 10)
	require.NoError(t, err)
	checkpointer := &mockCheckpointer{}

	called := make(chan struct{}, 1)
	checkpointer.
		On("Write", mock.Anything, uint64(43114), uint64(5)).
		Run(func(_ mock.Arguments) {
			select {
			case called <- struct{}{}:
			default:
			}
		}).
		Return(nil).
		Once() // Only periodic write, no shutdown write

	cfg := Config{
		Interval:     10 * time.Millisecond,
		WriteTimeout: 1 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 300 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Start(ctx, state, checkpointer, cfg, 43114)
	}()

	select {
	case <-called:
		// Stop scheduler - should exit immediately without another write
		cancel()
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "timeout waiting for checkpoint write")
	}

	select {
	case err := <-done:
		require.NoError(t, err, "context cancellation should return nil")
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "timeout waiting for scheduler to exit")
	}
	checkpointer.AssertExpectations(t)
}

func TestStart_ErrorPropagates(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(1, 1)
	require.NoError(t, err)
	checkpointer := &mockCheckpointer{}
	writeErr := errors.New("write failed")
	checkpointer.
		On("Write", mock.Anything, uint64(43114), uint64(1)).
		Return(writeErr).
		Times(4) // initial try + 3 retries

	cfg := Config{
		Interval:     5 * time.Millisecond,
		WriteTimeout: 1 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 1 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	gotErr := Start(ctx, state, checkpointer, cfg, 43114)
	require.ErrorIs(t, gotErr, writeErr)
	checkpointer.AssertExpectations(t)
}

func TestStart_ImmediateCancel(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(0, 0)
	require.NoError(t, err)
	checkpointer := &mockCheckpointer{}

	// No writes expected - immediate cancellation should exit immediately
	// (no shutdown checkpoint write)

	cfg := DefaultConfig()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = Start(ctx, state, checkpointer, cfg, 43114)
	require.NoError(t, err, "immediate cancellation should return nil")
	checkpointer.AssertExpectations(t)
}

func TestStart_CancelDuringRetry(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(1, 1)
	require.NoError(t, err)
	checkpointer := &mockCheckpointer{}

	writeErr := errors.New("write failed")
	writeCalled := make(chan struct{}, 1)
	checkpointer.
		On("Write", mock.Anything, uint64(43114), uint64(1)).
		Run(func(_ mock.Arguments) {
			select {
			case writeCalled <- struct{}{}:
			default:
			}
		}).
		Return(writeErr).
		Once()

	cfg := Config{
		Interval:     10 * time.Millisecond,
		WriteTimeout: 1 * time.Second,
		MaxRetries:   10, // Many retries to ensure we can cancel during retry
		RetryBackoff: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Start(ctx, state, checkpointer, cfg, 43114)
	}()

	// Wait for first write attempt, then cancel during retry backoff
	select {
	case <-writeCalled:
		// Cancel during retry backoff - should exit cleanly
		time.Sleep(10 * time.Millisecond)
		cancel()
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "timeout waiting for first write attempt")
	}

	select {
	case err := <-done:
		require.NoError(t, err, "context cancellation during retry should return nil")
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "timeout waiting for scheduler to exit after cancellation")
	}
}
