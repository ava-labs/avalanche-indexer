package checkpointer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestStartCheckpointScheduler_WritesAndCancels(t *testing.T) {
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
		Twice() // Once for periodic write, once for graceful shutdown

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
		// stop scheduler
		cancel()
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "timeout waiting for checkpoint write")
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "timeout waiting for scheduler to exit")
	}
	checkpointer.AssertExpectations(t)
}

func TestStartCheckpointScheduler_ErrorPropagates(t *testing.T) {
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

func TestStartCheckpointScheduler_ImmediateCancel(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(0, 0)
	require.NoError(t, err)
	checkpointer := &mockCheckpointer{}

	// Expect shutdown checkpoint write
	checkpointer.
		On("Write", mock.Anything, uint64(43114), uint64(0)).
		Return(nil).
		Once()

	cfg := DefaultConfig()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = Start(ctx, state, checkpointer, cfg, 43114)
	require.NoError(t, err)
	checkpointer.AssertExpectations(t)
}

func TestDefaultConfig(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	assert.Equal(t, 30*time.Second, cfg.Interval)
	assert.Equal(t, 1*time.Second, cfg.WriteTimeout)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.Equal(t, 300*time.Millisecond, cfg.RetryBackoff)
}
