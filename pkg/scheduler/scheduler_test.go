package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/checkpoint"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockCheckpointRepo struct {
	mock.Mock
}

func (m *mockCheckpointRepo) CreateTableIfNotExists(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockCheckpointRepo) WriteCheckpoint(ctx context.Context, s *checkpoint.Checkpoint) error {
	args := m.Called(ctx, s)
	return args.Error(0)
}

func (m *mockCheckpointRepo) ReadCheckpoint(ctx context.Context, chainID uint64) (*checkpoint.Checkpoint, error) {
	args := m.Called(ctx, chainID)
	if v := args.Get(0); v != nil {
		return v.(*checkpoint.Checkpoint), args.Error(1)
	}
	return nil, args.Error(1)
}

func TestStartCheckpointScheduler_WritesAndCancels(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(5, 10)
	require.NoError(t, err)
	repo := &mockCheckpointRepo{}

	called := make(chan struct{}, 1)
	repo.
		On("WriteCheckpoint", mock.Anything, mock.AnythingOfType("*checkpoint.Checkpoint")).
		Run(func(args mock.Arguments) {
			s := args.Get(1).(*checkpoint.Checkpoint)
			assert.Equal(t, uint64(5), s.Lowest)
			assert.Positive(t, s.Timestamp)
			select {
			case called <- struct{}{}:
			default:
			}
		}).
		Return(nil).
		Once()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Start(ctx, state, repo, 10*time.Millisecond, 43114)
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
	repo.AssertExpectations(t)
}

func TestStartCheckpointScheduler_ErrorPropagates(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(1, 1)
	require.NoError(t, err)
	repo := &mockCheckpointRepo{}
	writeErr := errors.New("write failed")
	repo.
		On("WriteCheckpoint", mock.Anything, mock.AnythingOfType("*checkpoint.Checkpoint")).
		Return(writeErr).
		Times(4) // initial try + 3 retries

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	gotErr := Start(ctx, state, repo, 5*time.Millisecond, 43114)
	require.ErrorIs(t, gotErr, writeErr)
	repo.AssertExpectations(t)
}

func TestStartCheckpointScheduler_ImmediateCancel(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(0, 0)
	require.NoError(t, err)
	repo := &mockCheckpointRepo{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = Start(ctx, state, repo, time.Second, 43114)
	require.NoError(t, err)
}
