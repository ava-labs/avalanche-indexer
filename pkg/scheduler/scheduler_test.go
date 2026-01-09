package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/snapshot"
	"github.com/ava-labs/avalanche-indexer/pkg/slidingwindow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockSnapshotRepo struct {
	mock.Mock
}

func (m *mockSnapshotRepo) WriteSnapshot(ctx context.Context, s *snapshot.Snapshot) error {
	args := m.Called(ctx, s)
	return args.Error(0)
}

func (m *mockSnapshotRepo) ReadSnapshot(ctx context.Context, chainID uint64) (*snapshot.Snapshot, error) {
	args := m.Called(ctx, chainID)
	if v := args.Get(0); v != nil {
		return v.(*snapshot.Snapshot), args.Error(1)
	}
	return nil, args.Error(1)
}

func TestStartSnapshotScheduler_WritesAndCancels(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(5, 10)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	repo := &mockSnapshotRepo{}

	called := make(chan struct{}, 1)
	repo.
		On("WriteSnapshot", mock.Anything, mock.AnythingOfType("*snapshot.Snapshot")).
		Run(func(args mock.Arguments) {
			s := args.Get(1).(*snapshot.Snapshot)
			assert.Equal(t, uint64(5), s.Lowest)
			assert.Greater(t, s.Timestamp, int64(0))
			select {
			case called <- struct{}{}:
			default:
			}
		}).
		Return(nil).
		Once()

	ctx, cancel := context.WithCancel(context.Background())
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
		t.Fatalf("timeout waiting for snapshot write")
	}

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for scheduler to exit")
	}
	repo.AssertExpectations(t)
}

func TestStartSnapshotScheduler_ErrorPropagates(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(1, 1)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	repo := &mockSnapshotRepo{}
	repo.
		On("WriteSnapshot", mock.Anything, mock.AnythingOfType("*snapshot.Snapshot")).
		Return(errors.New("write failed")).
		Times(4) // initial try + 3 retries

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	gotErr := Start(ctx, state, repo, 5*time.Millisecond, 43114)
	assert.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "failed to write snapshot")
	repo.AssertExpectations(t)
}

func TestStartSnapshotScheduler_ImmediateCancel(t *testing.T) {
	t.Parallel()
	state, err := slidingwindow.NewState(0, 0)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	repo := &mockSnapshotRepo{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = Start(ctx, state, repo, time.Second, 43114)
	assert.NoError(t, err)
}
