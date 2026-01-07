package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// rowMock is a minimal implementation of driver.Row that populates provided destinations.
type rowMock struct {
	lowest    uint64
	timestamp int64
}

func (r rowMock) Scan(dest ...interface{}) error {
	if len(dest) != 2 {
		return errors.New("unexpected dest len")
	}
	if p, ok := dest[0].(*uint64); ok && p != nil {
		*p = r.lowest
	}
	if p, ok := dest[1].(*int64); ok && p != nil {
		*p = r.timestamp
	}
	return nil
}

func (r rowMock) Err() error {
	_ = r
	return nil
}

func (r rowMock) ScanStruct(dest any) error {
	return r.Scan(dest)
}

func TestRepository_WriteSnapshot_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := context.Background()
	// Expect Exec with query and args
	mockConn.
		On("Exec", mock.Anything, "INSERT INTO snapshots (lowest, timestamp) VALUES (?, ?)",
			mock.Anything, mock.Anything).
		Return(nil)

	repo := NewRepository(testutils.NewTestClient(mockConn, zap.NewNop().Sugar()), "snapshots")
	now := time.Now().Unix()
	err := repo.WriteSnapshot(ctx, &Snapshot{Lowest: 123, Timestamp: now})
	assert.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_WriteSnapshot_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := context.Background()
	mockConn.
		On("Exec", mock.Anything, "INSERT INTO snapshots (lowest, timestamp) VALUES (?, ?)",
			mock.Anything, mock.Anything).
		Return(errors.New("exec failed"))

	repo := NewRepository(testutils.NewTestClient(mockConn, zap.NewNop().Sugar()), "snapshots")
	err := repo.WriteSnapshot(ctx, &Snapshot{Lowest: 1, Timestamp: 2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write snapshot")
	mockConn.AssertExpectations(t)
}

func TestRepository_ReadSnapshot_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := context.Background()

	// Prepare row with values
	row := rowMock{lowest: 777, timestamp: 1700000000}
	mockConn.
		On("QueryRow", mock.Anything, "SELECT lowest, timestamp FROM snapshots").
		Return(row)

	repo := NewRepository(testutils.NewTestClient(mockConn, zap.NewNop().Sugar()), "snapshots")
	got, err := repo.ReadSnapshot(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, uint64(777), got.Lowest)
	assert.Equal(t, int64(1700000000), got.Timestamp)
	mockConn.AssertExpectations(t)
}

// rowErrMock returns a scan error
type rowErrMock struct{ err error }

func (r rowErrMock) Scan(dest ...interface{}) error {
	_ = dest
	return r.err
}

func (r rowErrMock) Err() error { return r.err }

func (r rowErrMock) ScanStruct(dest any) error {
	return r.Scan(dest)
}

func TestRepository_ReadSnapshot_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := context.Background()

	scanErr := errors.New("scan failed")
	mockConn.
		On("QueryRow", mock.Anything, "SELECT lowest, timestamp FROM snapshots").
		Return(rowErrMock{err: scanErr})

	repo := NewRepository(testutils.NewTestClient(mockConn, zap.NewNop().Sugar()), "snapshots")
	got, err := repo.ReadSnapshot(ctx)
	assert.Nil(t, got)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read snapshot")
	mockConn.AssertExpectations(t)
}
