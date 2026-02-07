package checkpoint

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse/testutils"
)

// rowMock is a minimal implementation of driver.Row that populates provided destinations.
type rowMock struct {
	chainID                uint64
	lowestUnprocessedBlock uint64
	timestamp              int64
}

func (r rowMock) Scan(dest ...interface{}) error {
	if len(dest) != 3 {
		return errors.New("unexpected dest len")
	}
	if p, ok := dest[0].(*uint64); ok && p != nil {
		*p = r.chainID
	}
	if p, ok := dest[1].(*uint64); ok && p != nil {
		*p = r.lowestUnprocessedBlock
	}
	if p, ok := dest[2].(*int64); ok && p != nil {
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

func TestRepository_WriteCheckpoint_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	// Expect Exec with query and args
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "checkpoints")
		})).
		Return(nil)
	mockConn.
		On("Exec", mock.Anything, "INSERT INTO default.checkpoints (chain_id, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?)\n",
			mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	now := time.Now().Unix()
	err = repo.WriteCheckpoint(ctx, &Checkpoint{ChainID: 43114, Lowest: 123, Timestamp: now})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_WriteCheckpoint_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	execErr := errors.New("exec failed")
	ctx := t.Context()

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "checkpoints")
		})).
		Return(nil)
	mockConn.
		On("Exec", mock.Anything, "INSERT INTO default.checkpoints (chain_id, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?)\n",
			mock.Anything, mock.Anything, mock.Anything).
		Return(execErr)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	err = repo.WriteCheckpoint(ctx, &Checkpoint{ChainID: 43114, Lowest: 1, Timestamp: 2})
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

func TestRepository_ReadCheckpoint_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Prepare row with values
	row := rowMock{chainID: 43114, lowestUnprocessedBlock: 777, timestamp: 1700000000}

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "checkpoints")
		})).
		Return(nil)
	mockConn.
		On("QueryRow", mock.Anything, "SELECT * FROM default.checkpoints WHERE chain_id = ? ORDER BY timestamp DESC LIMIT 1\n", mock.Anything).
		Return(row)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	got, err := repo.ReadCheckpoint(ctx, 43114)
	require.NoError(t, err)
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

func TestRepository_ReadCheckpoint_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	scanErr := errors.New("scan failed")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "checkpoints")
		})).
		Return(nil)
	mockConn.
		On("QueryRow", mock.Anything, "SELECT * FROM default.checkpoints WHERE chain_id = ? ORDER BY timestamp DESC LIMIT 1\n", mock.Anything).
		Return(rowErrMock{err: scanErr})

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	got, err := repo.ReadCheckpoint(ctx, 43114)
	assert.Nil(t, got)
	require.ErrorIs(t, err, scanErr)
	mockConn.AssertExpectations(t)
}

func TestRepository_CreateTableIfNotExists_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && containsSubstring(q, "CREATE TABLE IF NOT EXISTS") && containsSubstring(q, "checkpoints")
		})).
		Return(nil)
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 // Just verify a query is passed
		})).
		Return(nil)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	err = repo.CreateTableIfNotExists(ctx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_CreateTableIfNotExists_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}

	createTableErr := errors.New("table creation failed")
	mockConn.
		On("Exec", mock.Anything, mock.Anything).
		Return(createTableErr)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.Nil(t, repo)
	require.ErrorIs(t, err, createTableErr)
	mockConn.AssertExpectations(t)
}

func containsSubstring(s, substr string) bool {
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
