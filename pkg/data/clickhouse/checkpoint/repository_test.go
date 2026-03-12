package checkpoint

import (
	"database/sql"
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
	mode                   string
	lowestUnprocessedBlock uint64
	timestamp              int64
}

func (r rowMock) Scan(dest ...interface{}) error {
	if len(dest) != 4 {
		return errors.New("unexpected dest len")
	}
	if p, ok := dest[0].(*uint64); ok && p != nil {
		*p = r.chainID
	}
	if p, ok := dest[1].(*string); ok && p != nil {
		*p = r.mode
	}
	if p, ok := dest[2].(*uint64); ok && p != nil {
		*p = r.lowestUnprocessedBlock
	}
	if p, ok := dest[3].(*int64); ok && p != nil {
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

func TestRepository_Write_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()
	// Expect Exec with query and args
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && (containsSubstring(q, "CREATE TABLE IF NOT EXISTS") || containsSubstring(q, "ALTER TABLE"))
		})).
		Return(nil)
	mockConn.
		On("Exec", mock.Anything, "INSERT INTO `default`.`checkpoints` (chain_id, mode, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?, ?)\n",
			uint64(43114), "blocks", uint64(123), mock.MatchedBy(func(ts int64) bool {
				return ts > time.Now().Unix()-60 && ts <= time.Now().Unix()
			})).
		Return(nil)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	err = repo.Write(ctx, 43114, "blocks", 123)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_Write_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	execErr := errors.New("exec failed")
	ctx := t.Context()

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && (containsSubstring(q, "CREATE TABLE IF NOT EXISTS") || containsSubstring(q, "ALTER TABLE"))
		})).
		Return(nil)
	mockConn.
		On("Exec", mock.Anything, "INSERT INTO `default`.`checkpoints` (chain_id, mode, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?, ?)\n",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(execErr)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	err = repo.Write(ctx, 43114, "blocks", 1)
	require.ErrorIs(t, err, execErr)
	mockConn.AssertExpectations(t)
}

func TestRepository_Read_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Prepare row with values
	row := rowMock{chainID: 43114, mode: "blocks", lowestUnprocessedBlock: 777, timestamp: 1700000000}

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && (containsSubstring(q, "CREATE TABLE IF NOT EXISTS") || containsSubstring(q, "ALTER TABLE"))
		})).
		Return(nil)
	mockConn.
		On("QueryRow", mock.Anything, "SELECT * FROM `default`.`checkpoints` WHERE chain_id = ? AND mode = ? ORDER BY timestamp DESC LIMIT 1\n", uint64(43114), "blocks").
		Return(row)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	lowest, exists, err := repo.Read(ctx, 43114, "blocks")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, uint64(777), lowest)
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

func TestRepository_Read_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	scanErr := errors.New("scan failed")
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && (containsSubstring(q, "CREATE TABLE IF NOT EXISTS") || containsSubstring(q, "ALTER TABLE"))
		})).
		Return(nil)
	mockConn.
		On("QueryRow", mock.Anything, "SELECT * FROM `default`.`checkpoints` WHERE chain_id = ? AND mode = ? ORDER BY timestamp DESC LIMIT 1\n", uint64(43114), "blocks").
		Return(rowErrMock{err: scanErr})

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	lowest, exists, err := repo.Read(ctx, 43114, "blocks")
	assert.False(t, exists)
	assert.Equal(t, uint64(0), lowest)
	require.ErrorIs(t, err, scanErr)
	mockConn.AssertExpectations(t)
}

func TestRepository_Initialize_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && (containsSubstring(q, "CREATE TABLE IF NOT EXISTS") || containsSubstring(q, "ALTER TABLE"))
		})).
		Return(nil)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	err = repo.Initialize(ctx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_Initialize_Error(t *testing.T) {
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

func TestRepository_DeleteCheckpoints_Success(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && (containsSubstring(q, "CREATE TABLE IF NOT EXISTS") || containsSubstring(q, "ALTER TABLE"))
		})).
		Return(nil)

	mockConn.
		On("Exec", mock.Anything, "DELETE FROM `default`.`checkpoints_local` ON CLUSTER 'default' WHERE chain_id = ?\n", mock.Anything).
		Return(nil)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	err = repo.DeleteCheckpoints(ctx, 43114)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_DeleteCheckpoints_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && (containsSubstring(q, "CREATE TABLE IF NOT EXISTS") || containsSubstring(q, "ALTER TABLE"))
		})).
		Return(nil)

	deleteErr := errors.New("delete failed")
	mockConn.
		On("Exec", mock.Anything, "DELETE FROM `default`.`checkpoints_local` ON CLUSTER 'default' WHERE chain_id = ?\n", mock.Anything).
		Return(deleteErr)

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)
	err = repo.DeleteCheckpoints(ctx, 43114)
	require.ErrorIs(t, err, deleteErr)
	mockConn.AssertExpectations(t)
}

func TestRepository_Read_NotExists(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Return sql.ErrNoRows to simulate no checkpoint exists
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && (containsSubstring(q, "CREATE TABLE IF NOT EXISTS") || containsSubstring(q, "ALTER TABLE"))
		})).
		Return(nil)

	mockConn.
		On("QueryRow", mock.Anything, "SELECT * FROM `default`.`checkpoints` WHERE chain_id = ? AND mode = ? ORDER BY timestamp DESC LIMIT 1\n", uint64(43114), "traces").
		Return(rowErrMock{err: sql.ErrNoRows})

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)

	lowest, exists, err := repo.Read(ctx, 43114, "traces")
	require.NoError(t, err)
	assert.False(t, exists)
	assert.Equal(t, uint64(0), lowest)
	mockConn.AssertExpectations(t)
}

func TestNewRepository_Error(t *testing.T) {
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

func TestRepository_Write_Read_DifferentModes(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	// Expect table creation and migrations
	mockConn.
		On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
			return len(q) > 0 && (containsSubstring(q, "CREATE TABLE IF NOT EXISTS") || containsSubstring(q, "ALTER TABLE"))
		})).
		Return(nil)

	// Write for "blocks" mode
	mockConn.
		On("Exec", mock.Anything, "INSERT INTO `default`.`checkpoints` (chain_id, mode, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?, ?)\n",
			uint64(43114), "blocks", uint64(100), mock.Anything).
		Return(nil).
		Once()

	// Write for "traces" mode
	mockConn.
		On("Exec", mock.Anything, "INSERT INTO `default`.`checkpoints` (chain_id, mode, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?, ?)\n",
			uint64(43114), "traces", uint64(200), mock.Anything).
		Return(nil).
		Once()

	// Read for "blocks" mode
	blocksRow := rowMock{chainID: 43114, mode: "blocks", lowestUnprocessedBlock: 100, timestamp: 1700000000}
	mockConn.
		On("QueryRow", mock.Anything, "SELECT * FROM `default`.`checkpoints` WHERE chain_id = ? AND mode = ? ORDER BY timestamp DESC LIMIT 1\n", uint64(43114), "blocks").
		Return(blocksRow).
		Once()

	// Read for "traces" mode
	tracesRow := rowMock{chainID: 43114, mode: "traces", lowestUnprocessedBlock: 200, timestamp: 1700000001}
	mockConn.
		On("QueryRow", mock.Anything, "SELECT * FROM `default`.`checkpoints` WHERE chain_id = ? AND mode = ? ORDER BY timestamp DESC LIMIT 1\n", uint64(43114), "traces").
		Return(tracesRow).
		Once()

	repo, err := NewRepository(testutils.NewTestClient(mockConn), "default", "default", "checkpoints")
	require.NoError(t, err)

	// Write both modes
	err = repo.Write(ctx, 43114, "blocks", 100)
	require.NoError(t, err)
	err = repo.Write(ctx, 43114, "traces", 200)
	require.NoError(t, err)

	// Read blocks mode - should get 100
	lowest, exists, err := repo.Read(ctx, 43114, "blocks")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, uint64(100), lowest)

	// Read traces mode - should get 200 (not 100)
	lowest, exists, err = repo.Read(ctx, 43114, "traces")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, uint64(200), lowest)

	mockConn.AssertExpectations(t)
}
