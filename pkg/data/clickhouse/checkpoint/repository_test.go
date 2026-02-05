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
		On("Exec", mock.Anything, "INSERT INTO checkpoints (chain_id, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?)",
			mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	repo := NewRepository(testutils.NewTestClient(mockConn), "checkpoints")
	now := time.Now().Unix()
	err := repo.WriteCheckpoint(ctx, &Checkpoint{ChainID: 43114, Lowest: 123, Timestamp: now})
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_WriteCheckpoint_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	execErr := errors.New("exec failed")
	ctx := t.Context()
	mockConn.
		On("Exec", mock.Anything, "INSERT INTO checkpoints (chain_id, lowest_unprocessed_block, timestamp) VALUES (?, ?, ?)",
			mock.Anything, mock.Anything, mock.Anything).
		Return(execErr)

	repo := NewRepository(testutils.NewTestClient(mockConn), "checkpoints")
	err := repo.WriteCheckpoint(ctx, &Checkpoint{ChainID: 43114, Lowest: 1, Timestamp: 2})
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
		On("QueryRow", mock.Anything, "SELECT * FROM checkpoints WHERE chain_id = 43114").
		Return(row)

	repo := NewRepository(testutils.NewTestClient(mockConn), "checkpoints")
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
		On("QueryRow", mock.Anything, "SELECT * FROM checkpoints WHERE chain_id = 43114").
		Return(rowErrMock{err: scanErr})

	repo := NewRepository(testutils.NewTestClient(mockConn), "checkpoints")
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
			return len(q) > 0 // Just verify a query is passed
		})).
		Return(nil)

	repo := NewRepository(testutils.NewTestClient(mockConn), "test_db.checkpoints")
	err := repo.CreateTableIfNotExists(ctx)
	require.NoError(t, err)
	mockConn.AssertExpectations(t)
}

func TestRepository_CreateTableIfNotExists_Error(t *testing.T) {
	t.Parallel()
	mockConn := &testutils.MockConn{}
	ctx := t.Context()

	createTableErr := errors.New("table creation failed")
	mockConn.
		On("Exec", mock.Anything, mock.Anything).
		Return(createTableErr)

	repo := NewRepository(testutils.NewTestClient(mockConn), "test_db.checkpoints")
	err := repo.CreateTableIfNotExists(ctx)
	require.ErrorIs(t, err, createTableErr)
	mockConn.AssertExpectations(t)
}
