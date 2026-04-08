package batchwriter

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
)

func testBlock(hash string) *evmrepo.BlockRow {
	return &evmrepo.BlockRow{
		Hash:         hash,
		EVMChainID:   big.NewInt(1),
		BlockNumber:  big.NewInt(1),
		BlockchainID: strPtr("bc"),
	}
}

func strPtr(s string) *string { return &s }

// --- fakeBlocks implements evmrepo.Blocks for tests (only BatchInsertBlocks is used).

type fakeBlocks struct {
	mu sync.Mutex

	batchSizes []int
	err        error

	// If set, the first BatchInsertBlocks call blocks until this channel is closed.
	blockFirstFlush chan struct{}
}

func (f *fakeBlocks) CreateTableIfNotExists(ctx context.Context) error { return nil }
func (f *fakeBlocks) WriteBlock(ctx context.Context, block *evmrepo.BlockRow) error {
	return nil
}
func (f *fakeBlocks) DeleteBlocks(ctx context.Context, chainID uint64) error { return nil }

func (f *fakeBlocks) BatchInsertBlocks(ctx context.Context, blocks []*evmrepo.BlockRow) error {
	if f.blockFirstFlush != nil {
		select {
		case <-f.blockFirstFlush:
		case <-ctx.Done():
			return ctx.Err()
		}
		f.blockFirstFlush = nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchSizes = append(f.batchSizes, len(blocks))
	return f.err
}

func (f *fakeBlocks) batchSizesSnapshot() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int, len(f.batchSizes))
	copy(out, f.batchSizes)
	return out
}

func TestTakeBatch(t *testing.T) {
	t.Parallel()

	t.Run("empty pending", func(t *testing.T) {
		t.Parallel()
		var pending []*WriteRequest
		got := takeBatch(&pending)
		require.Nil(t, got)
		require.Empty(t, pending)
	})

	t.Run("copies and clears pending", func(t *testing.T) {
		t.Parallel()
		a, b := &WriteRequest{}, &WriteRequest{}
		pending := []*WriteRequest{a, b}
		got := takeBatch(&pending)
		require.Len(t, got, 2)
		require.Equal(t, a, got[0])
		require.Equal(t, b, got[1])
		require.Empty(t, pending)
		require.Equal(t, 2, cap(pending)) // length cleared; backing array retained for reuse
	})

	t.Run("batch independent of pending reuse", func(t *testing.T) {
		t.Parallel()
		r1, r2 := &WriteRequest{}, &WriteRequest{}
		pending := []*WriteRequest{r1, r2}
		batch := takeBatch(&pending)
		pending = append(pending, &WriteRequest{})
		require.Len(t, batch, 2)
		require.Equal(t, r1, batch[0])
		require.Equal(t, r2, batch[1])
	})
}

func TestWriter_FlushOnMaxBlocks(t *testing.T) {
	t.Parallel()

	fb := &fakeBlocks{}
	w := New(Config{
		Workers:      1,
		MaxBlocks:    2,
		FlushTimeout: time.Hour,
	}, Repositories{Blocks: fb}, zap.NewNop().Sugar(), metrics.NewNoOp())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	doneStart := make(chan struct{})
	go func() {
		close(doneStart)
		_ = w.Start(ctx)
	}()
	<-doneStart
	time.Sleep(20 * time.Millisecond)

	bg := context.Background()
	req1 := &WriteRequest{Block: testBlock("h1")}
	ch1 := w.Submit(bg, req1)
	req2 := &WriteRequest{Block: testBlock("h2")}
	ch2 := w.Submit(bg, req2)

	select {
	case err := <-ch1:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first write")
	}
	select {
	case err := <-ch2:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for second write")
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	sizes := fb.batchSizesSnapshot()
	require.Equal(t, []int{2}, sizes)
}

func TestWriter_FlushOnTimeout(t *testing.T) {
	t.Parallel()

	fb := &fakeBlocks{}
	w := New(Config{
		Workers:      1,
		MaxBlocks:    100,
		FlushTimeout: 150 * time.Millisecond,
	}, Repositories{Blocks: fb}, zap.NewNop().Sugar(), metrics.NewNoOp())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)

	req := &WriteRequest{Block: testBlock("solo")}
	ch := w.Submit(context.Background(), req)

	select {
	case err := <-ch:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for flush")
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	sizes := fb.batchSizesSnapshot()
	require.Equal(t, []int{1}, sizes)
}

func TestWriter_FlushErrorPropagates(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("clickhouse unavailable")
	fb := &fakeBlocks{err: wantErr}
	w := New(Config{
		Workers:      1,
		MaxBlocks:    1,
		FlushTimeout: time.Hour,
	}, Repositories{Blocks: fb}, zap.NewNop().Sugar(), metrics.NewNoOp())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)

	ch := w.Submit(context.Background(), &WriteRequest{Block: testBlock("x")})

	select {
	case err := <-ch:
		require.ErrorIs(t, err, wantErr)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	cancel()
}

func TestWriter_ShutdownSignalsPending(t *testing.T) {
	t.Parallel()

	fb := &fakeBlocks{}
	w := New(Config{
		Workers:      1,
		MaxBlocks:    10,
		FlushTimeout: time.Hour,
	}, Repositories{Blocks: fb}, zap.NewNop().Sugar(), metrics.NewNoOp())

	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	go func() {
		close(started)
		_ = w.Start(ctx)
	}()
	<-started
	time.Sleep(30 * time.Millisecond)

	// One block in pending, no timeout flush yet — cancel before timer fires.
	req := &WriteRequest{Block: testBlock("orphan")}
	ch := w.Submit(context.Background(), req)

	cancel()

	select {
	case err := <-ch:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for shutdown signal")
	}
}

func TestSubmit_ContextCancelledBeforeEnqueue(t *testing.T) {
	t.Parallel()

	// Hold first flush so dispatcher blocks in spawnFlush on the second batch,
	// filling the request channel buffer so a third Submit can use ctx cancel.
	unblock := make(chan struct{})
	fb := &fakeBlocks{blockFirstFlush: unblock}

	w := New(Config{
		Workers:      1,
		MaxBlocks:    1,
		FlushTimeout: time.Hour,
	}, Repositories{Blocks: fb}, zap.NewNop().Sugar(), metrics.NewNoOp())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Start(ctx) }()
	time.Sleep(30 * time.Millisecond)

	bg := context.Background()
	_ = w.Submit(bg, &WriteRequest{Block: testBlock("a")})
	_ = w.Submit(bg, &WriteRequest{Block: testBlock("b")})

	ctx3, cancel3 := context.WithCancel(context.Background())
	cancel3()
	ch3 := w.Submit(ctx3, &WriteRequest{Block: testBlock("c")})

	select {
	case err := <-ch3:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("expected immediate cancel on Submit")
	}

	close(unblock)
	cancel()
}

func TestWriter_NilMetricsUsesNoOp(t *testing.T) {
	t.Parallel()

	fb := &fakeBlocks{}
	w := New(Config{
		Workers:      1,
		MaxBlocks:    1,
		FlushTimeout: time.Millisecond,
	}, Repositories{Blocks: fb}, zap.NewNop().Sugar(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Start(ctx) }()
	time.Sleep(30 * time.Millisecond)

	ch := w.Submit(context.Background(), &WriteRequest{Block: testBlock("m")})
	select {
	case err := <-ch:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
	cancel()
}
