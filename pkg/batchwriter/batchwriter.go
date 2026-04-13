package batchwriter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/ava-labs/avalanche-indexer/pkg/clickhouse"
	"github.com/ava-labs/avalanche-indexer/pkg/data/clickhouse/evmrepo"
	"github.com/ava-labs/avalanche-indexer/pkg/metrics"
)

// Config holds tuning knobs for the batch writer.
type Config struct {
	// Workers is the maximum number of concurrent flush goroutines. A
	// semaphore of this size limits how many batches may write to ClickHouse
	// at once; the dispatcher blocks until a slot is free before starting
	// the next flush.
	Workers int

	// MaxBlocks is the maximum number of block-level requests the dispatcher
	// accumulates before triggering a flush (subject to semaphore availability).
	MaxBlocks int

	// FlushTimeout is the maximum time the dispatcher waits after the first
	// request in a batch before flushing whatever has accumulated.
	FlushTimeout time.Duration
}

// Repositories groups the ClickHouse repositories the batch writer flushes to.
//
// Not every field is used in every deployment mode (e.g. blocks mode uses
// Blocks/Transactions/Logs; traces mode uses InternalTransactions). Callers
// populate only the fields relevant to their mode.
//
// When adding a new repository or extending what the batch writer persists,
// extend this struct, the flush path in this package, and the wiring in
// cmd/consumerindexer (see newProcessor)—those places must stay in sync.
type Repositories struct {
	Blocks               evmrepo.Blocks
	Transactions         evmrepo.Transactions
	Logs                 evmrepo.Logs
	InternalTransactions evmrepo.InternalTransactions
}

// WriteRequest represents a single block's worth of data submitted by a
// processor goroutine. The processor receives the write result on the
// channel returned by [Writer.Submit].
type WriteRequest struct {
	Block        *evmrepo.BlockRow
	Transactions []*evmrepo.TransactionRow
	Logs         []*evmrepo.LogRow
	InternalTxns []*evmrepo.InternalTransactionRow

	done chan error
}

// Writer accumulates [WriteRequest]s from processor goroutines and flushes
// them to ClickHouse in batches, amortising the per-round-trip cost across
// many blocks.
type Writer struct {
	cfg      Config
	repos    Repositories
	requests chan *WriteRequest
	log      *zap.SugaredLogger
	metrics  *metrics.Metrics
}

// New creates a Writer ready to accept requests. The internal request channel
// is buffered to Workers * MaxBlocks so producers only block when the
// dispatcher is backlogged.
func New(cfg Config, repos Repositories, log *zap.SugaredLogger, m *metrics.Metrics) *Writer {
	if m == nil {
		m = metrics.NewNoOp()
	}
	bufSize := cfg.Workers * cfg.MaxBlocks
	if bufSize < 1 {
		bufSize = 1
	}
	return &Writer{
		cfg:      cfg,
		repos:    repos,
		requests: make(chan *WriteRequest, bufSize),
		log:      log,
		metrics:  m,
	}
}

// Submit enqueues a write request and returns a channel that will receive
// exactly one error value (nil on success). The caller must consume the
// returned channel (or select on ctx.Done) to avoid goroutine leaks.
//
// Submit blocks when the internal buffer is full, providing back-pressure
// to the calling processor. If ctx is cancelled before the request is
// enqueued, the returned channel contains ctx.Err() immediately.
func (w *Writer) Submit(ctx context.Context, req *WriteRequest) <-chan error {
	if req == nil {
		ch := make(chan error, 1)
		ch <- errors.New("batchwriter: nil WriteRequest")
		close(ch)
		return ch
	}
	req.done = make(chan error, 1)

	select {
	case w.requests <- req:
		return req.done
	case <-ctx.Done():
		req.done <- ctx.Err()
		close(req.done)
		return req.done
	}
}

// Start runs a single dispatcher goroutine until ctx is cancelled. The
// dispatcher accumulates up to MaxBlocks requests or flushes on
// FlushTimeout; when a batch is ready it blocks until a flush slot is
// available (semaphore size Workers), then starts a goroutine that performs
// the ClickHouse batch insert and signals each request. In-flight flushes
// are waited on before draining the request channel.
func (w *Writer) Start(ctx context.Context) error {
	workers := w.cfg.Workers
	if workers < 1 {
		workers = 1
	}

	sem := semaphore.NewWeighted(int64(workers))
	var inflight sync.WaitGroup
	w.runDispatcher(ctx, sem, &inflight)

	inflight.Wait()
	w.drainRemaining()

	return nil
}

// runDispatcher is the main loop: read requests, accumulate, flush on max
// size or timeout by spawning async flushes gated by the semaphore.
func (w *Writer) runDispatcher(ctx context.Context, sem *semaphore.Weighted, inflight *sync.WaitGroup) {
	maxBlocks := w.cfg.MaxBlocks
	if maxBlocks < 1 {
		maxBlocks = 1
	}

	pending := make([]*WriteRequest, 0, maxBlocks)

	timer := time.NewTimer(w.cfg.FlushTimeout)
	stopAndDrainTimer(timer)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Infof("context cancelled, draining %d requests", len(pending))
			signalAll(pending, ctx.Err())
			return

		case req := <-w.requests:
			if req == nil {
				continue
			}
			pending = append(pending, req)
			if len(pending) == 1 {
				stopAndDrainTimer(timer)
				timer.Reset(w.cfg.FlushTimeout)
			}
			if len(pending) >= maxBlocks {
				w.log.Infof("max blocks reached, flushing %d requests", len(pending))
				stopAndDrainTimer(timer)
				batch := takeBatch(&pending)
				w.spawnFlush(ctx, sem, inflight, batch)
			}

		case <-timer.C:
			if len(pending) == 0 {
				w.log.Debugf("no requests to flush on timeout")
				continue
			}
			w.log.Infof("flush timeout reached, flushing %d requests", len(pending))
			stopAndDrainTimer(timer)
			batch := takeBatch(&pending)
			w.spawnFlush(ctx, sem, inflight, batch)
		}
	}
}

// takeBatch moves pending into a new slice so in-flight flushes do not share
// the backing array with pending (append would otherwise overwrite batch slots).
func takeBatch(pending *[]*WriteRequest) []*WriteRequest {
	if len(*pending) == 0 {
		return nil
	}
	batch := append([]*WriteRequest(nil), *pending...)
	*pending = (*pending)[:0]
	return batch
}

// spawnFlush waits for a semaphore slot or context cancellation, then runs
// flush in a new goroutine and signals all requests in the batch.
func (w *Writer) spawnFlush(ctx context.Context, sem *semaphore.Weighted, inflight *sync.WaitGroup, batch []*WriteRequest) {
	if len(batch) == 0 {
		return
	}
	err := sem.Acquire(ctx, 1)
	if err != nil {
		signalAll(batch, err)
		return
	}

	inflight.Add(1)

	go func(batch []*WriteRequest) {
		defer inflight.Done()
		defer sem.Release(1)

		err := w.flush(ctx, batch)
		signalAll(batch, err)
	}(batch)
}

// stopAndDrainTimer stops t and drains its channel if Stop did not succeed.
func stopAndDrainTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

// signalAll sends err on each request's done channel and closes it.
func signalAll(requests []*WriteRequest, err error) {
	for _, req := range requests {
		if req.done != nil {
			req.done <- err
			close(req.done)
		}
	}
}

// flush collects rows from all requests and batch-inserts them into ClickHouse.
// Blocks, transactions, logs, and internal transactions are inserted
// concurrently via an errgroup.
func (w *Writer) flush(ctx context.Context, requests []*WriteRequest) error {
	if len(requests) == 0 {
		return nil
	}

	var (
		blocks  []*evmrepo.BlockRow
		txs     []*evmrepo.TransactionRow
		logs    []*evmrepo.LogRow
		intTxns []*evmrepo.InternalTransactionRow
	)

	for _, req := range requests {
		if req.Block != nil {
			blocks = append(blocks, req.Block)
		}
		txs = append(txs, req.Transactions...)
		logs = append(logs, req.Logs...)
		intTxns = append(intTxns, req.InternalTxns...)
	}

	g, gctx := errgroup.WithContext(ctx)

	if len(blocks) > 0 && w.repos.Blocks != nil {
		g.Go(func() error {
			start := time.Now()
			err := w.repos.Blocks.BatchInsertBlocks(gctx, blocks)
			w.metrics.RecordClickHouseWrite(clickhouse.DefaultRawBlocksTableName, err, time.Since(start).Seconds())
			if err != nil {
				return fmt.Errorf("batch insert blocks (%d rows): %w", len(blocks), err)
			}
			return nil
		})
	}

	if len(txs) > 0 && w.repos.Transactions != nil {
		g.Go(func() error {
			start := time.Now()
			err := w.repos.Transactions.BatchInsertTransactions(gctx, txs)
			w.metrics.RecordClickHouseWrite(clickhouse.DefaultRawTransactionsTableName, err, time.Since(start).Seconds())
			if err != nil {
				return fmt.Errorf("batch insert transactions (%d rows): %w", len(txs), err)
			}
			return nil
		})
	}

	if len(logs) > 0 && w.repos.Logs != nil {
		g.Go(func() error {
			start := time.Now()
			err := w.repos.Logs.BatchInsertLogs(gctx, logs)
			w.metrics.RecordClickHouseWrite(clickhouse.DefaultRawLogsTableName, err, time.Since(start).Seconds())
			if err != nil {
				return fmt.Errorf("batch insert logs (%d rows): %w", len(logs), err)
			}
			return nil
		})
	}

	if len(intTxns) > 0 && w.repos.InternalTransactions != nil {
		g.Go(func() error {
			start := time.Now()
			err := w.repos.InternalTransactions.BatchInsertInternalTransactions(gctx, intTxns)
			w.metrics.RecordClickHouseWrite(clickhouse.DefaultRawInternalTransactionsTableName, err, time.Since(start).Seconds())
			if err != nil {
				return fmt.Errorf("batch insert internal transactions (%d rows): %w", len(intTxns), err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		w.log.Errorw("batch flush failed",
			"error", err,
			"blocks", len(blocks),
			"transactions", len(txs),
			"logs", len(logs),
			"internalTransactions", len(intTxns),
		)
		return err
	}

	w.log.Infow("batch flush completed",
		"blocks", len(blocks),
		"transactions", len(txs),
		"logs", len(logs),
		"internalTransactions", len(intTxns),
	)
	return nil
}

// drainRemaining signals context.Canceled to any requests still sitting in
// the channel after the dispatcher exits.
func (w *Writer) drainRemaining() {
	for {
		select {
		case req := <-w.requests:
			if req != nil && req.done != nil {
				req.done <- context.Canceled
				close(req.done)
			}
		default:
			return
		}
	}
}
