// Package slidingwindow implements a concurrent scheduler that processes blocks
// within a sliding window of heights. It is designed for indexers that need to
// catch up historical gaps (backfill) while also ingesting new blocks (realtime)
// under bounded concurrency and without duplicating work.
//
// Terminology
//   - LUB (Lowest Unprocessed Block): the lowest height in the window that is not
//     yet fully processed.
//   - HIB (Highest Ingested Block): the highest height known/ingested so far.
//     The active window is [LUB..HIB], inclusive, and the invariant HIB >= LUB holds.
//
// Main components
//   - Manager: the scheduler and coordinator. It continually looks for the next
//     unprocessed height in the window and dispatches work while respecting bounded
//     concurrency and a configurable backfill priority relative to realtime work.
//     It prevents duplicate processing by tracking in‑flight heights and accounts
//     per‑height failures, stopping the run when a threshold is exceeded. Realtime
//     headers are accepted via a channel and processed with low latency when capacity
//     is available; otherwise they are picked up by backfill scanning.
//   - Worker: the user‑provided unit of work that processes a single block height
//     given a context. The manager invokes the worker and reacts to success (marking
//     and advancing the window) or failure (incrementing failure counts and enforcing
//     thresholds).
//   - Cache: a thread‑safe in‑memory store of the sliding window state. It maintains
//     LUB and HIB watermarks and which heights within the window are already processed,
//     and it advances the LUB when contiguous processed heights allow the window to slide.
//     The cache preserves invariants and is safe for concurrent access by the manager.
//
// Scheduling strategy
//   - Backfill (aggressive fill):
//     The manager scans [LUB..HIB] to find the next height that is not processed
//     and not inflight. If capacity is available (both backfillSem and workerSem),
//     it dispatches a worker and repeats until capacity or work is exhausted.
//   - Realtime:
//     On a new header, the manager ensures HIB >= header height, then attempts to
//     acquire only a worker slot (realtime does not consume backfill priority).
//     If no worker capacity is available, the event is dropped; backfill will pick
//     it up from the window.
//
// Success and failure handling
//   - On worker success, the manager:
//   - Marks the height processed in the Cache, then calls AdvanceLUB() to slide
//     the window forward if contiguous.
//   - Resets the failure counter for the height.
//   - On worker failure, the manager:
//   - Increments the height’s failure count. When it reaches maxFailures,
//     the manager sends the height to failureChan and Run returns an error
//     (ErrMaxFailuresExceeded), cancelling its internal context so workers exit.
//   - In all cases, workers release any acquired semaphores, clear the inflight mark,
//     and signal workReady to prompt the scheduler.
//
// Usage
//  1. Construct a Cache with initial LUB/HIB.
//  2. Construct a Manager with NewManager(logger, cache, worker, concurrency, backfillPriority, blocksChCapacity, maxFailures).
//  3. Start Run(ctx) in a goroutine.
//  4. Send realtime headers into BlockChan() as they arrive.
//  5. Cancel ctx to stop Run; it will return when shutdown completes or when the failure threshold is exceeded.
package slidingwindow
