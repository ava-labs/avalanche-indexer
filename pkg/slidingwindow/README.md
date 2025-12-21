## Sliding Window Scheduler (slidingwindow)

A concurrent scheduler that processes block heights within a sliding window. It is designed for indexers that must both backfill historical gaps and ingest new heights in realtime under bounded concurrency, without duplicating work and maximum thread utilization.

### Terminology
- **lowest**: the lowest unprocessed height in the window.
- **highest**: the highest unprocessed height. The active window is `[lowest..highest]`, inclusive, and the invariant `highest >= lowest` holds.

### Main Components
- **Manager**: coordinator and scheduler.
  - Finds the next unprocessed height in `[lowest..highest]`.
  - Dispatches work respecting bounded concurrency and a configurable backfill priority relative to realtime work.
  - Tracks per‑height failures and stops when a threshold is exceeded.
  - Realtime heights are accepted via a method and processed with low latency when capacity is available; otherwise backfill scanning picks them up.
- **Worker**: user‑provided unit of work that processes a single height with a context. The Manager reacts to success (mark + advance) or failure (increment failure count, enforce threshold).
- **State**: thread‑safe in‑memory store of the sliding window. Maintains `lowest/highest`, processed heights, advances `lowest` when contiguous, and preserves invariants for concurrent access, tracks inflight heights to preent duplicate work.
- **Subscriber**: integrates with a chain client (e.g., Coreth) to receive new heads and forward their heights to the Manager via `SubmitHeight`.

### Scheduling Strategy
- **Backfill (aggressive fill)**:
  - Scan `[lowest..highest]` to find the next height that is not processed and not inflight.
  - If both backfill and worker capacity are available, dispatch a worker and repeat until capacity or work is exhausted.
- **Realtime**:
  - On a new height, ensure `highest >= height`.
  - Try to acquire only a worker slot (realtime does not consume backfill priority).
  - If no worker capacity, the event is dropped; backfill picks it up from the window.

### Success and Failure Handling
- On worker success:
  - Mark the height processed in the State.
  - Call `AdvanceLowest()` to slide the window forward if contiguous.
  - Reset the failure counter for the height.
- On worker failure:
  - Increment the height’s failure count.
  - When it reaches `maxFailures`, the Manager sends the height to a failure channel and `Run` returns an error, cancelling its internal context so workers exit.
- In all cases, workers release acquired semaphores, clear the inflight mark, and signal `workReady` to prompt the scheduler.

### Usage
1. Construct a State with initial `lowest/highest`.
2. Construct a Manager with `NewManager(logger, state, worker, concurrency, backfillPriority, heightChanCapacity, maxFailures)`.
3. Start `Run(ctx)` in a goroutine.
4. Submit realtime heights via `SubmitHeight(h)` as they arrive.
5. Cancel `ctx` to stop `Run`; it returns when shutdown completes or when the failure threshold is exceeded.

### Subscriber
- Purpose: listen for new chain heads and submit their heights to the Manager.
- Reference implementation: `pkg/slidingwindow/subscriber/coreth.go`:
  - `Subscribe(ctx, capacity, manager)` — BLOCKING; subscribes to Coreth new heads and forwards `header.Number.Uint64()` to `manager.SubmitHeight`.
  - Returns on subscribe failure, subscription error, or when `ctx` is done (it unsubscribes on exit).
- Backpressure and drops:
  - If the Manager’s height channel is full, `SubmitHeight` returns false but raises `highest` first so backfill can pick the height up later.
  - Duplicate work is avoided by the Manager’s `inflight` gating in `State`.
- Typical usage:
  - Create the subscriber with a chain client, start `manager.Run(ctx)`, then call `subscriber.Subscribe(ctx, capacity, manager)` in another goroutine.

### Notes
- Realtime submission is non‑blocking: if the height channel is full, the Manager ensures `highest` covers the submitted height so backfill can pick it up.
- Backfill priority is strictly less than concurrency to guarantee that realtime tasks always have the opportunity to acquire worker capacity.

