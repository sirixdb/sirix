# Async commit failure and recovery

Decision: “A, but with a clear design of course. Why does the cleanup for instance fail? It should simply be a truncation once a \".commit\" file is in the resource”.

R4 is a shared completion-boundary defect. The implemented slot, failure and teardown contracts are owned by [the async commit design](../../docs/ASYNC_COMMIT_DESIGN.md#failure-and-teardown-ownership).

R5 is an ownership/recovery abstraction defect: closed means unusable, not recovered. After fencing workers, bound and unbound writers release independent in-memory ownership even when cleanup fails. Recovery is exclusively the existing marker-driven admission barrier. The same contract covers public clean close, explicit session abort, failed rollback and both node/page writer admissions, for XML and JSON. Public dirty close remains refused.

Truncation repairs persisted state. Successor construction, task submission, catalogue serialization, data/beacon writes, cache invalidation, listener/reader/log/backend close and marker I/O can independently fail; none warrants a second disk-repair algorithm. The admission contract and marker-creation failure handling are owned by [marker recovery](../../docs/operations.md#marker-recovery).

For the final teardown boundary, see [ResourceSession.close()](../../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java) and the [async ownership design](../../docs/ASYNC_COMMIT_DESIGN.md#failure-and-teardown-ownership). These supersede the initial proposed cleanup order: scheduler shutdown follows node cleanup; external transaction close fences termination before write-permit release outside the node lock. Scheduler-initiated close instead returns through the safe callback epilogue, as specified in R6 below.

Successful hardening work and record/page loops retain their ordering. Recovery adds I/O only when the marker/guard is present. Failure-only marker retention and slot completion share boundaries; no retries or second disk recovery subsystem is added.

## Executable specification / independent oracles

- Construction or submission plus hardening failure: first injected error identity, suppressed hardening error, async permit count exactly one, repeated await retains cause; session abort frees the shared permit and scheduler in XML/JSON.
- Failed prewrite teardown: marker exists despite terminal writer state; a second user opens only after truncate/catalogue/cache recovery, sees the recorded durable value, commits a new value and reads it back. Bound/unbound owners obey the same barrier.
- Recovery fault at truncate/catalogue/cache stage: no reader construction or writer returned, marker-naming error and permit returned; clearing the fault permits recovery. Marker creation failure cannot produce a clean admission after the first session closes.
- Durable boundary: pre-beacon failure keeps revision 1; post-beacon cleanup failure keeps revision 2. An appended garbage tail is removed at admission, not teardown. Sibling resource caches remain usable.
- Existing real paused-worker tests cover R2/R3 and public dirty close; lifecycle iterator/axis tests remain unchanged.

This review phase runs one focused core verification round after all edits. Scoped PIT, work budgets, compiled-shape comparison and broader validation belong to the outer executor; no historical result is claimed for this revision.

Rollback ownership checks precede failure recovery: calling rollback on a closed node must reject without creating a marker or changing a successor writer's admission state. A failed rollback must reject mutations/commit until a successful rollback reconstructs the epoch. Existing rollback-only state provides that guard. The bound prewrite fault test uses the production KEEP_OPEN_ASYNC_FLUSH mode, whose epochs append to the reclaimable data tail rather than the default spill file.

## R6: scheduler-initiated teardown

A termination fence may only be performed by a thread outside the executor being fenced. Both XML and JSON timed transactions may close themselves, or their session, from a public post-commit hook. Such close completes node cleanup, publishes CLOSED, shuts down scheduling and releases admission exactly once without waiting for its own thread. Once a hook closes the transaction, neither synchronous nor asynchronous publication dispatches another hook. The timed callback then only returns through the lock-release/executor epilogue; it must never revisit readers, writers or epoch state.

Remember the scheduler worker's identity at thread creation, not by thread name. External close still waits for termination, including a repeated close after scheduler-initiated teardown. Do not hold the transaction monitor during that fence or while waiting for a timed transaction's lock: a scheduler hook can need to enter close concurrently. Timed cleanup uses its existing transaction lock; untimed cleanup remains serialized by its monitor. The existing admission-release guard becomes atomic so independent closers cannot release twice.

Regressions use real timed XML/JSON transactions, latches and a queued external closer. They prove transaction-close and session-close hooks return, another user reads the committed value and writes while the old hook is paused, no later hook runs on closed state, and external close returns only with the executor terminated. Bounded completion waits are deadlock guards, not performance assertions. Successful ordinary hooks retain their order; count-based asynchronous publication follows the same close-stops-hooks rule.
