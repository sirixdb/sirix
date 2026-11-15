# Async commit failure and recovery

Decision: “A, but with a clear design of course. Why does the cleanup for instance fail? It should simply be a truncation once a \".commit\" file is in the resource”.

R4 is a shared completion-boundary defect. The implemented slot, failure and teardown contracts are owned by [the async commit design](../../docs/ASYNC_COMMIT_DESIGN.md#failure-and-teardown-ownership).

R5 is an ownership/recovery abstraction defect: closed means unusable, not recovered. After fencing workers, bound and unbound writers release independent in-memory ownership even when cleanup fails. Recovery is exclusively the existing marker-driven admission barrier. The same contract covers public clean close, explicit session abort, failed rollback and both node/page writer admissions, for XML and JSON. Public dirty close remains refused.

Truncation repairs persisted state. Successor construction, task submission, catalogue serialization, data/beacon writes, cache invalidation, listener/reader/log/backend close and marker I/O can independently fail; none warrants a second disk-repair algorithm. The admission contract and marker-creation failure handling are owned by [marker recovery](../../docs/operations.md#marker-recovery).

For the final teardown boundary, see [ResourceSession.close()](../../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java) and the [async ownership design](../../docs/ASYNC_COMMIT_DESIGN.md#failure-and-teardown-ownership). These supersede the initial proposed cleanup order: scheduler shutdown follows node cleanup, and its termination fence precedes write-permit release outside the node lock.

Successful hardening work and record/page loops retain their ordering. Recovery adds I/O only when the marker/guard is present. Failure-only marker retention and slot completion share boundaries; no retries or second disk recovery subsystem is added.

## Executable specification / independent oracles

- Construction or submission plus hardening failure: first injected error identity, suppressed hardening error, async permit count exactly one, repeated await retains cause; session abort frees the shared permit and scheduler in XML/JSON.
- Failed prewrite teardown: marker exists despite terminal writer state; a second user opens only after truncate/catalogue/cache recovery, sees the recorded durable value, commits a new value and reads it back. Bound/unbound owners obey the same barrier.
- Recovery fault at truncate/catalogue/cache stage: no reader construction or writer returned, marker-naming error and permit returned; clearing the fault permits recovery. Marker creation failure cannot produce a clean admission after the first session closes.
- Durable boundary: pre-beacon failure keeps revision 1; post-beacon cleanup failure keeps revision 2. An appended garbage tail is removed at admission, not teardown. Sibling resource caches remain usable.
- Existing real paused-worker tests cover R2/R3 and public dirty close; lifecycle iterator/axis tests remain unchanged.

This review phase runs one focused core verification round after all edits. Scoped PIT, work budgets, compiled-shape comparison and broader validation belong to the outer executor; no historical result is claimed for this revision.

Rollback ownership checks precede failure recovery: calling rollback on a closed node must reject without creating a marker or changing a successor writer's admission state. A failed rollback must reject mutations/commit until a successful rollback reconstructs the epoch. Existing rollback-only state provides that guard. The bound prewrite fault test uses the production KEEP_OPEN_ASYNC_FLUSH mode, whose epochs append to the reclaimable data tail rather than the default spill file.
