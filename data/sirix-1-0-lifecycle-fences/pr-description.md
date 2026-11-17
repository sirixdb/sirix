# Fence lifecycle workers and enforce callback ownership

Failed/interrupted parallel work could return while workers still used owner state; teardown could skip independent owners, and bounded REST iteration did not close nested prefetch producers. Async failure and callback reentrancy could also strand writer admission or permit unsafe offset reuse.

Worker fences now cover partial starts, interruption, reset and fallback. Iterator/stream/composite-axis closure reaches nested concurrent producers. Session/cache teardown preserves the first failure while attempting independent cleanup. Failed writers leave marker-driven truncation and resource-cache invalidation to the next exclusive admission; public dirty close remains protected. Node/page admission waits outside the session monitor and revalidates before construction.

Callback behavior is defined by [PreCommitHook](../../bundles/sirix-core/src/main/java/io/sirix/api/PreCommitHook.java), [PostCommitHook](../../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java) and [revision listeners](../../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/InternalResourceSession.java). The [ownership design](../../docs/ASYNC_COMMIT_DESIGN.md#failure-and-teardown-ownership) owns the notification, reader-retirement and truncation boundaries; the current specification and evidence follow.

## Callback and resource lifecycle contract

[Database hooks](../../docs/hooks.md) defines synchronous pre-publication veto hooks and asynchronous
post-hooks/listeners on a dedicated database dispatcher. Delivery uses exact committed read views
and remains independent of writer/successor progress. Same-database lifecycle reentry rejects before
locks, including public handles, sessions, writer/page/facade operations, manager and shutdown.

Close and destructive history changes use a configurable notification deadline. Unopened queued
work may be dropped with a warning; active readers/backend/file ownership remain reachable on
explicit timeout and can be closed on retry. Resource creation and retained removal establish
mutation-gate ownership, then quiescence before admission monitors. Session registration stays serialized;
newly opened derived setup/rebuild finishes after those monitors are released. Warm session opens
skip eager setup. Removal rejects registered owners before waiting and revalidates under the monitor.
Callbacks retain their permitted independent metadata reads and existing-session opens while quiescence is pending.

Notification retirement now shares an admission pause across overlapping lifecycle owners instead
of exclusively locking them. Execution and history ownership remain with their existing owners;
callback waits stay outside admission monitors. Direct local close stops resource admission after
quiescence, drains derived workers and registered user sessions outside its monitor, and releases
shared storage only afterward. Failed owner cleanup retains admission closure and storage for retry.

Raw and node page truncation share node execution/hardening ownership, revalidate captured writers,
and fence submitted async append work before changing files or snapshots. Closed predecessors and
interrupted maintenance reject before destructive work. A temporary pre-hook admission refusal
skips only that scheduled tick; user-hook exceptions and Errors retain their veto behavior.

Notification jobs now carry resource/revision identity. Shared destructive page admission refuses
before mutation if a deferred publication would lose its original history; unrelated resources and
retained revisions remain admissible. The single capacity budget covers active, executor-queued
and deferred work (default 1,024, configured before database open with `sirix.notifications.capacity`).
Overflow drops new work without waiting for consumers, running callbacks on committers or vetoing
durable commits. Delivery is best effort; hooks expose their view's revision and listeners receive
that revision. Repeated publication observations remain possible. Resume/replay is not implemented,
and revision numbers alone are not durable replay cursors across truncation/reissue.

## Specification and executable evidence

| Requirement | Independent observable checks |
| --- | --- |
| Started workers stop before release/reset/fallback | ConcurrentAxisLifecycleTest, ProjectionDirectoryWorkerFenceTest, DirectPageScannerLifecycleTest, WorkerTerminationTest; prior fixes retained |
| Real iterator/composite close propagation | QuerySerializerLifecycleTest; partial traversal reaches real nested producers; prior fix retained |
| Failure, recovery and independent owner teardown | NodeTransactionAbortLifecycleTest, AbstractResourceSessionWriteFailureCleanupTest, AsyncFlushFailurePathTest, WriterConstructionFailureCleanupTest; original error/suppression, physical fences, exact admission release |
| Callback rejection and the external handle-close cycle | DatabaseNotificationLifecycleTest; JSON/XML pre/post/listener calls through every lifecycle wrapper reject without partial teardown |
| Bounded timeout, retained reads and safe retry | DatabaseNotificationLifecycleTest and SharedCallbackLifetimeTest; live navigation after refusal, dropped queued followers, retry and interruption |
| R13 overlap and immutable revision views | asyncDurabilityAndBlockedDeliveryDoNotBlockSuccessorOrWriterClose, asyncPostHookNavigationCannotMoveTheCountOnlyWriterCursor; known revision/node values with held durability and delivery barriers |
| Creation/removal and permitted metadata reads | resourceLifecycleQuiescesBeforeMetadataMonitors; JSON/XML × direct/handle × creation/removal × pre/post/listener; actual metadata/navigation and resource state |
| Refusal before resource mutation | resourceLifecycleRefusalPreservesFilesAndOwnersForRetry; timeout/interruption leaves files and retained owner usable, then retry succeeds |
| Implicit derived lifecycle and ordinary opens | eagerDerivedRebuildQuiescesOutsideSessionAdmission, warmSessionOpenDoesNotEnterNotificationRetirement, DerivedIndexMaintainerTest; genuine hidden rebuild, usable snapshots and no warm-open callback wait |
| Registration lifetime | delayedSessionReleaseStaysRegisteredUntilItReleasesItsBackend; early rejection while the backend still belongs to the previous generation, then recreation and writer reuse |
| Deferred publication identity | deferredPublicationPreventsHistoryReuse; 24 JSON/XML hook/listener cases through node, bound and unbound page entry, including interruption, unchanged data on refusal, original delivery, retry and revision reuse |
| Bounded non-blocking admission | saturatedNotificationsDoNotBlockDurableCommits; 8 cases with latched active/deferred delivery, successful durable commits, bounded physical/counted retention, ordered admitted views and closed readers; mixed-backlog, abandonment and resource/revision selection unit cases |
| Physical storage maintenance fence | rawPageTruncationFencesBoundHardening, truncationFencesAsyncAppend, nodeTruncationCanTargetTheRevisionBeingHardened; JSON/XML raw/node/unbound entry, success/failure/interruption, closed predecessors and readable durable revisions |
| Scheduled publication after temporary refusal | timedCommitResumesAfterTemporaryAdmissionRefusal; a real other-handle close owns retirement while blocked on a different resource writer, then a later periodic tick publishes the known value |
| Ordinary ownership overlap | executionOwnerCanQuiesceWhileCloseWaitsForExecution, overlappingRetirementsKeepAdmissionPausedUntilBothOwnersLeave, historyOwnerCanTruncateWhileDerivedCleanupWaits; no active callback needed to exercise execution/history/cleanup ordering |
| Local retained owners and safe retry | refusedNodeExecutionRetainsItsPageWriterUntilRetry; reader/page ownership survives failed local/session close, new resource admission stays closed, metadata reads and retry remain safe |

Current focused results cover **1,496 selected cases with 0 final failures/errors and 5 configured
skips**, plus **69 auxiliary cases with 0 failures/errors and 2 skips**. The full selection passed
all other suites; the notification suite was retried after correcting a new test's unrequested
exactly-once listener assumption. All **325 notification cases** then passed, including all 35 new
cases. Production sources did not change between those runs. Existing callback metadata, held-gate,
worker ownership, scheduled commit, R13 overlap and core/query/MCP budget checks remain green
without wider bounds.

Five independently compiled starting-HEAD lifecycle classes fail all **32 engine counterfactual
cases**: deferred history reuse is not refused and saturation retains 8 or more jobs despite a
configured bound of 3. These expected failures are separate from passing checks. The final listener
oracle verifies every admitted original job against its original value, and requires ordered
admitted delivery while preserving existing repeated publication observations.

Six selected hot methods have identical compiled instructions and exception tables against
independently compiled starting HEAD `320ce6ab`, with constant-pool indices normalized. Cold
notification dispatch and destructive admission intentionally differ. This comparison is not
against the new review base `ce56286b`, nor does it establish runtime/native equivalence. Earlier
shape, mutation and counterfactual artifacts remain historical for their recorded source hashes.
No benchmark was run. Exact reports, skips, source hashes and limits are in
[current verification](deferred-notification-verification.json).

Root cause and learning: [deferred-publication postmortem](../../docs/postmortems/deferred-notification-history.md),
[storage/retirement ownership postmortem](../../docs/postmortems/storage-maintenance-retirement-ownership.md),
[mutation-gate postmortem](../../docs/postmortems/mutation-retirement-lock-order.md),
[monitor-order postmortem](../../docs/postmortems/bootstrap-notification-monitor-order.md),
[dispatcher postmortem](../../docs/postmortems/database-notification-dispatch.md) and
[Lessons](../../docs/LESSONS.md). Fresh scoped PIT, repository-wide gates, post-custody probes and
delivery remain with the outer executor; no historical mutation count validates the current fix.
