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
Callbacks retain their permitted independent metadata reads and existing-session opens.

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

Fresh result: **1,415 selected cases, 0 failures/errors, 5 configured skips**, plus **69 auxiliary
cases, 0 failures/errors, 2 skips**. All 278 database-notification cases, 69 derived-maintainer cases,
49 query gate/store cases and documented core/query/MCP budgets passed without wider bounds.
Independently compiled starting-HEAD classes failed all 22 held-outer-gate cases with retirement
timeouts; the same 22 pass on the fix. Earlier bootstrap evidence separately reproduces the original
metadata cycle in 24 cases. Expected baseline failures are separate from passing checks.

The five hot class files remain byte-identical to the snapshot used for the earlier six compiled
caller comparisons against `fc8b9071`. That baseline is not original main, and bytecode identity is
not runtime equivalence. The operator's separate fifteen-region proof against actual base `ba000071`
at `ffba256a` remains external evidence, not a fresh benchmark claim. No benchmark was run. Exact
suites, skips, hashes and limits are in [current verification](mutation-retirement-order-verification.json).

Root cause and learning: [mutation-gate postmortem](../../docs/postmortems/mutation-retirement-lock-order.md),
[monitor-order postmortem](../../docs/postmortems/bootstrap-notification-monitor-order.md),
[dispatcher postmortem](../../docs/postmortems/database-notification-dispatch.md) and
[Lessons](../../docs/LESSONS.md). Older artifacts are historical snapshots. Scoped PIT, full lint,
broader test/CI and delivery phases remain with the outer executor; no historical mutation count is
claimed as current evidence.

Mutation-gate ordering also covers existing JSON/XML collection/store callers: gates precede
notification retirement, and callback mutation rejects at the shared gate entry before any wait.
Held-outer-gate regressions cover creation, removal and guarded cleanup; the metadata-read matrix
remains mandatory. See [lock-order postmortem](../../docs/postmortems/mutation-retirement-lock-order.md).
The 1,320-case bootstrap snapshot remains historical evidence for its recorded source hashes.
