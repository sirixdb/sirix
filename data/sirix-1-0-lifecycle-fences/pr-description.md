# Fence lifecycle workers and enforce callback ownership

Failed/interrupted parallel work could return while workers still used owner state; teardown could skip independent owners, and bounded REST iteration did not close nested prefetch producers. Async failure and callback reentrancy could also strand writer admission or permit unsafe offset reuse.

Worker fences now cover partial starts, interruption, reset and fallback. Iterator/stream/composite-axis closure reaches nested concurrent producers. Session/cache teardown preserves the first failure while attempting independent cleanup. Failed writers leave marker-driven truncation and resource-cache invalidation to the next exclusive admission; public dirty close remains protected. Node/page admission waits outside the session monitor and revalidates before construction.

Callback behavior is defined by [PreCommitHook](../../bundles/sirix-core/src/main/java/io/sirix/api/PreCommitHook.java), [PostCommitHook](../../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java) and [revision listeners](../../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/InternalResourceSession.java). The [ownership design](../../docs/ASYNC_COMMIT_DESIGN.md#failure-and-teardown-ownership) owns the notification, reader-retirement and truncation boundaries; the current specification and evidence follow.

## Current callback contract

[Database hooks](../../docs/hooks.md) supersedes the earlier synchronous callback fence design.
Pre-hooks remain synchronous vetoes. Durable post-hooks and revision listeners use a dedicated
per-database dispatcher with exact committed read views and ordered delivery. Same-database lifecycle
calls reject before admission locks, including nested callbacks and public database handles. Commit
and successor mutation never wait for notification delivery. Close/truncation have a configurable
notification deadline: unopened queued work can be dropped with a warning; active readers/storage
remain owned on explicit timeout and can be closed on retry.

## Specification and executable evidence

| Requirement | Executable oracle |
| --- | --- |
| Worker termination and iterator close propagation | ConcurrentAxisLifecycleTest, ProjectionDirectoryWorkerFenceTest, DirectPageScannerLifecycleTest, QuerySerializerLifecycleTest; original lifecycle fixes retained |
| Recovery and independent owner cleanup | NodeTransactionAbortLifecycleTest, AbstractResourceSessionWriteFailureCleanupTest, AsyncFlushFailurePathTest, WriterConstructionFailureCleanupTest; first error, suppression, physical worker fencing and permit ownership |
| Pre/post/listener rejection at all public wrappers | DatabaseNotificationLifecycleTest.allLifecycleEntryPointsRejectCallbackReentry and closingHandleRejectsCallbackBeforeItsClosingWait; JSON/XML immediate IllegalStateException and usable owner |
| Bounded close, queue loss and safe retry | unreturnedDeliveryRetainsReadersDropsQueueAndAllowsRetry, blockedPreHookCannotPinAnExternalCloserForever, refusedNodeExecutionRetainsItsPageWriterUntilRetry; precise timeout, live cursor navigation, retained registry and final release |
| Durability, immutable views and R13 overlap | asyncDurabilityAndBlockedDeliveryDoNotBlockSuccessorOrWriterClose, asyncPostHookNavigationCannotMoveTheCountOnlyWriterCursor; known per-revision values, independent node cursors, setter/close completion while hook is blocked |
| Every publication path | inlineHardeningFallbackStillDeliversDurableHooks, notificationViewUsesDurableRootWhilePendingCleanupIsBlocked; original setup failure and data readable after predecessor close |
| Hook errors, scope and ordering | preHookVetoRemainsSynchronousAndRetryable, notificationErrorCannotVetoDurabilityOrSkipTheNextView, anotherDatabaseRemainsIndependentDuringCallbacks; exception identity, later views and nested database independence |
| Pre-hook readers and destructive history | SharedCallbackLifetimeTest; direct/shared/page readers, interruption, independent-close failure, distant pinned pages versus truncation |

Root cause and learning: [database notification postmortem](../../docs/postmortems/database-notification-dispatch.md)
and [Lessons](../../docs/LESSONS.md). Earlier postmortems and verification artifacts are historical
and do not validate the changed callback contract. No benchmark is authorized for this round.
Fresh focused counts, work budgets, compiled caller comparison and limitations are recorded in
notification-dispatch-verification.json. Scoped mutation and broader delivery gates remain owned by
the outer executor; no old mutation or benchmark result is claimed as fresh evidence.

Fresh result: **1,207 selected cases, 0 failures/errors, 5 configured skips**, plus **69 auxiliary
cases, 0 failures/errors, 2 skips**. All documented core/query/MCP work-budget selections passed;
all 188 new notification lifecycle cases passed. The six selected hot caller instruction and
exception-table comparisons match independently compiled starting HEAD `fc8b9071`; normal lock/unlock
now inherits ReentrantLock, and cold commit/retirement paths intentionally changed. No native-code
or latency equivalence is claimed. Exact suites, source hashes, diagnostic attempts, skips and
limitations are in [verification](notification-dispatch-verification.json) and
[compiled comparison](notification-dispatch-shape.json). Full lint/PIT/CI remain outer gates.
