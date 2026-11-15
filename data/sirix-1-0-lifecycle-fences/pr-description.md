# Fence lifecycle workers and enforce callback ownership

Failed/interrupted parallel work could return while workers still used owner state; teardown could skip independent owners, and bounded REST iteration did not close nested prefetch producers. Async failure and callback reentrancy could also strand writer admission or permit unsafe offset reuse.

Worker fences now cover partial starts, interruption, reset and fallback. Iterator/stream/composite-axis closure reaches nested concurrent producers. Session/cache teardown preserves the first failure while attempting independent cleanup. Failed writers leave marker-driven truncation and resource-cache invalidation to the next exclusive admission; public dirty close remains protected. Node/page admission waits outside the session monitor and revalidates before construction.

The clarified callback contract keeps KEEP_OPEN's lifetime permit P and pre-hook veto timing. One session context rejects commit, rollback/abort and node/session close from pre/post hooks and revision listeners before locking. Post-publication work is queued before commit ownership ends and runs after outermost transaction execution T and the scheduled Runnable boundary. Its borrowed current transaction view can include a subsequent mutation of the enclosing operation; it is not a revision snapshot. A callback phase protects payload lifetime while T is released. Deferred notification errors are accumulated/logged after phase restoration; they cannot replace an execution failure or veto a durable revision. Pre-hook exceptions still propagate before publication.

## TXN requirements and lock order

- TXN-CB-1: Every pre/post/listener dispatch sets the same session callback context, on its actual foreground/scheduler/async thread. Lifecycle entry rejects that context before any lock.
- TXN-CB-2: Post-publication delivery follows actual outermost T release and scheduled Runnable return. P remains owned; competing execution cannot retire a callback's borrowed payload. Async notifications wait for hardening-slot release outside T.
- TXN-CB-3: Close rejects an enclosing T acquisition and never waits for scheduler termination while holding T. No scheduler-thread close exception remains.
- TXN-CB-4: Revert and truncate fence pending hardening before lineage replacement or truncation, preserving terminal failure causes.
- TXN-ADMIT: Acquire P outside the session monitor, revalidate/register under it, and release failed admission in finally. Session close drops its snapshot monitor before node execution/fences; owner removal and P release remain atomic under a short monitor section.

Execution T may use short session bookkeeping sections. Callback dispatch drops T while reserving its borrow phase; lifecycle attempts from that phase reject before either lock. External close fences that phase, cleans up, drops T, then joins the scheduler and releases P through the registry. JSON rollback resets execute inside the shared boundary without an extra monitor or outer runLocked wrapper. Marker recovery is unchanged.

## Specification and executable evidence

| Requirement | Executable checks / oracle |
| --- | --- |
| Started workers stop before release/reset/fallback | ConcurrentAxisLifecycleTest, ProjectionDirectoryWorkerFenceTest, ProjectionParallelDirectoryWalkTest, DirectPageScannerLifecycleTest, WorkerTerminationTest; latch-controlled owner lifetime and interruption |
| Partial REST iteration closes the real producer chain | QuerySerializerLifecycleTest; bounded real descendant traversal and close propagation |
| Independent teardown / first failure / writer ownership | BufferManagerTeardownTest, AbstractResourceSessionWriteFailureCleanupTest, NodeTransactionAbortLifecycleTest, DatabaseOwnershipTest; suppressed failures, live-worker retention and exact permit counts |
| One marker recovery path | NodeTransactionAbortLifecycleTest, AsyncFlushFailurePathTest, WriterConstructionFailureCleanupTest; durable revision values, real prewrites, resource-cache isolation and recovery-stage faults |
| Callback rejection and execution boundaries | callbacksRejectLifecycleBeforeLockingAndRunAfterExecution; XML/JSON explicit, count+timer with queued scheduler, timer and async dispatch; every guarded lifecycle entry rejects, T/task boundary observed released |
| Callback borrow fencing / ordinary work | externalCloseFencesCallbackPayloadOwnership, ordinaryPostCommitHooksContinueInOrder; real payload remains usable until callback return, external close then terminates and another user writes |
| Nested execution, callback faults and stale close | TransactionExecutionLockTest, enclosingExecutionCannotEnterATerminationWait, postNotificationFailureCannotVetoDurabilityOrStrandLaterHooks; outermost delivery, scheduled Runnable completion, phase restoration and durable values |
| Cold lifecycle and late publication | untimedRevertCompletesNotificationDelivery, finalSessionNotificationsStillRejectLifecycleBeforeClosedChecks, publicationBeforeOwnerRegistrationStillUsesTheExecutionBoundary; callbacks observe published state outside T and reject before closed-state checks |
| Lineage changes during hardening | lineageChangesFenceHardeningBeforeWriterRecovery; JSON/XML revert/truncate wait for the predecessor on success/failure, preserve the original failure, and recover independently checked values |
| Admission and late concurrent close | queuedWriterAdmissionDoesNotHoldTheSessionMonitor, lateConcurrentCloseRetainsOwnerUntilPermitRelease; deterministic queues/latches, coherent registry and exactly-once release |

The [governing design](async-commit-failure-design.md#txn-callback-contract-r11--review-1-clarified), [callback postmortem](../../docs/postmortems/commit-callback-reentrancy.md), [lifecycle review](../../docs/lifecycle-fence-review.md) and [lessons](../../docs/LESSONS.md) contain the root causes and adversarial exploration. The former reentrant-close contract is superseded; historical validation counts and blocked attempts do not validate this revision.

Final focused verification is **blocked by shared capacity**: the settled heavy.sh invocation remained queued for over 27 minutes and was cancelled before Gradle started (empty log). No tool is missing. An earlier diagnostic run passed 198 cases, but it predates the final cold-lifecycle/lineage edits and does not validate this revision. The final planned scope is 210 cases; zero final cases executed. See [verification status](callback-contract-verification.json). Outer validation still requires scoped PIT for callback context/dispatch, execution lock/scheduler boundary, JSON rollback reset and factories, with individual survivor rationale and separate timeout reporting. Compiled-path/work-budget evidence must cover the changed timed lock boundary; no historical unchanged-shape claim is made. There is no per-row allocation, new success-path disk I/O or widened budget. Untimed no-lock row paths remain unchanged. Broader tests, lint, push/PR and CI belong to the outer executor.
