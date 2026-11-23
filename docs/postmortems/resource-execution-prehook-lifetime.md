# Bug: retirement missed sibling execution and pre-hooks

Historical review-round design: the shared completion mechanism below was superseded by
the [database notification contract](../hooks.md). Recorded results describe that earlier
implementation, including its resource-scoped callback ownership.

- Fix / issue: review-shared-retirement-reentrancy and review-prehook-lifetime.
- Symptom: A.close could wait for a post-hook queued by B while the caller still held B's outer execution lock; alternatively A.close could retire an independent reader still used by B's pre-hook.
- Trigger: same-resource user sessions A/B; timed B writer commits inside runLocked and then closes A or directly truncates its page writer. Separately pause B's pre-hook before publication while it uses A's direct/shared/page reader, and start A.close.
- Root cause: the enclosing-execution guard consulted only A's node owners, despite resource-wide completion ownership. Pre-hook entry set callback context but did not participate in that completion ownership.
- Why tests missed it: the enclosing-execution oracle used only the writer's own session; the cross-session reader matrix covered post-publication callbacks only.
- Regression test: SharedCallbackLifetimeTest.enclosingExecutionRejectsRetirementAcrossUserSessions covers JSON/XML, queued post-hook and no-hook controls, same-resource close/page truncation rejection, continued reader use, different-resource independence and later normal closure. preHooksRetainCrossSessionReadersUntilReturnOrVeto covers JSON/XML, sync/async commits, initial/late direct/shared/page readers, success and RuntimeException/Error veto, interruption and independent cleanup faults, exact failure identity and retry. The pre-fix source was not executed in this round; focused final results are recorded separately.
- Lesson: apply a resource-scoped retirement invariant to every user-session owner and every before/after-publication callback phase; see [the lesson](../LESSONS.md).

The writer registry changes only at cold registration and final permit release. Session close rejects before its monitor/snapshot. Raw page truncation checks before waiting for shared completion. Public node truncation rejects enclosing execution at entry, then performs its existing local hardening/callback fence under T; its validated internal page call identifies that cold owner using the existing execution context. Callback context always rejects before this internal allowance. Normal transaction lock/unlock, runLocked, timed mutators and count-only row paths are unchanged. Pre-hook completion wraps the original synchronous hook phase and restores context in finally before completion; veto timing/order and the original exception remain unchanged. No queues, threads, benchmark or per-row bookkeeping were added.

Focused outcome: 370 selected tests passed (362 core/eight query), with zero failures/errors/skips; configured auxiliary tasks contributed 45 cases, two skipped and no failures. The final lifetime class includes 122 cases. `data/sirix-1-0-lifecycle-fences/resource-prehook-verification.json` binds exact classes, scope, limitations and final source hashes; scoped PIT and final compiled/work-budget checks remain with the outer executor.
