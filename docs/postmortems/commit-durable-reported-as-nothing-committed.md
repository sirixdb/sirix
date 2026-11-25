# Bug: a durable commit reported as "nothing is durable yet"

- Fix / issue: [#1380](https://github.com/sirixdb/sirix/issues/1380); spec
  [TXN-307](../../spec/TXN-transactions-durability.md#txn-307--known-durable-commit-survives-cleanup-failure)
- Symptom: a `commit()` whose revision was already durable threw the storage failure of a
  later cleanup step, and the transaction went back to `RUNNING` with its modification count intact —
  exactly the state a commit that never happened leaves behind. An application therefore learned that
  its data was not persisted when it was. Retrying wrote the same change into a second revision; not
  retrying reported a failure for a revision that exists.
- Trigger: any failure in a step of `NodeStorageEngineWriter#hardenCommit` after
  `Writer#writeUberPageReference` returned — forgetting the spill file, closing durable read pages,
  releasing the page guard, `TransactionIntentLog#clear()`, deleting the recovery marker. Reproduced
  by wrapping the real `FILE_CHANNEL` writer of a live resource session in `FaultInjectingWriter` and
  throwing at the first storage call after the beacon write
  (`Point.BEFORE_DISCARD_SPILLED_WRITES`).
- Root cause: `hardenCommit` had no boundary at its commit point. The beacon write and the cleanup
  that follows it sat in one straight-line block, so a cleanup failure left the method the same way a
  pre-durability failure does, and `commit()` rethrew it. `commitInternal`'s catch around the whole
  storage commit then applied the only classification it could see — "nothing is durable yet" — and
  restored `State.RUNNING` without clearing `modificationCount`. The broken invariant is that the
  durability of revision N is decided by the beacon write's return and by nothing that runs after it;
  the code instead let the *last* statement of the commit decide what the caller was told.
- Why tests missed it: the seam that can reach these steps existed but was never connected to a real
  commit. `FaultInjectingWriter`'s own test verified the decorator's contract against a stub, and its
  Javadoc recorded the live wiring as deferred, so no test ever ran a resource session's commit
  through an injected post-beacon failure. The spec's `PostDurableCleanupConformance` case was
  written as planned work. Nothing else exercises the window: a crash test kills the process, which
  destroys the transaction instead of asking it what it reports.
- Regression test:
  `bundles/sirix-core/src/test/java/io/sirix/access/trx/node/PostDurableCleanupFailureTest.java`.
  `./gradlew :sirix-core:test --max-workers=2 --tests 'io.sirix.access.trx.node.PostDurableCleanupFailureTest' --tests 'io.sirix.access.trx.node.NodeTransactionAbortLifecycleTest'`.
  Before the fix `cleanupFailureAfterTheCommitPointReportsTheCommitAsSucceeded` fails with
  `SimulatedCrashException: crash at BEFORE_DISCARD_SPILLED_WRITES` thrown out of `wtx.commit()`
  (`hardenCommit` → `commit` → `commitInternal`); after it the commit returns, the writer is terminal
  and the revision is readable through a fresh open.
- Lesson: a durability boundary inside a method needs to be a boundary in the code, not a comment
  above a statement; see [LESSONS.md](../LESSONS.md). A second, narrower one: an injection point
  named `AFTER_<call>` is not a post-durable failure when the call's contract makes its *return* the
  acknowledgement — a throw out of `writeUberPageReference` leaves durability undetermined, which is
  TXN-705's case, not this one.

The review also found the same invariant broken outside storage hardening: successor construction,
publication, pending-root cleanup and retired-writer close could still throw after N was durable.
The original terminal bookkeeping reused the ordinary `COMMITTED` state, so commit and rollback
could reopen the transaction. An irreversible admission flag now covers both commit paths,
rollback, revert, history maintenance and every automatic rotation return. The orchestration
boundary retains durable success and flags repair, including when a foreground async failure is
followed by successful inline hardening. A speculative successor can already be dirty: it stays
uncommitted, rejects further work and is discarded by fenced internal abort.

The expanded regression cases use TXN-307 as the oracle: injections must fire, known durable
outcomes must not become hardening failures, terminal admission must stay closed, and reopening
must reveal exactly the committed empty array without speculative successor edits. Ordinary
successful continuation and the existing pre-point contrast guard the other side of the boundary.
The review-round focused command also selects the existing abort-lifecycle suite; new cases were
not executed before the fix, so no counterfactual execution is claimed for them. Mutation evidence
remains the outer pipeline's responsibility.

Vision check: the fix preserves the single committed revision history and its readable content,
adds only cheap admission guards to mutation paths, changes no storage format or default, and
implements TXN-307 without adding a public API.

Review-round result: the focused command passed all 179 selected cases (30 durability, 147
abort-lifecycle, one async ordering and one async sidecar case). Gradle's required isolated
startup/storage prerequisites also passed. No whole-repository test or lint suite was run.
