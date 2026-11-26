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
implements TXN-307 without changing the public commit signatures.

Review-round result: the focused command passed all 179 selected cases (30 durability, 147
abort-lifecycle, one async ordering and one async sidecar case). Gradle's required isolated
startup/storage prerequisites also passed. No whole-repository test or lint suite was run.

A further review identified reporting as another escape from storage hardening: an error logger,
the marker-error wrapper allocation, repair-error logging or the phase-two debug logger could
throw after the beacon returned. The synchronous caller would restore running/dirty accounting;
the async caller would never reach its durable flag. Marker errors now keep their original exception,
post-beacon catches keep only the first cause by reference, repair precedes guarded error logging,
and the redundant secondary repair log is removed. Debug reporting has the same non-throwing boundary.

`postBeaconReportingFailurePreservesDurableSuccess` uses the real logger's filter boundary to throw
preallocated runtime or out-of-memory failures during cleanup-error, marker-deletion and phase-two
debug reporting, synchronously and asynchronously. Its independent oracle is TXN-307: the injection
must fire, the committed revision must be published, further lifecycle work must be rejected, repair
responsibility must remain, and reopening must recover exactly the committed empty array. These new
cases have not been executed before the fix; no counterfactual execution is claimed.

Focused execution exposed a related consumer assumption: async orchestration synthetically rethrew
the recorded cleanup cause into an unchecked-only catch. Keeping the original marker IOException
made that assumption visible. Known durable causes are now reported directly before retirement;
the synthetic rethrow remains only for pre-point failures. The marker cases exercise this boundary
in both commit modes, including the existing JSON/XML lifecycle regression.

Reporting-round result: the focused command passed all 189 selected cases (42 durability and 147
abort-lifecycle), including all 12 reporting-failure cases. Its initial run reproduced the checked-cause
escape in the two new async marker cases and both existing JSON/XML marker-deletion cases; those
same four cases passed after the direct-consumption correction. Gradle's required startup/storage
prerequisites also passed. The reporting guards themselves were not tested counterfactually against
the preceding commit, and mutation evidence remains with the outer pipeline.

CI exposed two test defects in this change. The catalogue, construction-cleanup and notification
regressions still expected a post-durable construction/submission error and same-writer rollback.
That expectation predates TXN-307's irreversible terminal lifecycle. All four platform lanes failed
those same 15 cases; PIT stopped on seven of them during its unmutated baseline. The revised tests
derive the result from TXN-307: explicit commit succeeds, automatic rotation refuses the next edit,
commit/rollback/revert remain refused, and closing then opening a fresh writer preserves the durable
values, index lookups, historical revisions, projection-owner retirement and notification delivery.
The obsolete duplicate rollback-recovery branches are removed; ordinary successful revert and
rollback coverage remains.

The Windows-only reporting failures had a separate trigger: the synchronous fixture replaced
`.commit` with a nonempty directory before `commitWritePages` tried to create the marker. Windows
reports `AccessDeniedException` for that creation, before durability; Linux accepted it as an existing
path. Both synchronous and async fixtures now replace the real marker through a one-shot test-writer
action at spill cleanup, after the beacon returned. The independent oracle remains the durable
revision and its recovered data, plus the recorded marker-deletion `IOException` and an observed
reporting failure. A pre-point exception, a skipped injection, a retried revision or lost catalogue
must fail these tests. No production behavior or default changes in this CI correction.

The focused CI correction command selected `PostDurableCleanupFailureTest`,
`WriterCatalogueRecoveryTest`, `WriterConstructionFailureCleanupTest` and
`DatabaseNotificationLifecycleTest.inlineHardeningFallbackStillDeliversDurableHooks` through
`:sirix-core:test --max-workers=2 --tests ...`. It reproduced the 15 shared failures before the test
correction, then passed all 60 cases (42 durability, 10 catalogue, six construction, two notification),
along with Gradle's required isolated prerequisites. Formatting passed and all 266 mutation-tooling
tests passed. The Windows diagnosis uses the CI stack trace; no local Windows execution is claimed.

PIT's original selection omitted the requirement-owned durability and catalogue regression suites.
Explicit associations now include both for the transaction and storage writers, so the changed-line
lane measures their existing behavior checks too.

The local changed-line PIT run completed with a green unmutated baseline: 59 mutants, 33 killed,
20 survived and six uncovered. Thus the original seven failing baseline cases are fixed, but the
mutation verdict remains unclean; no equivalence or coverage claim is made for the remaining 26.
The reports are under `build/ci-mutation-fixed/` for the outer executor's mutation review. The local
checker also flagged 40 existing dispositions in other modules because those modules' classes were
not compiled in this core-only workspace run; their entries were left unchanged. No guard removal,
waiver or mutant-directed test was used to suppress either result.


The next CI round exposed missing contract evidence rather than an unmutated failure. The terminal
admission tests started with an already terminal transaction, so they could not distinguish the
necessary check after a predecessor wait from duplicate checks before it. The regression now makes
catalogue acknowledgement fail during the waiting lifecycle operation itself, across commit,
rollback, revert, truncate, maintenance, atomic scope, automatic rotation, close and abort. Admission
uses one post-wait check where possible; already terminal and ordinary successful paths remain covered.

`PostDurableStorageCleanupTest` exercises the storage API without a node transaction to perform a
second cleanup. Its independent TXN-307 oracle is resource ownership: a completed hardening releases
durable cursor pages, cursor guards, intent-log entries and captured records, and later reads return
the persisted record. A spill-cleanup failure must preserve those releases and establish repair
responsibility before node orchestration can observe it. The decorator test checks the same failure
object across the public forwarding API. The lane now selects these tests and the existing JSON
sidecar contract. First-cause selection is shared by the guarded cleanup steps, avoiding repeated
untested conditionals without allocating suppression storage. Sonar's volatile-Throwable warning
is a reference-publication false positive: the hardening thread owns cleanup, completion fences
precede consumption, and Throwable synchronizes its diagnostic mutation; the exception is scoped
to that one field.

The final focused run passed 88 cases (52 durability, four storage cleanup, 16 sidecar, ten
catalogue and six construction-cleanup cases). JaCoCo on the working-tree diff measured 94/111
covered executable lines and 17/20 covered branches: 84.7% combined changed-code coverage, above
the Sonar gate's 80% floor. This is local focused evidence, not a claim that a new hosted Sonar
analysis has run. The optional phase-two elapsed-time arithmetic was removed; its guarded
revision diagnostic remains. A per-writer live-capture counter now proves that hardening releases
captured heap records before upper-layer writer retirement can hide retained memory.

The final changed-line PIT run completed with all 50 mutants killed, zero survivors and zero
uncovered mutants; no mutation dispositions were added. The cold-open, catalogue and sidecar
assertions protect the vision's immutable, readable revisions; the live-capture assertion protects
owned-memory cost. The local runner's first disposition pass overlapped an Error Prone rebuild,
which temporarily removed three core class files; its separate post-build check is recorded with
the final verification evidence rather than treating missing local artifacts as source defects.

Error Prone compilation of main and test sources and `:sirix-core:spotlessJavaCheck` completed
successfully. After compilation finished, the official disposition checker reported 43 valid
entries and zero errors against the final report and census. Hosted Sonar analysis remains the
outer CI executor's responsibility.
