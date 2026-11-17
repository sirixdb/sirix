# Bug: lifecycle cancellation and CI ownership contracts

- Fix / issue: [PR #1353](https://github.com/sirixdb/sirix/pull/1353), CI run 44148435928.
- Symptom: cancelled URI imports retained sessions after inputs drained; the sorted-view retry
  fixture failed before its injected veto; macOS timeout tests failed at the registry assertion.
  Mutation analysis missed dedicated ownership suites and exhausted core's execution share.
- Trigger: interrupt two HTTP inputs while parsers stall; invoke projection maintenance from a
  pre-hook; open a database through a temporary-directory alias; select a large lifecycle diff.
- Root cause: the worker retained cancellation during `DatabaseNotifications.retire()`, which
  intentionally rejects interrupted admission. `shredOne` now consumes cancellation at the session
  retirement boundary, including failure before writer construction. Writer fences retain their
  existing handling; the public caller still restores its own status. Terminal executor tasks
  need no extra interrupt state. Projection maintenance follows `beforeCommit ->
  awaitDurablePredecessor -> awaitPendingAsyncCommit -> assertNotInCommitCallback`; invoking it
  from a user hook is intentionally forbidden. The fixture now maintains before commit entry and
  then vetoes publication. Registry keys use `toRealPath`, while the assertion used an alias.
  Mutation heuristics selected unrelated package histories instead of ownership regressions;
  caps then omitted sibling mappings. The reporter accepted PIT timeout detections as changed-line
  kills. Timeout cleanup also missed Gradle's detached single-use daemon. Explicit mappings,
  source-preserving partitions, assertion-kill verdicts and descendant cleanup address these
  shared runner boundaries without increasing the execution budget.
- Why tests missed it: input-factory interruption did not cover cancellation through real session
  cleanup. Ordinary Linux temporary paths have no alias. The projection fixture predated callback
  rejection. Package coverage did not execute the query close methods.
- Regression tests: `ParallelJsonShredderTest.interruptedWorkerReleasesItsSessionAfterFactoryOrReaderFailure`
  fails before the fix with suppressed retirement errors, then passes with exact input-failure
  identity and successful resource removal. The unchanged HTTP regression preserves stalled-input
  ownership and gate-release assertions. The sorted fixture retains its independent document/data
  oracle after further edits and retry. A symbolic-link notification test retains live-reader and
  successful-retry assertions. `TraversalCloseOwnershipTest` starts real concurrent producers and
  checks termination through JSON/XML streams, lazy iteration and descendant caches, plus primary
  error identity, suppression and upstream release. Python tests execute detached child cleanup,
  parse PIT reports and assert complete partition/source assignment. Six deliberate runner faults
  are assertion-killed; the timeout-verdict and detached-child tests fail before their fixes.
- Mutation result: query changed lines: 8/8 assertion-killed. Exhaustive `shredOne` testing with
  `DEFAULTS` and `NON_VOID_METHOD_CALLS`: 11 killed, one equivalent survivor, no timeouts/uncovered
  mutants. The survivor is `ConditionalsBoundaryMutator`, line 457, `> 0` to `>= 0`: only zero
  changes overload selection, and both shipped session overloads delegate to
  `beginNodeTrx(0, 0, MILLISECONDS, afterCommitState)`. The positive-threshold regression rejects
  removing configured intermediate commits. No timeout is counted as an assertion kill.
- Evidence: [source hashes, commands/log locations, counts and scope](evidence/lifecycle-ci-ownership/verification.json),
  [query PIT XML](evidence/lifecycle-ci-ownership/query-mutations.xml.gz),
  [worker PIT XML](evidence/lifecycle-ci-ownership/shredder-worker-mutations.xml.gz).
  Complete release-scoped core mutation and compiled evidence remain the outer executor's
  authorized follow-up. The interrupted exploratory core attempt is invalid, not complete evidence.
- Lesson: test retirement admission with cancellation still set, use canonical registry identity,
  and map ownership suites explicitly. See [Lessons](../LESSONS.md#cancellation-must-allow-its-own-cleanup).
