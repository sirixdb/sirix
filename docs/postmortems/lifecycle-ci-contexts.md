# Bug: lifecycle CI raced teardown without writer ownership

- Fix / issue: [PR #1353](https://github.com/sirixdb/sirix/pull/1353), CI run
  45051278498, based on `31b2c05fe7c16c51edeeb9a2be8c3cbac0deb444`.
- Symptom: macOS core reported `Transaction is already closed` in
  `DerivedIndexMaintainerTest.historyOwnerCanTruncateWhileDerivedCleanupWaits(true)`.
  Sonar rejected the reliability gate with five diagnostics.
- Trigger: the test started database close while holding only the derived-history lock.
  Close could retire the idle node writer before the test invoked node truncation.
  Sonar also reported thread-local cleanup, monitor ownership and timeout-validation
  diagnostics in the lifecycle implementation.
- Root cause: the concurrency fixture confused derived-history ownership with active
  writer execution ownership. It now enters storage maintenance before starting cleanup
  and invokes page truncation inside that existing scope. Both resource-cleanup and
  database-close cases keep their revision and termination assertions. Runtime callback
  and history scope behavior is unchanged.
- Why tests missed it: a favorable schedule let truncation start before close; the
  history lock alone did not guarantee this. Callback guard assertions cannot distinguish
  an absent thread-local entry from a null-valued entry. They verify preserved behavior,
  not retained-entry memory usage.
- Regression oracles: TXN-504 requires active maintenance ownership to survive teardown;
  the corrected history test exercises both cleanup consumers. TXN-804 requires nested
  callbacks to preserve the enclosing guard and remove restrictions on return;
  `callbackScopeRestoresItsCallerAfterSuccessOrFailure` checks both outcomes. CONF-201
  requires positive, nanosecond-representable timeouts;
  `invalidTimeoutAdjustmentPreservesTheConfiguredTimeout` checks rejected inputs and
  unchanged configuration. These assertions reject premature retirement, lost/leaked
  callback guards and invalid timeout publication without inspecting private state.
- Sonar false positives: the callback and history thread-local values are already
  restored in `finally`, including restoration to null at the outermost exit. That
  releases callback/session/writer ownership; a null-valued map entry retains none of
  those owners. Nested synchronous callbacks still restore their enclosing guard.
  Removing the map entry itself is not part of the lifecycle contract, and `get()` on
  the guard can create an empty entry again. Narrow field suppressions retain this
  existing behavior. `DatabaseNotifications.execute` is called only by `submit`
  and `Retirement.close`, both under `synchronized (progress)`.
  `abandonAfterFailure` is called only by the failure branches of `retire`, also under
  that monitor. Their `notifyAll` calls therefore already hold the monitor. The ignored
  `Duration.toNanos()` result deliberately checks overflow before publishing the timeout.
  Method-level suppressions name only S2273 or S2201 and explain the invariant; no extra
  locking or removal of validation is needed. All five diagnostics have individual
  justifications; no rule-wide project exclusion is added.
- Evidence limits: the original macOS failure is preserved in the downloaded CI report.
  An initial fixture revision incorrectly reentered node maintenance and failed the
  existing execution guard; it was corrected to use page truncation inside the owned
  scope. A trial `ThreadLocal.remove` implementation produced uncovered restoration
  branches and surviving cleanup mutants. It was removed after rechecking the actual
  ownership invariant; its raw report is retained as superseded evidence, not final
  mutation acceptance. Local logs, XML, task graphs and results for this repair are under
  `build/ci-096`. They do not establish a remote CI pass. CodeFactor's eleven diagnostic
  sites remain unavailable (HTTP 403). The original PIT artifact has empty reports after
  core partitions 1 and 2 exhausted their 270-second shares; other partitions completed.
  No mutation-selection, sampling, timeout or verdict bound is relaxed by this repair.
- Final local regression verification: Temurin 25 ran 1,189 selected core cases with no
  failures or skips, including all 345 notification cases, 71 derived-index cases and
  325 core work-budget cases. Another 55 isolated prerequisite cases had no failures;
  two arena-generation tests were skipped in the diagnostic task that explicitly
  disables arena closing. Spotless passed. These counts describe this repair's
  selected local run, not the full cross-platform lane or release acceptance. The final
  run after restoring the original runtime logic reproduced the same passing counts;
  its XML and task graph are recorded separately under `build/ci-096/final-regression-*`.
  Compiling the two original production sources at `31b2c05` with the same compiler
  and dependencies produced identical `javap -c -p` output for all ten emitted classes,
  including nested classes. This checks executable instructions and exception tables
  for this repair; it is not the outstanding full PR main/head comparison or a latency
  claim. The final production edits are comments and source-retained suppressions.
- Lesson: [own the operation before racing teardown](../LESSONS.md#own-the-operation-before-racing-teardown).
