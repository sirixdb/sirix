# Bug: lifecycle tests raced interrupted retirement against callback completion

- Fix / issue: [PR #1353](https://github.com/sirixdb/sirix/pull/1353),
  [CI run 44163474869](https://github.com/sirixdb/sirix/actions/runs/44163474869),
  core remaining and changed-source PIT.
- Symptom: Linux core completed 16,398 tests with one failure at
  `SharedCallbackLifetimeTest.java:276`; three PIT partitions rejected the baseline.
  A fourth reported the changed `WorkerTermination.shutdownAndAwait` termination-loop
  mutant as `TIMED_OUT`, which does not meet the assertion-kill criterion.
- Trigger: interrupt notification retirement while a pre-hook or pinned post-hook is
  active, then release the callback without first observing cancellation.
- Root cause: the tests conflated retirement cancellation with callback completion.
  The [hook contract](../hooks.md) permits interrupted retirement to return promptly
  while retaining readers/history. In the pre-hook test, callback completion can notify
  the waiting closer before it processes interruption, changing the observed failure.
  The truncation test instead required the future to remain incomplete after interruption.
  Separately, the terminated executor mock returned false from every unexpected wait,
  allowing the inverted termination loop to spin until PIT killed its process.
- Why tests missed it: scheduling usually hid both ordering assumptions; the executor
  test asserted shutdown and interrupt state only after the helper returned.
- Regression test: retain every JSON/XML, synchronous/asynchronous, direct/shared/page,
  early/late reader and success/exception/Error case in `SharedCallbackLifetimeTest`.
  Observe interrupted close before releasing its pre-hook. Prove truncation is pending
  before interruption, observe cancellation, and then ask the still-active callback to
  navigate its distant committed page. Keep non-interrupted pending assertions, failure
  identity/suppression, retry, reader closure and write-permit assertions.
  `WorkerTerminationTest` now assertion-fails any wait on an already terminated executor
  across all four cancellation/interrupt combinations.
- Verification: original CI failure log and selection, guarded local commands and source
  hashes are retained under `build/ci-repair/`. Local verification writes full PIT XML/logs
  and classified results there. Only complete local results establish acceptance; original
  baseline failures and timeout reports are diagnostic evidence, not passing campaigns.
- Lesson: synchronize on the cancellation result while the callback is still held;
  assert retained ownership independently of whether its cancelling owner returned.
  Fail impossible mock interactions immediately so a termination mutant dies by assertion.
  See [Lessons](../LESSONS.md#coordinate-cancellation-before-releasing-borrowed-state).

## Compile-and-package formatting follow-up

- Symptom / trigger: CI run 44175487225 at `252be492dee3fcf6578e5df5801009c1f5929d49`
  failed `:sirix-core:spotlessJavaCheck` on the new `WorkerTerminationTest` wait stub.
- Root cause: the fluent call wrapped before `.thenAnswer` instead of inside its argument,
  violating the configured Eclipse formatter. The build stopped before runtime/PIT jobs.
- Correction: apply the formatter's reported two-line layout, preserving all four cases,
  the impossible-wait assertion oracle and every shutdown/interruption assertion.
- Regression check: execute `:sirix-core:spotlessJavaCheck` with the actual configured
  formatter; compile tests with `-PerrorProne=true`. The hosted failure is the before-fix
  evidence. Local execution status is recorded by the CI repair; pending runtime and
  original four-part core mutation acceptance are not replaced by this formatting fix.
- Local status (2026-11-17 21:12 UTC): `git diff --check` passed. The required
  `heavy.sh` timing gate remained exclusively held; the waiting command was cancelled
  before admission or any Gradle/JVM launch. About 88 GB was available before waiting;
  no post-admission disk check ran. Spotless, Error Prone/NullAway test compilation,
  repaired suites, the full remaining-core lane, all four original PIT partitions and
  the exact WorkerTermination assertion kill remain unverified; no pass is claimed.
- Lesson: run the configured formatter after the last test edit, including oracle-only
  edits, before treating a runtime/mutation repair as ready for CI.
