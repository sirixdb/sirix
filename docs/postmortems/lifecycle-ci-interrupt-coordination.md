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
