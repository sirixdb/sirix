# Bug: database close bypassed retryable lifetime state

- Fix / issue: [PR #1353](https://github.com/sirixdb/sirix/pull/1353), CI run 45043300388.
- Symptom: the Linux and macOS core lanes failed four TXN-615 lifetime cases; Windows
  failed the two teardown cases (its POSIX cleanup cases are skipped).
- Trigger: close an in-memory database with a one-shot teardown failure, rejoin it and
  open a resource; or close a direct database with denied tombstone deletion, restore
  permissions and repeat close. Both JSON and XML reproduce the failures.
- Root cause: the lifecycle wrapper returned on `isClosed`, bypassing the existing
  pending-cleanup branch. It also set `closing` without clearing it after failure,
  and permanently stopped the notification dispatcher before retryable teardown
  succeeded. The shared close boundary now reaches cleanup retry, restores admission
  on backend failure after user sessions drain, and stops notifications after successful
  owner teardown. A refused user-session drain retains admission and its live owners
  until retry; unconditional admission restoration would let resource creation bypass it.
- Why tests missed it: the existing TXN-615 tests already detect the first two
  regressions, but were not run after the wrapper change. Their readback oracle did
  not exercise hook admission after failed teardown. The extended AC15 test commits
  another value, observes synchronous pre-hook execution and asynchronous revision-2
  delivery, and reads both historical and new values.
- Regression tests: `DatabasesInMemoryLifetimeTest` reproduced all four CI failures
  locally before the fix (36 cases, four failures). Its direct/facade/new-instance
  cleanup matrix and `LocalDatabaseInMemoryLifetimeTest` cover cleanup identity,
  successful close, reopen, removal and both node models. The mutation association
  now includes these requirement-derived suites for `LocalDatabase`.
  `DatabaseCloseStrandingTest` checks both backend failure boundaries (resource store
  and transaction manager): original exception identity, continued pre-hook admission
  after failure, successful retry, terminal hook refusal and idempotent repeated close.
- Related CI defect: `test_scope` called the removed `source_package` helper.
  It now uses the same declared-package parser as target resolution and changed-line
  attribution, preserving Java, Kotlin and default-package selection and existing
  caps. The partition fixture also lagged its callee's association parameter and
  the emitted `results.json` object's `modules` field. A new timeout test contradicted
  CONV-202: PIT `TIMED_OUT` with `detected=true` remains detected, never an assertion
  kill. The corrected test preserves the raw status rather than changing gate policy.
  Syntax checking cannot catch the absent runtime name or fixture/API drift; the
  executable selection and report-consumer tests reproduce them.
- Verification: Python's 161 tests pass with 99% branch-inclusive coverage; replacing
  package resolution with the default package causes 13 selection-test assertion
  failures. All 478 selected core cases pass, with no skips. Exhaustive default PIT
  mutation of `LocalDatabase.close` and `closeQuiescent` reports 14 `KILLED`, five
  `SURVIVED`, three `NO_COVERAGE`, and no timeouts. All three mutants on this repair's
  changed lines (relative to `316d75f6ddaf5a7bebe1c2ab7d8709c3a4fdc337`) are killed.
  The other eight records are outside this repair's changed lines; no new disposition
  is claimed for them. Core commands, XML and mutation evidence are retained in the
  worktree's `build/ci-095/`; these are scoped results, not full release acceptance.
- Lesson: [retryable teardown must restore admission](../LESSONS.md#retryable-teardown-must-restore-admission).
