# Bug: timed commit callback joined its own scheduler

Historical round: the callback behavior/evidence below predates the clarified contract in [commit callback ownership](commit-callback-reentrancy.md). Callback lifecycle reentrancy is now rejected; KEEP_OPEN admission and pre-hook timing are preserved. Historical passing counts do not validate the new dispatch boundary.

- Fix / issue: lifecycle audit V05/V06, review R6.
- Symptom: a supported timed post-commit hook calling transaction close or session close never returned; the shared write permit remained held.
- Trigger: open a timed XML or JSON writer and register a post-commit hook that closes that writer or its resource session.
- Root cause: teardown treated executor termination as a fence that any caller could perform. On the scheduler worker, the running callback itself prevented termination. Holding the transaction monitor while an external close waited for the scheduler/transaction lock could also prevent the hook from entering close. Skipping only the self-wait would still allow later post-commit hooks to access closed state.
- Why tests missed it: earlier cases initiated teardown from external threads or used count-based asynchronous commits, whose scheduler has no timed task. No test composed a real scheduler callback with public close or session abort.
- Regression test: `NodeTransactionAbortLifecycleTest.timedPostCommitCloseReleasesAdmissionWithoutJoiningItself` covers both actions and both formats, including an already queued external closer. Latches establish the ordering; bounded completion waits guard against deadlock. A second user reads the committed value and writes while the old callback is paused; later hooks never run, repeated close releases no extra permit, and external close returns only after scheduler termination. Ordinary hook order and count-based asynchronous hook closure have separate controls. No pre-fix run is claimed: the old self-join is established by tracing the task through the unconditional termination loop, avoiding a deliberately stranded non-daemon worker.
- Lesson: executor fences require an external owner; self-initiated teardown needs a safe return path and must stop subsequent callbacks. See [Lessons](../LESSONS.md).

Focused result: 126 selected cases passed, zero failures/errors/skips; see [the lifecycle review](../lifecycle-fence-review.md#r6-scheduler-initiated-close) for the exact scope and log. Scoped mutation validation remains pending with the outer executor; deadlock timeouts must be reported separately.
