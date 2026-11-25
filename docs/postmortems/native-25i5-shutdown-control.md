# Bug: native shutdown control assumed LTS diagnostics

- Fix / issue: PR #1488, CI finding ci-1.
- Symptom: normal core C shared-image cycles passed on 25i5, but the omitted-shutdown
  control failed its oracle despite completing data work and hanging in isolate teardown.
- Trigger: `SIRIX_SMOKE_TEST_TEARDOWN_MUTATION=1 tests/native/run-smoke.sh all ABSOLUTE_OUTPUT_DIR`
  with Oracle GraalVM 25.5.5.0.3 on Linux x64.
- Root cause: the oracle required exit 124 and `laggard` text from the LTS runtime.
  The pinned runtime instead prints `Teardown is taking too long` and the snapshot worker
  record; its diagnostic path reports a fatal deadlock error and remains stuck through
  SIGTERM until the watchdog escalates to SIGKILL (137). Both core and query share this oracle.
  The control now uses SIGKILL directly and checks the pinned runtime's teardown diagnostic
  and snapshot worker's native thread record, as well as completed data work and entry into teardown.
- Why tests missed it: the control's previous validation used 25.0.3; a toolchain-only pin
  changed the external diagnostic and watchdog behavior without changing the engine.
- Regression test: the existing core/query omitted-shutdown executable controls, specified
  in [SMOKE.md](../../tests/native/SMOKE.md#ci-evidence-contract). Hosted run
  [45252506282](https://github.com/sirixdb/sirix/actions/runs/45252506282) reproduced the
  old oracle's failure; normal callers must still return zero, while omission controls
  must identify snapshot workers and reach the watchdog. This rejects unrelated hangs
  and failures before data work completes.
- Verification (2026-11-23): ran that command locally with the pinned Oracle GraalVM
  25.5.5.0.3 and `SIRIX_SMOKE_DISK_FLOOR_KIB=10485760`, output under
  `build/ci-smoke/normal` (exit 0). Both shared images built, all four normal C caller
  cycles returned zero (persisted/core and query results 42, no JVM loaded), and both
  omitted-shutdown controls reported snapshot workers and reached SIGKILL exit 137.
  Bash syntax, ShellCheck, actionlint and `git diff --check` passed.
- Lesson: [pin negative-control oracles with the toolchain](../LESSONS.md#pin-negative-control-oracles-with-the-toolchain).
- Vision check: preserves evidence for an embeddable, JVM-free engine and explicit worker
  shutdown; changes only the deliberate failure control, with no engine or I/O default change.
