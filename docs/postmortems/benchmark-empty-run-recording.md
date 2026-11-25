# Bug: first-process failure erased a nightly run from the ledger

- Fix / issue: review finding R14 on `fm/sirix-perf-gate-statistics`; CONV-303 and CONV-304.
- Symptom: the nightly trend report could select yesterday after today's first engine process failed,
  despite the report correctly selecting the latest persisted nightly run.
- Trigger: either kit successfully opens a nightly run, then its stub engine fails on its first
  invocation before any measurement is imported.
- Root cause: the shared exit hook made persistence conditional on a nonempty measurements file.
  That confused whether a run existed with whether it produced measurements. Removing that guard
  lets the existing commit operation persist the run row, with or without measurements. The hook
  still requires a successfully opened run; pre-opening refusals append nothing. JSONBench's
  delegated exit hook and ClickBench's direct exit hook both use this boundary.
- Why tests missed it: kit failure tests had already completed processes. Trend tests supplied an
  empty latest run directly, bypassing the producer that discarded it.
- Regression tests: both kits' `test_a_first_process_failure_records_its_nightly_run` execute real
  kit scripts with stub engines, a previous complete night and a first-call failure. CONV-303 is
  the oracle for one new run row and zero new measurements; CONV-304 requires the trend CLI to name
  the new night and exit 2. The tests reject both losing the run and stale success. Pre-opening
  refusal tests cover missing commits, dataset names and conditions entries. ClickBench uses a
  copied script with a stub rig lock, so the shared rig lock is never acquired.
- Lesson: [LESSONS.md](../LESSONS.md), via `lessons.d/benchmark-empty-run-recording.md`.
- Vision check: reliable recording preserves truthful evidence for the engine's cost contract.
  No acceptance thresholds, nightly constants, allocation rules or engine defaults change.

Focused verification: all 21 tests in `test_kit_recording` passed in 69.824 seconds. The equivalent
command, from `bundles/sirix-query/bench/common`, is `python3 -B -m unittest test_kit_recording`.
The same verification driver then restored the pre-fix shared hook in temporary kit copies: both
new regressions failed because the new run row was absent. Inverting the opened-run guard also
failed both regressions. Neither mutation survived. Mutation execution used stub engines and the
stub rig lock, and left repository sources unchanged. The focused supplement is recorded in
[MUTATION_DISPOSITIONS.md](../../bundles/sirix-query/bench/common/MUTATION_DISPOSITIONS.md).
