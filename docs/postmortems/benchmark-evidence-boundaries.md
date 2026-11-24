# Bug: benchmark evidence boundaries leaked options and hid missing nights

- Fix / issue: review findings R11–R13 on `fm/sirix-perf-gate-statistics`; CONV-303 and CONV-304.
- Symptom: multiline JVM option announcements could persist private paths; the default trend report
  could report an earlier night after the newest run aborted; missing ClickBench option values
  could leave the argument parser looping indefinitely.
- Trigger: a JVM banner followed by a newline-separated `-Djava.io.tmpdir=/home/alice/tmp`; a latest
  nightly run with 19 accepted processes, only rejected processes, or no measurements; a final
  `--arm-commit` or `--arm-kind` without a value.
- Root cause: R10 filtered individual lines rather than complete announcements and inherited the
  probe's option environment. Trend date selection depended on usable measurements rather than
  the run that owns the date. Argument consumption depended on a value that was never checked.
  Fixes suppress option variables only in version probes and discard complete announcements at
  the runtime boundary, select dates from nightly run rows, and check all value-taking options
  before consuming any of them. Timed arms retain their own environment.
- Why tests missed it: banner fixtures used single lines, trend tests explicitly requested incomplete
  nights or measured a later complete night, and kit tests always supplied option values.
- Regression tests and independent oracles:
  - `RuntimeVersionTest` executes version launchers and public capture/run creation. CONV-303 requires
    all three option variables absent in the probe while the parent retains them, complete banner
    removal for probed and supplied text, correct version lines, and unavailable evidence when no
    version remains. It rejects surviving continuation lines and a diagnostic selected as a version.
  - `ReportTest` exercises the CLI and public report with incomplete, rejected and empty latest runs,
    an unrelated later control run, explicit historical dates, iterable input, and partially missing
    series. CONV-304 requires the latest nightly run date or an explicit date, never stale success.
  - `ClickBenchColdRoundsTest.test_value_options_refuse_a_missing_argument_without_running_engines`
    invokes the copied kit with each value-taking option missing its value, under a three-second
    timeout. The CLI's argument contract requires an error before measurement. Stub engines and a
    stub rig lock ensure the shared measurement rig is never taken.
- Lesson: [LESSONS.md](../LESSONS.md), via `lessons.d/benchmark-evidence-boundaries.md`.
- Vision check: trustworthy evidence preserves the engine's cost contract. These fixes change no
  acceptance floor, precision, pair limits, allocation rule, nightly constants or engine defaults.

Focused verification: all 140 tests in `test_bench_ledger`, `test_bench_trend` and
`test_kit_recording` passed in 96.519 seconds. The verification driver then ran the relevant
regression tests with in-memory function substitutions and mutated temporary kit copies:

| Counterfactual or mutation | Observed rejection |
|---|---|
| R11's previous single-line filter | 44 failed cases |
| R11's previous inherited probe environment | 1 failed case |
| Keep announcement continuations | 44 failed cases |
| Keep a diagnostic without a version | 2 failed cases |
| Inherit each of the three option variables separately | 1 failed case per variable |
| R12's previous date selection from usable series | 4 failed cases |
| Select the oldest nightly run instead | 7 failed cases |
| Ignore an explicitly requested date | 2 failures and 3 errors |
| Omit either reported CLI option's guard | A three-second timeout per option |

All 12 mutations were detected, with no survivors; repository sources remained unchanged during
mutation execution. The ordinary focused command, from `bundles/sirix-query/bench/common`, is
`python3 -B -m unittest test_bench_ledger test_bench_trend test_kit_recording`. The mutation supplement
is recorded in [MUTATION_DISPOSITIONS.md](../../bundles/sirix-query/bench/common/MUTATION_DISPOSITIONS.md).
Black was unavailable on PATH, so automatic formatting was not run; the outer executor owns lint.
