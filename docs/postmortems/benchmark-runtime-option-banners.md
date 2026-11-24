# Bug: runtime capture persisted JVM environment options

- Fix / issue: review finding R10 on `fm/sirix-perf-gate-statistics`; CONV-303.
- Symptom: `environment.json` and the run row could contain a username and home-directory path;
  the arm's runtime line could name an option announcement instead of the runtime version.
- Trigger: a Java version probe prints `Picked up JAVA_TOOL_OPTIONS: -Djava.io.tmpdir=/home/alice/tmp`
  before its version. `_JAVA_OPTIONS`, `JDK_JAVA_OPTIONS` and the `NOTE:` prefix have the same risk.
- Root cause: capture treated all successful probe stderr as version evidence and selected its
  first line for the arm. The shared runtime boundary now removes option announcements from both
  probed and supplied text before either representation is persisted. Banner-only output becomes
  unavailable with an option-free reason. Probe failures still retain their existing safe reasons.
- Why tests missed it: the real-launcher fixture covered stderr, failure, silence and a missing
  executable, but never combined option announcements with a version or omitted the version after
  an announcement.
- Regression test: `RuntimeVersionTest.test_option_banners_are_excluded_from_captures_and_run_rows`
  executes launcher scripts through public capture and run creation, and exercises supplied runtime
  text. Its independent oracle is CONV-303: all six announcement forms disappear, the exact version
  and runtime detail remain, persisted captures agree, and banner-only output is unavailable.
  It rejects recording private options or selecting the banner as the version.
- Lesson: [LESSONS.md](../LESSONS.md), via `lessons.d/benchmark-runtime-option-banners.md`.
- Vision check: trustworthy, publishable measurement evidence supports the engine's cost contract.
  This correction introduces no acceptance default and changes no timed launcher or engine path.

Focused verification: all 90 tests pass with `python3 -B -m unittest test_bench_ledger` from
`bundles/sirix-query/bench/common` (run through an in-memory verification driver). The same driver
bypassed the filter to reproduce the pre-fix output: the regression failed in 14 cases. Seven
additional mutations removed individual banner names, the `NOTE:` form, the exclusion, whitespace
trimming or the unavailable result. All were detected; no source files were changed for mutation
execution. The [mutation record](../../bundles/sirix-query/bench/common/MUTATION_DISPOSITIONS.md)
records this focused supplement separately from its historical whole-module figures.
