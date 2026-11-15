# Bug: bounded PIT selection omitted the compiler refusal contract

- Fix / issue: PR #1352; [hosted report](https://github.com/sirixdb/sirix/actions/runs/43758256779/job/132948242124).
- Symptom: the completed hosted report had a changed-line conditional survivor and
  an uncovered fallback return in `SirixBlockPipelineStrategy`; the ordinary return
  mutant timed out. The run completed in 8m19s, so this was not the earlier global
  deadline failure.
- Trigger: the nine changed query sources selected 20 deterministic test patterns,
  omitting `VectorizedReleaseCorrectnessTest`. `SirixCompileChain` and
  `VectorizedRouteRefusedException` had no mapped tests.
- Behaviour / invariant (specified before repair): every compiler/refusal entry
  point must select the executable release contract under the existing module
  cap, retaining all previously selected tests. This covers the compile chain,
  sequential and block strategies, and refusal exception; sequential, parallel,
  ordered, unordered and morsel factories; single/composite grouping, refused and
  admitted routes, and warm/cold-reopened stores. Missing test declarations must
  never become phantom PIT filters, and matching simple names in other packages
  must not acquire these associations.
- Root cause: name/package heuristics cannot connect a multi-class release
  contract in the scan package to the compiler factories and strategies that it
  exercises. Package fallback selected translator unit tests, which did not
  execute the block grouping fallback.
- Why tests missed it: selection tests covered naming and budget mechanics but
  had no executable expectation for this integration contract. Passing the
  ordinary release suite did not imply PIT would select it.
- Regression tests: `SelectionTest.test_release_compiler_entry_points_select_the_end_to_end_refusal_contract`
  failed for all four entry points before repair. Its independent oracle is the
  release suite's documented public factory/refusal contract; it asserts the
  actual emitted Gradle test filter, rejecting omission. Companion tests exercise
  absent declarations, unrelated packages, all three heuristic resolution kinds,
  union preservation and module-cap rejection.
- Fix: exact associations at the shared test-selection boundary augment the
  existing heuristic candidates before the unchanged cap is applied. The actual
  selection now has 21 patterns, no unmapped sources and no removed patterns.
  Production kernels, PIT deadlines, sampling and blocking rules are unchanged.
- Ordinary-path coverage: the first focused rerun killed the conditional and
  grouping-fallback return mutants, but exposed `SirixBlockPipelineStrategy:16`
  as uncovered by the release suite. Its parallel cases all grouped rows, even
  when the grouping itself remained admitted. The required invariant is that
  non-grouping pipelines return their ordinary compiled expression through all
  five compiler factories. `ordinaryPipelineRetainsRowsAcrossReopen` adds a
  three-row JSON filter/order query with the independent expected result `2 3`,
  checking warm and cold-reopened execution with the projection registry cleared.
  This rejects a null ordinary pipeline without depending on the large XML
  concurrency fixture. The shared block boundary also serves non-grouping XML
  pipelines; no production branch or helper was changed.
- Tooling verification: all 46 Python tests pass. Five deliberate selector mutants
  (omit associations, discard heuristic matches, invert unresolved-kind handling,
  admit absent declarations and match unqualified names) are rejected by executable
  tests. The prepared target inventory is identical to the hosted artifact's 71
  classes, with the unchanged sample of four mutants per class. Local evidence is
  under `build/ci-evidence/round10/`.
- JVM verification: the focused `VectorizedReleaseCorrectnessTest` run passes all
  84 cases with no skips. The command used `heavy.sh`, two processors/workers,
  a 768 MiB Gradle heap and a 256 MiB–3 GiB test heap; free root disk was 89 GiB.
  The full bounded PIT rerun retains all 71 targets and all 21 selected test
  patterns, using the existing 1,320-second deadline and four-mutant class sample.
- Full-run limitation: local PIT stopped before mutation execution because
  `XmlPathSummaryParallelQueryTest.parallelFlworCachesKeepPublicationWithinTheirWorker`
  invocation 3 failed its baseline latch wait after 61,044 ms. This suite is in
  the original 20-pattern selection and its source is unchanged from the base.
  The full run is correctly recorded as an error with no mutation score; it is
  not a complete passing report or an unchanged-line-debt exception. Its log and
  `results.json` remain under `build/ci-evidence/round10/full/`. No XML-test wait,
  assertion or selection was weakened. A separate focused run verifies the
  originally reported compiler/fallback mutants; it does not replace full CI.
- Final focused mutation evidence: all seven sampled mutants match the hosted
  changed-line inventory by class, line and mutation description, and all seven
  are `KILLED`, with no survivors, uncovered mutants or timeouts. The three block
  mutants at lines 13, 14 and 16 are killed by the release suite: refused grouping
  kills the first two; the new ordinary-query control kills the last. The four
  fallback-wrapper mutants remain killed by `VectorizedFallbackExprTest`. Line
  coverage for these two classes is 15/15. Complete XML/HTML and the cross-check
  are retained under `build/ci-evidence/round10/focused-with-control/`; its log is
  `build/ci-evidence/round10/focused-with-control.log`. The updated release suite,
  including all five new factory controls, passes PIT's unmutated baseline.
- Scope correction: reverted only commit `7219bbef`'s timestamp allocation warmup
  changes to the test, postmortem and lesson. Independent unchanged-main evidence at
  `650263ffe30173ed39cf0862c6d614ae070c8a8f` reports writer 1,600,048 bytes and
  retained-result control 1,600,072 bytes. This is a baseline instability, not a
  branch-exclusive regression. The original assertions and allocating controls
  remain intact, as does the earlier interval warmup repair. Evidence supplied
  by the operator: `main-allocation-comparison.md` and
  `main-baseline-allocation.xml` under the task evidence directory.
- Lesson: associate cross-class integration contracts explicitly while retaining
  bounded selection and verify the emitted consumer arguments. See [Lessons](../LESSONS.md).
