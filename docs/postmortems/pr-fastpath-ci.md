# Bug: fast-path CI cannot complete its quality checks

- Fix / issue: PR #1352, CI run 43706774317.
- Symptom: PIT exits before coverage collection; Sonar rejects new maintainability debt (coverage is 93.8%).
- Trigger: the query test task supplies a native-image system property containing a comma; the PIT command-line transport splits it into two JVM arguments.
- Root cause: inherited JVM arguments must remain single arguments through PIT's comma-separated option parser. The shared PR configuration did not encode embedded commas or quote values for PIT's Java argument file, and copied properties before late configuration completed. The native-image guard also populated two properties only in `test.doFirst`, so PIT never received them. Separately, new refusal branches increased already complex dispatch/scan methods rather than naming the decision boundaries; summary admission also retained an average route that always declined later.
- Behaviour / invariants: preserve every inherited JVM argument and publish native-image test properties before either test runner consumes them, route priority, cache publication, source scoping, typed folds and explicit-refusal propagation. Grouping (single/multiple keys), aggregates (predicated/unpredicated), and summary serving must retain the generic evaluator's values/errors. No new per-row objects or scans.
- Error cases: commas and spaces in system-property values must not create a JVM main-class argument or launcher option; unrelated query failures must not become fallback; mixed types and order-sensitive double folds must still refuse.
- Why tests missed it: ordinary Gradle tests do not cross PIT's argument parser; result tests do not measure maintainability.
- Regression / acceptance checks (specified before editing): run PIT with the inherited native-image property and `VectorizedFallbackExprTest`, first with the original shared configuration (expect launcher failure), then the correction (expect coverage and mutation report). Also run `NativeImageDowncallConfigTest` through PIT in both portable and opted-in configurations; the original baseline fails its main/smoke-image assertions. Use `VectorizedSpiFailureTest` for the storage-error envelope and `BulkPathStatsSummaryServedTest` for the positive summary-serving witness. Reuse `VectorizedReleaseCorrectnessTest`, `NoPathSummarySourceScopeDifferentialTest`, `TypedGroupByDifferentialTest` and the query/core work budgets as behavioural oracles for extraction-only cleanup. Assertions derive from generic execution, fixed expected values, and documented work limits.
- Path audit: PIT now reads final test properties, protects commas through its CLI, and quotes whitespace/backslashes/quotes through its Java argument file; normal Gradle tests consume the same values. Aggregate validation still precedes cache publication, explicit refusals cross all three wrapping entry points unchanged, and ordinary failures keep their cause/code. Summary average declines at admission, removing its unreachable late checks while preserving decimal evaluation.
- Lesson: encode at the shared command-line boundary, and keep routing, cache lookup, worker traversal and result validation as separately named decisions; see [Lessons](../LESSONS.md).

## Local verification

- Original PIT configuration reproduced the reported `PosixFadvise$AdviceCall` main-class failure. After fixing transport, the existing native-image guard exposed two missing late properties; both guard failures were reproduced before their fix.
- PIT with `NativeImageDowncallConfigTest` and `VectorizedFallbackExprTest` passes with both `-Pnative.preinitializeDowncalls=false` (default) and `true`; all 7 fallback-expression mutants are killed.
- Focused PIT over `vectorizedFailure`, `completeSourceGroupByCount`, `checkedAggregateResult` and `hasPureNumericStats` kills all 18 mutants, with none uncovered. Tests: `VectorizedReleaseCorrectnessTest`, `TypedGroupByDifferentialTest`, `VectorizedSpiFailureTest`, `BulkPathStatsSummaryServedTest`. Unchanged constructor/other methods are outside this focused run; the CI changed-source lane retains its full scope and thresholds.
- Final combined query regression/budget run: 535 tests, zero failures (5 existing skips). Core budgets: 325 tests, zero failures. MCP budgets: 3 tests, zero failures.
- The query, core and MCP work/allocation-budget commands are those in [Verification](../VERIFICATION.md#running-the-layers). Local test logs and PIT XML/HTML are retained under `build/ci-evidence/` in the repair worktree. The first overlapping local Gradle runs invalidated class/jar reads; those suites were rerun without overlapping writers.
- The outer executor must rerun the full changed-source PIT lane and SonarCloud on the resulting commit; local checks do not attest those remote gates.

## Follow-up: coverage JVM heap exhaustion

- Fix / issue: PR #1352, [CI run 43713039413](https://github.com/sirixdb/sirix/actions/runs/43713039413/job/132809996024).
- Symptom: after argument transport was repaired, PIT's coverage JVM exited with
  `UNKNOWN_ERROR` before producing mutation XML.
- Trigger: the module-wide baseline includes `ClickBenchStringDecodeRouteEvidenceTest`.
  Its 300,000-row corpus exercises more than 256 projection leaves; its generic grouping
  oracle exceeds the PR runner's 1,536 MiB heap.
- Root cause: the shared PR configuration capped test JVMs below the 3 GiB used by the
  smallest ordinary query CI lane. The coverage and mutation JVMs consume the same
  `jvmArgs`, so both need that baseline capacity. The PIT reporting JVM does not execute
  the tests and needs no change.
- Why earlier checks missed it: focused mutation runs exercised small fixtures and the
  native-image configuration guard, without this existing large differential oracle.
- Behaviour / invariant: both coverage discovery and mutant execution must be able to run
  the unchanged deterministic test baseline. Apply the correction at their shared JVM
  configuration for all PR modules; preserve test selection, corpus sizes, mutation
  thresholds and the shared execution deadline.
- Regression / acceptance checks (specified before the configuration edit): the exact
  CI targets with module-wide tests reproduce `OutOfMemoryError: Java heap space: failed
  reallocation of scalar replaced objects`, followed by `UNKNOWN_ERROR`. A live heap
  histogram and thread dump identify the generic q33 grouping oracle at the exhausted
  1,536 MiB limit. Rerun `ClickBenchStringDecodeRouteEvidenceTest` through PIT alongside
  `VectorizedFallbackExprTest` with the corrected limit: require successful coverage and
  XML, and all seven fallback-expression mutants killed. The differential test's oracle
  is the generic evaluator, with independent route counters; it rejects changed query
  results and accidental fallback. Restoring the undersized heap is the configuration
  mutation this regression must reject.
- Lesson: include the largest selected deterministic fixture when validating a bounded
  mutation runner; focused mutation success does not establish baseline capacity. See
  [Lessons](../LESSONS.md).

### Follow-up verification

- The original configuration reproduced the Java heap exhaustion with the exact CI
  class list and module-wide test selection. Evidence: `build/ci-evidence/round2/full-before.log`,
  `heap-histogram.txt` and `full-threads-6.txt` in the repair worktree.
- With the corrected configuration, PIT ran the entire
  `ClickBenchStringDecodeRouteEvidenceTest` and `VectorizedFallbackExprTest` baseline,
  produced XML, and killed all seven fallback-expression mutants (none survived or
  timed out). The live coverage JVM reported `MaxHeapSize=3221225472`.
  Evidence: `build/ci-evidence/round2/heap-after.log`, `heap-after-vm-flags.txt` and
  `heap-after-report/mutations.xml`. The mutation tooling's 23 Python tests also passed.
- Path audit: the shared `jvmArgs` provider supplies the same corrected heap to coverage
  discovery and both mutation workers, after preserving the inherited module arguments
  and late properties. The normal Gradle test task and PIT reporting JVM retain their
  existing configuration; no branch or helper became unreachable.
- A broader local acceptance run used the exact CI class list and module-wide baseline,
  with one mutant per class for the subsequent smoke analysis only. It passed the q33,
  q13 and 300,000-row route checks that the original run could not finish. It did not
  complete: the host kernel's global OOM killer terminated coverage PID 194591
  (`anon-rss:5452044kB`), and PIT reported a socket EOF. The tool runtime was also replaced
  during this interval. This is incomplete evidence, not a green full mutation report;
  the outer executor must confirm the full CI lane. The checked-in mutation scope and
  thresholds were not reduced.
