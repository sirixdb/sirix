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

## Follow-up: revision interval baseline flake

- Fix / issue: PR #1352, [CI run 43720439139](https://github.com/sirixdb/sirix/actions/runs/43720439139/job/132832004606).
- Symptom: coverage discovery completed, but `OpenRevisionsTest.test` failed without mutation.
- Root cause: the test created five wall-clock commits and expected the interval starting at
  revision 1's timestamp to include all five. Persisted timestamps have millisecond precision;
  if revisions 1 and 2 tie, floor lookup correctly starts at revision 2. Instrumentation and
  runner speed change the chance of that tie, not the contract.
- Behaviour / invariant: interval expectations must follow the last revision at a timestamp,
  independent of commit speed. Cover distinct timestamps and a tie at the starting boundary;
  retain the existing before-creation, after-latest and concurrent-commit interval cases.
- Regression / acceptance checks (specified before editing): build five revisions with explicit
  timestamps, first with a tie between revisions 1 and 2. The old five-revision expectation must
  fail deterministically. Replace the wall-clock fixture with distinct/tied timestamp cases and
  assert exact revision IDs (1–5 / 2–5), then run the complete interval and point-in-time suites
  through ordinary tests and PIT coverage alongside the fallback-expression mutation target.
  The independent oracle is the public `ResourceSession.getRevisionNumber(Instant, int)`
  floor/last-tie contract, with hand-selected timestamps;
  returning the first tie, skipping a revision, reordering or duplicating nodes must fail.
- Lesson: elapsed wall-clock time is not a fixture guarantee. Specify timestamps explicitly
  when the expected result depends on their ordering or equality; see [Lessons](../LESSONS.md).

### Revision-interval repair verification

- The tied fixture with the old five-revision expectation failed deterministically:
  `expected:<1> but was:<2>` (`build/ci-evidence/round3/tie-before.log`).
- After correction and rebase, the interval/point-in-time and fast-path regression suites,
  including the isolated lifetime and translator threshold tasks, passed 247 tests without skips.
  Query work/allocation budgets passed 332 tests (five existing opt-in skips); core budgets
  passed 325 and MCP budgets passed three. All 23 mutation-tooling tests and query Spotless
  passed. No work/allocation bound was changed.
- PIT coverage ran all eight `OpenRevisionsTest` cases, the sibling XML point-in-time suite,
  native-image configuration and translator startup guards, and the fallback-expression suite.
  Coverage completed successfully; all seven fallback-expression mutants were killed, with
  none uncovered, surviving or timed out. This is targeted evidence, not a full changed-source run.
  Logs, XML/HTML and generated JUnit execution IDs are under `build/ci-evidence/round3/`.
- Path audit: both callers of the replacement fixture execute the public query with fixed
  timestamps. The distinct case checks revisions 1–5, and the tied-start case checks 2–5,
  in order, followed by end-of-sequence. The existing interval and XML/JSON point-in-time
  fixtures use explicit boundaries outside the commit interval or explicit custom timestamps;
  none retain the removed assumption that rapid commits must have distinct timestamps.
- Rebased the eight feature/fix commits onto `21cef670d56a6ef25bef835d5fef16bc8b0c8f6d`,
  whose dependency squash already contains their earlier branch ancestry. Preserve the base's
  corrected grouping documentation and all lessons, independent translator thresholds and
  startup-test exclusions, alongside the prior PIT argument, property and heap repairs.
  Blob checks preserve all 45 base-only and 14 feature-only files exactly; the four overlaps
  were checked individually. No production path was added or removed by the interval repair.
- The outer executor still owns publication and full hosted changed-source PIT confirmation.

## Follow-up: shared execution deadline (decision required)

- Issue: PR #1352 at `e5b2c72682dea5f803c2b2c8e0934efea2996197`,
  [CI run 43730487790](https://github.com/sirixdb/sirix/actions/runs/43730487790/job/132862910158).
  This finding remains unresolved; no passing mutation score or automatic repair is claimed.
- Symptom: `Shared 1320s execution deadline exceeded`. The downloaded artifact contains an
  empty `mutations.xml`, not a survivor report. Coverage completed in 852 seconds; PIT created
  43 mutation units at 15:55:20 UTC and the process group was killed at 15:59:57 UTC.
  The preceding Gradle compilation also consumed the shared deadline.
- Invariant: every selected mutation must complete against the relevant deterministic tests
  within the unchanged shared 1,320-second execution budget, and incomplete evidence must fail.
  This applies to workflow invocation, `run`/`analyze_module`/`execute`, source and test selection,
  coverage discovery, mutation units and XML/blocking reporting, across all supported modules,
  Java/Kotlin outputs and nested classes. The timeout/reporting boundary already fails correctly.
- Root cause established by the logs and inventory: whole-source selection expands seven changed
  sources to 64 exact classes and 11,649 mutants. Of these, 9,803 belong to the executor's single
  [default, unlimited-size mutation unit](https://pitest.org/quickstart/maven/#mutationunitsize).
  Baseline discovery and unit construction consume most
  of the hosted budget, leaving 277 seconds for mutant execution and reporting. Prior launcher,
  heap and timestamp repairs do not address this workload. The evidence does not establish that
  a particular batching or fixture change would make the full gate pass within its current limit.
- Why earlier checks missed it: focused helper/fallback mutation runs validated individual
  repairs, but did not demonstrate completion or coverage of the full changed-source selection.
- Diagnostic acceptance checks, specified before any orchestration edit: run PIT's real dry-run
  consumer with the artifact's complete target list and unchanged `*` test selection; retain
  coverage, inventory and logs. Inspect mutation distribution and covering test identities
  before considering a selection or scheduling change. No production/configuration edit followed.
- Evidence: `build/ci-evidence/round4/inventory.gradle` adds only diagnostic `--dryRun=true` and
  line-coverage export, plus a local compiler-worker heap cap. The invocation used `heavy.sh`,
  two Gradle workers, a 2 GiB Gradle heap and the checked-in PIT configuration. Free disk on `/`
  was 81 GiB before Gradle. All selected tests passed baseline coverage; the generated inventory
  contains 11,649 `NOT_STARTED` entries. Dry run executes no mutants and is not a bounded gate
  pass, a kill score, or evidence that any mutant survived. Its exported line coverage is 78%.
- Exploratory result: matching exported class/method/block identities finds 2,231 mutants with
  no covered block. This is a coverage-gap inference from the two generated protocols, not a
  PIT `NO_COVERAGE` result from mutant execution. Examples include `MixedAgg.maxBd`/`minBd`
  and `ScanDiagnostics.mergeFrom`. Runtime optimization alone would not supply those assertions.
  All 258 covering test-class identities belong to the query module's compiled test output;
  integration tests in `io.sirix.access` packages also belong to that output. Package names alone
  are therefore not a safe module boundary. Discovery examined 1,111 test classes overall.
- Existing regression evidence: all 23 Python tooling tests pass, including timeout/partial-XML
  rejection in `ReportingTest`. Inventory, coverage-identity analysis and summaries are retained
  under `build/ci-evidence/round4/`. No new application test was invented for this diagnosis.
- Decision required: retain whole-source gating and authorize the broader coverage/survivor and
  orchestration remediation under the existing limits, or explicitly approve a changed-code
  mutation gate with whole-source analysis retained in a separately bounded lane. The latter
  changes policy and must not be implemented implicitly. Neither deadlines, thresholds, targets,
  test selection nor production row/page kernels were changed during this diagnosis. No full
  changed-source mutation pass has been obtained.
- Lesson: focused mutation success does not establish full-selection capacity or coverage;
  obtain the complete inventory and baseline evidence before selecting a bounded-gate repair.
  See [Lessons](../LESSONS.md).
