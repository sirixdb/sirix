# Bug: hashless resources lost diff safety and planner size signals

- Fix / issue: the node-hashes-default-off change for Sirix 1.0.
- Symptom: a caller could force the hash shortcut on a resource without stored hashes, causing changed subtrees to compare equal. Query size heuristics also saw every hashless document as empty, disabling large-document optimization routes.
- Trigger: create a resource with `hashKind(NONE)`, edit it across revisions, and invoke a diff with `DiffOptimized.HASHED` and `hashKind(ROLLING)`. Independently, ask the query statistics provider for the size of a populated hashless document.
- Root cause: `DiffFactory.Builder` defaulted to `ROLLING` independently of the resource, and `AbstractDiff` trusted that setting. Stored descendant counts share the node-hash storage gate, but four planner size heuristics assumed the count was unconditional. Changing the Java default to `NONE` exposed both assumptions on the default path.
- Why tests missed it: public diff callers normally disabled the shortcut themselves. Query result tests could not detect degraded planner decisions because the fallback execution still produced correct values. Some hash-dependent fixtures also compared two absent counts (`0 == 0`).
- Regression test: `DiffFactoryHashKindTest` covers forced and default hash kinds; `JsonDiffHashEquivalenceTest` and `XmlDiffHashEquivalenceTest` compare revision histories with hashes on and off. `DocumentNodeCountTest` requires a positive size signal and tests both the stored-count and node-frontier branches. Removing the resource-authoritative guard or replacing the fallback with the stored count violates those assertions. Run the corresponding `:sirix-core:test` and `:sirix-query:test --tests` filters.
- Lesson: gate an optimization at the resource that makes it valid, and inventory all fields gated by a configuration change. Assert a planner's decision inputs independently of result correctness. See the two hash-default entries in [Lessons](../LESSONS.md).

Mutation evidence: actual PIT kills all three `DocumentNodeCount.estimate` mutants, both
`ResourceConfiguration.defaultHashType` mutants and the resource-authoritative `AbstractDiff`
constructor guard. The builder's new default assignment has no mutation under PIT's default
mutators; its behavior is asserted for both hash kinds.

Four surviving mutations in `DiffFactory.Builder` are outside the changed logic. All affect
the existing revision-argument checks before the new hash-default assignment:

| Method | Line | Mutation | Justification |
|---|---|---|---|
| `Builder` constructor | 226 | boundary `newRev >= 0` | Unchanged argument validation; the focused hash tests pass valid positive revisions. |
| `Builder` constructor | 226 | remove `checkArgument` | Same unchanged validation, outside the hash-kind contract. |
| `Builder` constructor | 228 | boundary `oldRev >= 0` | Unchanged argument validation; the focused hash tests pass valid positive revisions. |
| `Builder` constructor | 228 | remove `checkArgument` | Same unchanged validation, outside the hash-kind contract. |

A wider class-level run also mutates legacy traversal loops and unrelated configuration methods.
Its two memory errors and one timeout are incomplete analysis of those methods, not killed mutants.
The follow-up excludes those unchanged methods and retains the changed constructor and resolver.

## Follow-up: deleted keys are not a sampling population

- Fix / issue: review finding R1 in the node-hashes-default-off change.
- Symptom: histogram collection returned `null` and registration returned `false` for a surviving price value after a large subtree was deleted from a hashless resource.
- Trigger: create `{"discard":[100000 numbers],"price":[7]}`, delete `discard`, commit, close all handles and reopen. Only three live nodes remain, but the node-key frontier still exceeds 100000; using that frontier with a 10000-value sample gives stride 5 and skips every live node.
- Root cause: a historical upper bound suitable for planner size gating was reused as a current sampling population. The sampling invariant is that only a stored live descendant count may increase the stride; an unavailable count must retain stride 1. Both `collect` overloads and both `collectAndRegister` overloads, for latest and explicit revisions and for hashed and hashless resources, reach the shared `sampleValues` boundary. The four planner size heuristics still use `DocumentNodeCount.estimate` because overestimates there do not discard samples.
- Why tests missed it: `DocumentNodeCountTest` covers positive estimates without deletion history, while `HistogramCollectorTest` previously built histograms manually rather than exercising collection from a persisted resource.
- Regression test: `HistogramCollectorTest.deletedSubtreeDoesNotHideSurvivingSample` uses NONE and ROLLING resources and the collector's supported named-array shape. After cold reopen it requires exactly one sample, one distinct value and an exact most-common-value entry of 7 with frequency 1, then requires latest and explicit-revision registration to retain that histogram. Missing-field collection must still return `null` and refuse registration. The oracle is the known single surviving input value, independently of either count source. Restoring the frontier-based sampler must fail the NONE arm while the ROLLING control remains valid.
- Verification: all 13 focused tests in `HistogramCollectorTest` and `DocumentNodeCountTest` passed. A controlled historical-source mutation replacing the stored-count read with `DocumentNodeCount.estimate` failed the NONE arm on the missing histogram and passed the ROLLING control; restoring the fix passed both. The collector now matches its pre-change source, so no unrelated formatting adds a whole-class mutation baseline. Run-local logs and JUnit XML are under `build/review-r1/`; no complete suite, lint or other pipeline phase was run.

  Focused command (preceded by a free-disk check; run with over 60 GB available):

  ```sh
  /home/johannes/IdeaProjects/firstmate/data/tools/heavy.sh ./gradlew :sirix-query:test \
    --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx3g \
    -PtestHeapMin=256m -PtestHeapMax=3g \
    --tests 'io.sirix.query.compiler.optimizer.stats.HistogramCollectorTest' \
    --tests 'io.sirix.query.compiler.optimizer.stats.DocumentNodeCountTest' \
    -x :sirix-query:bitemporalLoaderAllocationTest \
    -x :sirix-query:temporalNodeReaderLifetimeGenericTest \
    -x :sirix-query:translatorThresholdIndependentTest \
    -x :sirix-query:translatorThresholdLegacyTest
  ```
- Lesson: a historical key frontier cannot substitute for a current sampling population; keep approximate size signals out of filters that can drop all valid observations. See [Lessons](../LESSONS.md).

## CI follow-up: NAME index selection lacked assertions

- Fix / issue: PR 1455, CI run 44152463888, changed-source PIT analysis.
- Symptom: negating the nonempty-path check in `lookupIndexInfo` or the NAME-index
  predicate in `matchIndexDef` still passed the selected statistics tests. Both mutants
  were on lines included by formatting of the statistics provider.
- Trigger: create a selective NAME index, then ask `getIndexInfo` for an included
  field. Losing the leaf name or rejecting NAME definitions must lose that index.
- Root cause: the tests asserted CAS selection and the absence of indexes, but had
  no positive NAME-index assertion. The invariant is that index lookup must report
  the highest-priority matching persisted definition (CAS, then PATH, then NAME),
  without accepting unrelated names. It applies to latest and explicit revisions,
  empty and nonempty paths, and cached and uncached lookups.
- Why tests missed it: CAS lookup returns before NAME matching, while the no-index
  case cannot distinguish correct NAME handling from rejecting every NAME index.
- Regression tests: `SirixStatisticsProviderTest.indexInfoMatchesNamesAndPreservesIndexPriority`
  creates known NAME, PATH and CAS definitions and asserts their independently known
  matches, priority and unrelated-name rejection at latest and revision 2, including
  a repeated cached lookup. `nameIndexDoesNotMatchAnEmptyPath` checks the empty-path
  boundary with a NAME-only catalog at both revisions. The oracle is the persisted
  definitions explicitly created by the fixture, not the provider's matching rule.
- Lesson: cover every index kind with a positive selection assertion, not just
  queries that return the right rows regardless of the selected access path. See
  [Lessons](../LESSONS.md).

Focused verification: 11 statistics tests, three `GetHashTest` cases and four
core diff tests pass. An unsampled PIT run of `SirixStatisticsProvider` kills both
CI-reported negations (`lookupIndexInfo:178`, bytecode index 48; `matchIndexDef:216`,
bytecode index 36), with the new positive NAME assertions as the killing tests.
Eight of ten mutants on the PR's formatted/changed lines are killed. The two
additional boundary survivors are justified separately:

| Method / line | Mutation | Justification |
|---|---|---|
| `lookupIndexInfo:178` | `path.getLength() > 0` to `>= 0` | Only an empty path differs. Neither version has a NAME leaf to match; a failing empty-tail access is caught by the existing lookup boundary and yields the same `NO_INDEX` result as the NAME-only empty-path regression. Nonempty paths execute the same tail access. |
| `matchIndexDef:216` | `currentBestPriority > 2` to `>= 2` | Equality means a NAME index was already selected. The mutant can choose another matching NAME definition of the same priority. Both are valid access paths; the public contract specifies priority among index kinds, not a stable ID among equally ranked NAME indexes. CAS and PATH priorities still exclude NAME replacement. |

The remaining undetected mutants in this unsampled whole-class run are on
unchanged lines (cache lifecycle, failure fallbacks and prefix matching), advisory
under the PR lane's documented criterion. Run-local reports and logs are under
`build/ci-investigation/`; the unsampled invocation uses
`:sirix-query:pitest -PprMutationModule=sirix-query`, an exact targets file containing
`io.sirix.query.compiler.optimizer.stats.SirixStatisticsProvider`, and
`-PprMutationTests=io.sirix.query.compiler.optimizer.stats.SirixStatisticsProviderTest*`.

The Sonar maintainability report also includes three existing complex methods:
`AbstractJsonPathWalker.replaceAstIfIndexApplicableImpl` (22),
`DerefDescendantExpr.getLazySequence` (139), and
`SirixTranslator.DescOrSelf.performStep` (77). Against base `ce56286b8`, each method's
only change is replacing a stored-count read with `DocumentNodeCount.estimate`;
their control flow and complexity are unchanged. They were left unresolved in
the first CI repair. The follow-up below resolves them without a traversal refactor.
The reported unused import, redundant Jupiter visibility and ineffective builder
`transient` modifiers are removed. CodeFactor's report returns HTTP 403 through both HTTP and
the connected browser, so its specific findings could not be retrieved.

## CI follow-up: unchanged traversal complexity

- Fix / issue: PR 1455, `Run Sonarqube` run 44162810777 and its companion
  `SonarCloud Code Analysis` check.
- Symptom: the scanner completed, but the quality gate rejected maintainability
  rating C; reliability, security, 91.5% new-code coverage and duplication passed.
- Trigger: replacing the count source touches three existing methods with S3776
  complexities 22, 139 and 77, although it adds no control flow.
- Root cause: the new-code analysis attributes existing method complexity to the
  count-source correction. The invariant is that correcting the size signal must
  retain index selection, traversal order, cursor ownership and fallback behavior.
  It applies to JSON index rewrites, JSON descendant dereferences and XML
  descendant/descendant-or-self accessors, including empty/single/multiple path
  matches, same/mixed depths, arrays/fused objects, summary-disabled resources,
  latest/historical revisions and hashed/hashless configurations.
- Resolution: use the repository's method-level `java:S3776` exception convention
  on exactly these three methods, with a specific reason at each site. No global
  rule, quality-gate condition, executable statement or test expectation changes.
  The JSON/XML runtime accessors execute per context node; retaining their shape
  also avoids an unmeasured hot-path refactor. The AST rewrite retains its existing
  revision and path safety checks together.
- Why tests missed it: behavioral tests cannot detect a static-analysis gate's
  classification of unchanged complexity. The regression check must execute the
  analyzer, rather than search source text for an annotation.
- Regression checks: run S3776 with the CI analyzer against the original and fixed
  files; the three reported methods must disappear only in the fixed analysis.
  Existing descendant/index-query tests provide an independent input/output oracle;
  work-budget tests reject extra loads, scans or allocations. No new runtime logic
  is introduced to mutation-test in this follow-up; earlier count-source and index
  selection mutation evidence remains above.
- Caller trace: the PATH, NAME and CAS walkers still call
  `replaceAstIfIndexApplicable`, which delegates to the same rewrite
  body, retains dynamic-index fallback and owns only its opened read handles;
  `SirixTranslator.derefDescendantExpr` still constructs `DerefDescendantExpr`, whose
  evaluation/sequence overloads reach the same path-summary dispatch and generic
  fallback; both `DescOrSelf` axis variants still select the same simple-node-test
  fast path or superclass fallback. No alias, branch, parameter or helper becomes
  unreachable because annotations do not alter execution.
- Lesson: diagnose the precise quality-gate condition, and use a documented local
  exception for inherited traversal complexity when the fix only changes a size
  signal. See the Sonar maintainability entry in [Lessons](../LESSONS.md).

Rebase: replayed all five change commits onto `6ea5e7ede1b39f847ee382c9af8b00b145c0190a`.
The sole conflict was the lessons table; both the HOT/MCP lessons from the base
and the hash-default lessons from this change were retained.

Local analyzer verification: Sonar Java **8.46.0.59609** reproduced complexities
22/139/77 before the fix; its real `SuppressWarningFilter` excluded exactly those
three findings afterward while retaining all eight other S3776 findings in the
same files. The before/after scanner output and executable verification harness
are under `build/ci-sonar-followup/`. The module formatting check also passed.
This is local rule verification; a fresh hosted quality-gate result belongs to
the outer CI executor.

Local runtime verification: query tests and all three modules' documented work-budget
commands passed, including their required isolated JVM tasks. Across these runs,
942 cases passed and 7 existing assumption-gated cases were skipped (five opt-in
valid-time cases and two arena-lifetime cases in the diagnostic skip-mode lane);
there were no failures or errors. The query run also selected `JsonIntegrationTest`,
`XmlDescendantQueryTest`, `ArrayIndexAccessOptimizerTest` and all statistics tests.
Commands used `--no-daemon --max-workers=1 -Dorg.gradle.jvmargs=-Xmx2g`,
`-PtestHeapMin=256m -PtestHeapMax=2g -Dsirix.allocator.maxSize=512M`;
no work-budget bound changed. Logs and per-suite XML summaries are retained in
`build/ci-sonar-followup/`. `git diff --check` and the pinned-base ancestry check pass.
