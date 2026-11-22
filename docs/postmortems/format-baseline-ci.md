# Bug: format compatibility proof rejected resource aliases

- Fix / issue: PR #1480, FMT-401; CI run `45041701813`.
- Symptom: Linux derived-index alias tests lost the registered derived resource; macOS,
  Windows and Linux core lanes also failed tests whose rejection boundary had moved.
- Trigger: rename the derived resource and retain its original name as a filesystem alias;
  open the source, which verifies the original path then opens the canonical path.
- Root cause: `Proof.requireResource` compared path spellings rather than filesystem identity.
  The proof must admit aliases of the verified resource and refuse every different resource.
  Both persistent storage constructors and derived rebuilds consume this same proof.
- Why tests missed it: targeted format tests used one spelling per resource. The existing
  `DerivedIndexMaintainerTest.filesystemAliasesPreserveHiddenOwnershipAndCanonicalSourceRemoval`
  exercised the missing case in the broader Linux lane. The mutation lane also omitted
  `FormatRepairBoundaryTest`, leaving its bounded historical-root behavior unobserved.
- Regression tests: the existing alias test and
  `FormatRepairBoundaryTest.compatibilityProofAcceptsAliasesButRefusesOtherResources`.
  The latter checks ordinary paths, an equivalent dot path, and an independently created
  other resource on both persistent backends. FMT-401 supplies the ownership oracle;
  both rejecting a same-resource alias and accepting an unrelated resource are wrong.
- Sibling test corrections: `PageEnvelopeContractTest` now expects the FMT-401 named version
  exception for every page kind. `RevisionRootPageHashValidationTest` and
  `SparseRevisionHistoryTest` acquire storage before injecting corruption so their existing
  direct-reader and index-loader assertions still execute; the missing-hash test separately
  asserts refusal by the opening gate. Their corruption oracles and index-publication
  assertions are unchanged.
- Mutation selection: associate `FileChannelReader` and `ResourceFormatCompatibility` with
  the existing repair-boundary behavior tests. This supplies tests rather than a waiver for
  CI's surviving record-position multiplication mutant.
- Lesson: preserve resource identity across aliases, and keep direct-consumer corruption
  tests reachable when adding earlier validation; see [Lessons](../LESSONS.md).

Verification commands and outcomes are recorded below after the local runs complete.
CodeFactor's status reports one issue, but its details page returns HTTP 403 through both
HTTP retrieval and the shared browser; its cause cannot be established from that status.

The association-validator regression preserves the four existing query contract pairs while
allowing additional valid mappings. The real association loader validates every entry against
module source/test declarations; no raw source-text assertion was added.

## Verification

- The local control run reproduced all three Linux alias failures (71 selected tests,
  three failures); the adjusted reader/index and envelope tests passed. This run used the
  original compiled proof. The upstream run independently shows the same three alias failures.
- The mutation-helper suite passed all 156 tests after allowing additional valid association
  pairs; the preceding run failed its whole-map equality assertion.
- The mutation selector validates both added associations and selects the repair-boundary
  suite in the full PR scope (28 test patterns).
- Ordinary-path trace: identical paths retain the allocation-free equality fast path; aliases
  use `Files.isSameFile`; different resources still throw before storage initialization or
  derived deletion. Both storage backends consume `Proof.requireResource`, as does the
  derived rebuild. Descriptor-bound `RepairProof` continues to require the exact verified
  channels. No caller, parameter, or branch became unreachable.

- Focused PIT: 11 mutations, 10 killed, one uncovered on unchanged
  `FileChannelReader.readRegionsFromChunk` code (line 578, advisory). The original
  changed-line multiplication/division survivor in `requireCompatibleRevisionRoots`
  is killed by `FormatRepairBoundaryTest.discoversAllSalvageBeforeHealingAnyRecord`.
  Both negated-condition mutations of the corrected proof predicate are killed by
  `compatibilityProofAcceptsAliasesButRefusesOtherResources`. No dispositions added.
  Report: `build/ci-diagnosis/pit-report/mutations.xml`; log: `build/ci-diagnosis/pit.log`.

Reproduce the alias/ownership regressions with:

```bash
./gradlew :sirix-core:test \
  --tests 'io.sirix.access.FormatRepairBoundaryTest' \
  --tests 'io.sirix.index.aggregate.DerivedIndexMaintainerTest'
```

- Final regression/work-budget selection: `build/ci-diagnosis/final-tests.log`, successful
  in 6m 32s. All 530 main tests passed, including the affected test classes, every
  `io.sirix.format.*` test, every `io.sirix.budget.*` test, projection work budgets and
  `RefusedOverflowLeafTableTest`. No bounds changed. The isolated suites total 55 tests
  with no failures/errors and two existing arena-policy skips (585 tests overall).
- The first expanded invocation stopped before the main task because the isolated
  lookup-cache task found no tests while PIT preparation overlapped it. Its standalone
  rerun passed all four tests (`build/ci-diagnosis/lookup-after.log`); the successful
  final main invocation excluded that separately completed task with
  `-x :sirix-core:validTimeDisabledLookupCacheTest`. Builds were then run sequentially.
- Formatting: the targeted formatter processed the five touched Java files; final
  `git diff --check` passed. No full cross-platform rerun, remote check trigger,
  pipeline control, commit, or push was performed in this assigned CI phase.
- Remaining check: CodeFactor's GitHub status says “1 issue found,” but its diagnostic
  URL returns HTTP 403 through both HTTP retrieval and Chrome. The issue's location
  and rule could not be established, so it remains unresolved; no speculative
  quality refactor was made.

## CI follow-up: rebase onto the required base

- Rebased the format branch onto `b2f7599e4f513dc434407ae7bd832903da8cfa63`,
  preserving its IN_MEMORY lifetime/retirement changes and the format gates.
- The old history contained a tree-neutral merge recording the pre-rebase initial
  format commit. Replaying that second copy conflicted with its already-applied
  corrections; it was skipped without dropping any unique change.
- The remaining conflict was the association-validator regression: both branches
  had corrected the whole-map equality assertion. Retained the base's membership
  checks, which require all four query contract pairs while permitting additional
  valid tests and associations. The actual loader still validates every entry.
  The existing lesson about configuration-coupled test fixtures applies; no new
  runtime behavior or test oracle was introduced.
- Re-traced the combined paths: session creation retains the base's IN_MEMORY
  bookkeeping and this PR's resource-bound proof before orphan-spill cleanup;
  derived rebuilding still verifies compatibility before removal; tool discovery
  still excludes retired directories before opening resources. All production
  changes from the format branch replayed without manual conflict edits.
- CodeFactor still reports failure through GitHub, but its diagnostic page returns
  HTTP 403 and the web reader cannot access it. There is no rule or source location
  available to diagnose, so the check remains unresolved rather than being
  classified as an external failure or fixed through speculative refactoring.
  The GitHub commit-status API confirms status `59599474877` belongs to the supplied
  target `52bb35256d4d7fbcd6dd7cdad24153f79a5e10ba`, says “1 issue found,” and
  supplies only the inaccessible provider link; it is not proven stale.
- The 156 mutation-tooling behavior tests pass after conflict resolution.
  Command: `python3 -B -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'`.
- Rebased Java verification passed in 6m 40s: 668 main tests and 55 isolated
  allocation/startup/storage-epoch tests, zero failures/errors and two existing
  arena-policy skips. Selection covered `FormatRepairBoundaryTest`, `io.sirix.format.*`,
  interrupted-first-commit recovery, resource-session factories, derived maintenance,
  both IN_MEMORY lifetime suites, RAM storage contracts/committed revisions, all core
  work budgets, projection work budgets and `RefusedOverflowLeafTableTest`.
  Log: `build/ci-rebase/core-tests.log`; per-task counts: `build/ci-rebase/test-results.json`.
- `git diff --check` passed, and the specified base is an ancestor of rebased HEAD
  `0b9ca2a13`. Manual conflict resolution changed no production logic, so the prior
  focused mutation evidence remains applicable; no new disposition was added.
  No push, remote check trigger or pipeline control was performed.

## CI follow-up: canonical paths in the discovery budget

- Fix / issue: PR #1480, CI run `45054105755`, macOS core job `137308946962`.
- Symptom: both `FormatDiscoveryWorkBudgetTest` backend cases report zero I/O despite
  one real compatibility proof; the other macOS core tests have no failures.
- Trigger: the observer's temporary-directory spelling differs from the canonical resource
  path used during open (macOS temporary paths can be filesystem aliases).
- Invariant: discovery reads of the fixture's files must be counted regardless of path
  spelling, on both persistent backends and in normal and repeated-discovery captures.
- Root cause: the test observer compared directory `Path` values rather than filesystem
  identity and therefore returned unobserved channels for an alias. Production discovery
  and its proof succeeded; the positive lower bound correctly rejected missing evidence.
- Why tests missed it: Linux's original fixture used one directory spelling throughout.
- Regression: retain a dot segment in the fixture's database path. On Linux this reproduces
  the same zero-byte failure for FILE_CHANNEL and MEMORY_MAPPED before correcting the
  observer. The existing test executes cold public database opens, asserts preserved
  revision/content, bounds actual I/O and repeats real discovery as an excess-work control.
  FMT-401 and CONV-301 supply the behavior and cost oracles; silently unobserved I/O and
  doubled discovery are the wrong behaviors it rejects. No budget is widened.
- Fix: use `Files.isSameFile` at the observer's shared file-open boundary. All data/revision
  channels and both capture modes use that boundary. No production code changes.
- Path audit: ordinary and aliased opens return observed real channels; data and revision
  reads still update their separate counters; closing each capture still closes both static
  mocks. No helper, branch or caller became unreachable.
- Vision check: restores portable evidence for “Speed is part of correctness”; introduces
  no engine behavior or new default.
- Evidence: `build/ci-round4/budget-before.log` and `budget-before.xml` reproduce both
  failures with the old observer and the dot-segment fixture. Verification results follow.
- CodeFactor status `59601378674` belongs to the supplied target `157ca06107399a20308ad6b47b99c6730779bb88`
  and reports one issue. Its diagnostic URL still returns HTTP 403, without rule/location;
  it remains unresolved, not proven external or stale.

The downloaded partial PIT report also contains an uncovered changed-line mutant in
`ResourceConfiguration.requireResourceIdentity`. The already-existing FMT-102 behavior
tests in `FormatVersionBaselineTest` exercise zero settings UUIDs and builder identities,
but the lane did not select that suite. Added it to the existing configuration association;
all 156 mutation-helper tests pass. The mutation scope and time budget are unchanged.

The verbose reproduction retained the original 81 targets, 30 test patterns, four-mutant
sampling and worker limits. Its baseline passed (including the corrected work budget).
Worker dumps then showed the full `DerivedIndexMaintainerTest` being executed during
`MutationTestMinion.findTestsForTestClasses`, before mutation timeout enforcement. Only
two workers completed in the first several minutes; this reproduction was stopped after
capturing the stacks rather than waiting for another deadline failure. Evidence is in
`build/ci-round4/pit.log` and `minion-*-*.txt`.

Removed the redundant `DerivedIndexMaintainerTest` association from the shared proof:
`FormatRepairBoundaryTest.compatibilityProofAcceptsAliasesButRefusesOtherResources`
already executes the proof's alias and ownership contract on both backends, and that suite
retains the derived-rebuild/refusal scenarios. The full maintenance suite remains in
ordinary CI. The reader, proof and settings associations still select the focused repair
tests; the discovery budget stays associated with the proof, and the settings association
also selects the existing baseline contract suite. Production target selection, mutant
sampling, verdict and shared deadline are unchanged. The real blocking reporter was rerun
with the resulting selection, not a manually reduced production target list.

### Follow-up verification results

- The complete core PIT analysis finished in 20m 42s within the unchanged 1,320-second
  share, with the original 81 production targets and four-mutant sampling. All 13
  changed-line mutants were killed, with zero surviving or uncovered changed-line mutants
  and no new dispositions. The previously uncovered UUID check is killed by
  `FormatVersionBaselineTest.zeroResourceUuidSettingIsRefusedBeforeAnyMutation`.
- PIT generated 214 total mutants: 112 killed, two timed out, 52 surviving and 48 uncovered;
  the latter survivors/uncovered mutants are all on unchanged lines (advisory).
- Evidence: `build/ci-round4/selection/sirix-core/report/mutations.xml`, `pitest.log` and
  `build/ci-round4/selection/summary.md`. The first reporter exit additionally flagged the
  two existing CLI dispositions because only core classes had been compiled locally;
  CI's Build stage compiles all modules. The prerequisite compilation and recheck succeeded as recorded below.
- All 156 mutation-helper tests pass with the final associations (`helper-final.log`).

- Ordinary Gradle verification passed all 325 selected main-task tests, zero failures/errors
  and no skips, in 2m 20s. This includes both discovery-budget backend cases, all 39
  repair-boundary cases, all nine baseline cases, every `io.sirix.format.*` and
  `io.sirix.budget.*` test, and all 69 derived-maintenance cases. Both history sizes and
  excess-work controls pass with the dot-segment fixture; bounds are unchanged.
  Command: `./gradlew :sirix-core:test --tests 'io.sirix.budget.*' --tests
  'io.sirix.access.FormatRepairBoundaryTest' --tests 'io.sirix.format.*' --tests
  'io.sirix.index.aggregate.DerivedIndexMaintainerTest' -x
  :sirix-core:validTimeDisabledLookupCacheTest -x :sirix-core:dictionaryReadViewAllocationTest
  -x :sirix-core:storageEpochStoreIdentityTest`. Log: `build/ci-round4/core-after.log`;
  counts: `build/ci-round4/test-counts.json`. The three unrelated isolated suites were
  excluded from this focused invocation.

- `./gradlew :sirix-kotlin-cli:classes` succeeded in 32s, supplying the existing CLI
  classes absent from the initial local core-only build. The disposition recheck then
  passed (exit 0), with both unchanged entries out of the mutated scope and zero failing
  entries. Command: `python3 .github/scripts/pr_mutation.py dispositions --check --base
  b2f7599e4f513dc434407ae7bd832903da8cfa63 --report
  build/ci-round4/selection/sirix-core/report/mutations.xml --sampled`. Logs:
  `build/ci-round4/cli-classes.log` and `dispositions-after.log`. No disposition was edited
  or waived; the original full-driver exit 1 reflected only that local build prerequisite.
- Final `git diff --check` passed. CodeFactor remains unresolved because its diagnostics
  return HTTP 403 without identifying a rule or location. No remote workflow, push, PR
  update or pipeline-control command was executed in this assigned phase.
