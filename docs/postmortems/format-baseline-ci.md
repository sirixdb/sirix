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
