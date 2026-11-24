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

## CI follow-up: identify CodeFactor's rule and preserve the rebase

- Rebased all 20 branch commits onto `ed2c177fea2b332f4751dcfdcb9a4c91bec87357`.
  Only two additions at the top of `docs/LESSONS.md` conflicted; each resolution retained
  both sides. Both changelogs merged automatically and retain both branches' entries.
  The range comparison records identical patches for all production and test changes;
  only the lessons' surrounding context differs. Evidence: `build/ci-round6/range-diff`.
- The previous HTTP attempts did not expose CodeFactor's diagnostics. A repository
  request with `Accept: application/json` and `User-Agent: Mozilla/5.0` returned its
  HTML response containing the complete `app.value('model', ...)` JSON payload.
  The PR response reports exactly one active issue: `Complex Method`, complexity 17,
  in `ResourceFormatCompatibility.verify`, lines 68–147, attached to revision
  `157ca06107399a20308ad6b47b99c6730779bb88`. The fixed-issues list is empty.
  Responses and parsed models are under `build/ci-round6/` (`json-accept.json`,
  `pr-fixed.json`, `repository-model.json`).
- Cross-check: CodeFactor's `/file/main/bundles/sirix-core/src/main/java/io/sirix/io/ResourceFormatCompatibility.java`
  returns 404; the specified base also has no such file. This issue belongs to a class
  introduced by this PR, rather than to a pre-existing main issue. Its source is identical
  in the provider's pinned revision and the supplied target `581825088`; the older entry
  therefore still applies. No unrelated active
  entry was reported by this response, and no unrelated code was changed.
- Root cause: `verify` combined bootstrap-prefix validation and ordinary envelope discovery
  in one method. Extracted only the bootstrap block into `verifyBootstrap`, preserving
  every check, its order, exception behavior and allocations. This removes the nested file
  check and two exception handlers from the reported method. Ordinary opens make no new
  call; no per-envelope loop moved. Both methods are below the reported complexity threshold.
- Invariant and sibling audit: both file identities precede every proof, with settings UUIDs
  on the ordinary path and a cross-file UUID check on the unreadable-settings path.
  Both persistent backends, derived rebuilding, orphan cleanup and interrupted/bootstrap
  recovery still enter through the same gate. Missing files retain their existing handling;
  present foreign headers still refuse before proof issuance. The incomplete-frame table,
  bounded discovery, alias handling, settings JSON cases and all proof consumers are unchanged.
  Bootstrap prefix refusals still propagate, non-version incomplete-tail failures still
  permit the existing proof, and decoded revision-zero missing-record recovery retains
  its separate fallback. No branch, parameter or helper became unreachable.
- Regression oracle: existing `FormatRepairBoundaryTest` and `FormatVersionBaselineTest`
  assert FMT-401/FMT-102 refusal and whole-resource preservation; their ordinary recovery
  controls reject an extraction that blocks compatible stores. `FormatDiscoveryWorkBudgetTest`
  retains its cold opens, both backends, positive controls and original bounds. This is a
  behavior-preserving extraction, so no test that merely inspects method shape was added.
- Vision check: preserves the refusal contract and counted open work, with no new behavior
  or default. The named `sirix-repair-format-gate` gap and FMT-402's three gaps remain intact.
- Lesson: a provider status is not a diagnosis; retrieve the embedded issue model and
  establish file/revision attribution before changing code. See [Lessons](../LESSONS.md).

### Verification

- Before the extraction, the rebased tree passed 562 main tests and 55 isolated tests,
  with zero failures/errors and two existing arena-policy skips. Selection included all
  core work budgets, projection budgets, refused-overflow checks, format contracts,
  repair boundaries, interrupted recovery, session factories and derived maintenance.
  Log: `build/ci-round6/core-tests.log`; counts: `rebase-test-counts.json`.
- All 156 mutation-helper behavior tests pass (`build/ci-round6/helper-tests.log`).
- The final bootstrap-only extraction passed Spotless, compilation and all 351 selected
  main tests in 3m 2s, with zero failures/errors/skips. Selection: `io.sirix.budget.*`,
  `FormatRepairBoundaryTest`, `io.sirix.format.*`, `InterruptedFirstCommitRecovery*`.
  The independently completed lookup-cache, dictionary-allocation and storage-epoch
  tasks were excluded from this focused run; other startup prerequisites completed.
  Log: `build/ci-round6/bootstrap-after.log`; counts: `bootstrap-test-counts.json`.

- Unsampled focused PIT completed in 3m 34s: 76 mutants, 52 killed, 24 survivors,
  zero uncovered mutants. All five mutants on the new `verifyBootstrap` method and
  its caller are killed by the existing public recovery/preservation tests:
  `beaconlessForeignEnvelopePreventsBootstrapTruncation`,
  `incompleteBootstrapTailRecoversWithoutForeignVersion` and
  `zeroDataHeaderCannotHideForeignRevisionsFormat`. Report:
  `build/ci-round6/pit-report/mutations.xml`; extracted records: `extraction-mutants.json`.
  Reproduce with `:sirix-core:pitest -PprMutationModule=sirix-core`, target class
  `io.sirix.io.ResourceFormatCompatibility`, and test patterns `FormatRepairBoundaryTest*`,
  `FormatVersionBaselineTest*`, `FormatDiscoveryWorkBudgetTest*` in their declared packages.
- Deliberately left: the unsampled survivors affect unchanged identity/feature/root checks
  in `verify` and `verifyRepair`, and unchanged bounds/arithmetic in `requireEnvelope`,
  `requireFramedPrefix` and `readFully`. They are not CodeFactor findings or changes made
  by this repair; the report does not establish a clean whole-class mutation verdict.
  No disposition, waiver, test weakening or unrelated gate refactor was added.
- Final `git diff --check` passes. The required base is an ancestor, all 20 branch commits
  remain, and both sides of the lessons conflicts are retained. The repair changes only
  the bootstrap extraction, this write-up and the existing CodeFactor lesson.

No push, PR state change, remote workflow trigger or pipeline-control command was performed.
CodeFactor's remote confirmation remains for the outer executor after publication.

## CI follow-up: rebased mutation attribution and native builder failure

- Fix / issue: PR #1480, run `45068208787`, jobs `137350207566` (PIT) and
  `137349035057` (query native image).
- Rebased all 22 branch commits onto `350cf2511dd050e02cfca58d85c7ab68492950b2`.
  Retained both changelogs' entries, both sides' lessons, the format decision table,
  aliases/settings cases, recovery tests, work budgets and their registrations. The base's
  microsecond timestamp implementation and precision contracts remain. The envelope test
  retains the branch's named refusal and the base's timestamp-preserving rewrite control;
  mutation associations retain both branches' mappings. `build/ci-round8/range-diff`
  records the replay; no production conflict required manual resolution.
- PIT symptom: the downloaded complete report has 13 changed-line mutants, 12 killed
  and one survivor: the conditional boundary in `ResourceFormatCompatibility.readFully`.
  This is a changed-line survivor, not advisory unchanged-line debt or a selection stall.
- Invariant: a compatibility read must make progress or refuse that check, rather than
  retrying unread bytes. The same helper reads the unreadable-settings cross-file identity,
  direct-repair data/revisions identities, frame lengths, envelope payloads and bootstrap
  prefix lengths. Existing bootstrap catches still apply the incomplete-frame decision
  table; this repair changes neither recovery nor refusal behavior.
- Root cause / why tests missed it: all selected fixtures used successful regular-file reads
  or EOF; none exercised the zero-byte result admitted by the channel interface. The
  production guard already rejects both zero and negative results. Its boundary mutation
  admits zero and retries, so the existing tests could not distinguish it.
- Regression: `FormatRepairBoundaryTest.envelopeDiscoveryRefusesReadsWithoutProgress`
  uses a real persisted compatible revision-root frame and verifies a normal public
  `requireRootEnvelope` call succeeds. A channel wrapper injects one zero-byte read at
  the frame-length or payload boundary, then delegates subsequent reads to the real file.
  FMT-401's progress/refusal contract supplies the oracle: each injected case must refuse
  and preserve the whole resource. Retrying and accepting the frame is the plausible wrong
  behavior. No private state, source-text assertion, wall-clock limit or mock-call assertion
  supplies the result. The existing lane association selects both cases.
- Path trace: positive reads advance the buffer until complete; EOF and zero refuse in
  the shared helper before the direct check returns. Both persistent backends and every
  proof consumer retain their existing paths. No production branch, helper, alias or
  parameter changed or became unreachable. No selector, sample, timeout, target or budget
  bound was changed, and no mutation disposition was added.
- Lesson: cover incomplete reads as well as incomplete files; see [Lessons](../LESSONS.md).
- Vision check: preserves the counted open-work contract and format refusal semantics,
  with no engine behavior or default added. The named full-scan gap remains deferred.

### Native-image attribution

The complete job log fails inside GraalVM's `SimulateClassInitializerSupport.publishResults`,
publishing contradictory simulation results (`true` and `false`) for Netty's
`TypeParameterMatcher`; its cluster also contains `Socks4ClientEncoder` and the anonymous
matcher initializer. This is an internal builder invariant failure, not a missing reflection
entry, missing downcall registration, forbidden image-heap handle, or application exception.
The [GraalVM implementation](https://github.com/oracle/graal/blob/release/graal-vm/25.0/substratevm/src/com.oracle.svm.hosted/src/com/oracle/svm/hosted/classinitialization/SimulateClassInitializerSupport.java)
reports this error when two simulation publications disagree.

Audited both new types first: neither `ResourceFormatCompatibility` nor
`UnsupportedFormatVersionException` has static fields or a class initializer (`javap`
output: `build/ci-round8/new-types.txt`), native-handle aliases, reflection-based construction
or a new downcall signature requiring metadata. Native properties, downcall metadata and
query native build configuration are unchanged. The preceding query image job
`137305419665` succeeded with the same GraalVM `25.0.4+7.1`, Netty `4.2.15.Final`,
metadata versions and four builder threads. These observations identify an upstream
simulation inconsistency; they do not establish its exact scheduling trigger.

The local `./gradlew :sirix-query:nativeSmokeTest --no-daemon --info` completed successfully
in 6m 8s, building the executable and passing all 49 native tests. Its analysis registered
the same 24 downcalls and one upcall. This uses the GraalVM 25.0.3 already on PATH, rather
than CI's 25.0.4; it supports the builder-failure attribution but is not a reproduction on
the exact CI compiler patch. Logs: `build/ci-round8/native-local.log`, `upstream-native.log`
and `previous-native.log`. No build-policy workaround, job exclusion, weakened runtime
check or CodeFactor change is included.

### Rebased verification

- All 158 mutation-helper behavior tests pass (`build/ci-round8/helper-tests.log`).
- The two new public-envelope progress cases pass against the existing guard
  (`progress-baseline.log`, `progress-baseline.xml`). Injecting the exact reported
  `<= 0` to `< 0` mutation makes both cases fail: expected `SirixIOException`, but
  no exception was thrown (`progress-injected.log`, `progress-injected.xml`). The
  injection command exited 1, the baseline exited 0, and the production source was
  restored byte-for-byte; `counterfactual.json` records the command and source hash.
  No injected production change remains.
- The restored rebased tree passed 582 main-task tests and 55 isolated tests in 5m 3s,
  with zero failures/errors and two existing arena-policy skips. Selection included
  all core work budgets, projection work budgets, refused-overflow checks, format/golden
  contracts, repair boundaries, interrupted recovery, session factories, derived
  maintenance, UUID/header checks, direct-reader corruption tests and the base's
  timestamp precision conformance. No bounds changed. Results:
  `build/ci-round8/core-tests.log`, `test-counts.json`.
- The first combined test/CLI command was rejected before execution because `--tests`
  was attached to the non-test CLI task. The successful invocation ran core tests alone;
  CLI compilation is separate. The initial selector also correctly rejected symbolic
  `HEAD`; the final manifest uses its full hexadecimal commit ID. Neither invocation
  failure was a source defect or changed any verification policy.
- CLI prerequisite compilation passed in 17s (`cli-classes.log`). The first full local
  PIT run retained 81 targets, 32 test patterns, four-mutant sampling and the 1,320s
  execution share, but reached that deadline with an incomplete report. It had killed
  the original `readFully` survivor and every published changed-line mutant; this is
  not a clean full-lane verdict. No disposition error was reported. Evidence remains
  under `build/ci-round8/selection/`.
- Worker stacks showed active test execution during JUnit/PIT discovery, including the
  base's timestamp conformance tests. The local JDK is GraalVM with JVMCI compilation;
  CI uses Temurin. Repeating the entire unchanged lane with the same installed JDK's
  C2 compiler (`JAVA_TOOL_OPTIONS=-XX:-UseJVMCICompiler`) tests that local runtime
  difference without changing source, selectors, test associations, sampling, deadline
  or any repository/native build flag. That separate report is under
  `build/ci-round8/c2/`; the incomplete run is preserved, not overwritten.
- The C2 repeat also reached the unchanged 1,320s share. Both full reports therefore
  remain **incomplete**, with zero disposition errors, and do not establish a green
  blocking lane. C2 did not eliminate the local execution cost, so the evidence does
  not isolate JVMCI as its cause. The eight changed-line results published in the C2
  partial report were all killed. No association, target, test, sample or timeout was
  removed or relaxed to turn either incomplete result into a pass.
- A separate supplemental run uses the six classes that produced the CI report's
  13 changed-line mutants, the same four-mutant sampling and all 32 current test
  patterns. It checks those identities on the rebased bytecode. This follows the
  [focused-run guidance](../VERIFICATION.md#pr-mutation-testing); it is individual
  mutant evidence, not a replacement full-lane completeness claim. Command, targets
  and report are under `build/ci-round8/focused/`.
- Supplemental analysis completed in 3m 59s: 20 mutants, 18 killed and two uncovered;
  **all 13 changed-line mutants are killed**, with zero surviving or uncovered
  changed-line mutants and zero unattributed results. The original boundary mutant
  is killed by `envelopeDiscoveryRefusesReadsWithoutProgress` (payload case).
  `attribution.json` uses the repository reporter and the rebased changed-line map;
  `dispositions --check --base 350cf2511dd050e02cfca58d85c7ab68492950b2 --sampled`
  also passed with no new or failing disposition.
  All 13 identities exactly match the CI report's changed-line identities, despite
  the rebase's line movement; this is a rerun of the reported set, not a different
  sample substituted for it.
- Deliberately left advisory: `FileChannelReader.readRegionsFromChunk` line 579
  (addition/subtraction) and `ResourceConfiguration.equals` line 584 (true return)
  are uncovered on **unchanged** lines. Neither is a new format-gate regression;
  no waiver or unrelated behavior test was added. The supplemental result resolves
  the CI-reported changed-line mutants but does not make either incomplete full run
  complete. Full blocking CI must still be rechecked by the outer executor.
- Final core Java Spotless verification and `git diff --check` passed. The specified
  base is an ancestor of HEAD and all 22 replayed commits remain.

No push, PR update, remote workflow trigger, pipeline-control command, system installation
or global configuration change was performed in this CI phase. All intentional evidence
and source/document edits remain inside the worktree.

## CI follow-up: fragment convention and changed-line PIT deadline

- Fix / issue: PR #1480; CI run `45449972080`, jobs `138644028565` and `138646683883`.
- Symptom and trigger: the rebased branch added one changelog entry and five lesson entries
  directly to shared files. The newly inherited fragment guard rejected all six additions.
- Root cause: the branch's entries predated the fragment convention. New entries must each
  live in their own fragment; extensions to existing lessons remain in `docs/LESSONS.md`.
- Why earlier verification missed it: the convention and its CI job arrived on the base
  after those entries were written. This is a documentation placement failure, not an engine
  regression. The existing fragment CLI is the regression oracle.
- Correction: move the changelog entry to `changelog.d/fmt-one-format.md` and each new lesson
  to its own `docs/lessons.d/` fragment, preserving the lesson bodies. Expand the release
  entry to name the strict version, UUID and stride contracts and retain FMT-401's partial
  discovery scope. `spec/CHANGELOG.md` is outside this convention and remains unchanged.
- Path audit: the guard reads both shared-file diffs and validates both fragment directories;
  the collector restores changelog bullets into the named release section, lesson rows into
  the table and notes at the end. Existing lesson extensions stay in place and all fragment
  links remain relative to the collected destination. No executable path changes.
- Lesson: apply the inherited [fragment convention](../../changelog.d/README.md) to every
  new entry after rebasing, including lessons; preserve their content rather than dropping it.
- Regression evidence: `python3 .github/scripts/changelog_fragments.py check --base-sha
  e2d784b7c7c8e249af4d74c9b621dcad9b8b22f9` reproduced the six hosted errors before the move.
  Because the CLI checks committed history, the corrected tree was committed only in the
  disposable `build/ci-round9/guard-repo/` fixture against the supplied base's shared files;
  its CLI check passes. The real collector preserves all original lesson rows and all three
  complete notes, plus the new release entry. All 63 existing fragment-tooling tests pass.
- Vision check: documentation placement changes no engine behavior or default. The release
  entry retains the one-format/history route and explicitly names the deferred repair gap.

The **new** changed-line PIT lane failed at its unchanged 1,320-second module deadline,
not the former partition deadline. The uploaded `summary.md` and `results.json` report
`state: timed out`, `partial: true`, 82 production targets, 40 test patterns, 604 changed lines,
111 killed mutants, 32 surviving changed-line mutants and zero uncovered mutants. The survivors
are in `FileChannelReader` (6), `InterruptedFirstCommitRecovery` (5),
`ResourceFormatCompatibility` (20) and `LocalDatabase` (1). All 32 remain unresolved evidence;
the deadline does not explain them away. The format repair, format baseline, interrupted
recovery, store-feature and discovery-budget suites were already selected, so their absence
is not an association fix. Seven sources were individually unmapped, but the union retained
all production targets and reported no uncovered mutant in its partial results.

CONV-202 forbids accepting dispositions from an unfinished report. No disposition, behavior
test, production change, target reduction, selector relaxation or timeout change is made for
this result. Complete mutation evidence and requirement-based review of the surviving
identities are still required; this round does not claim PIT is green. The downloaded hosted
artifact is retained locally under `build/ci-round9/hosted-pr-mutation/`.


## CI follow-up: current-base rebase and fresh PIT evidence

- Rebased submitted head `33ee97a8c31ee5ed29d351180fcc178005f79384` onto current
  `origin/main`, `31c9c16f1324bc6623bb17a64583e4763ee5ca92` (PRs #1490 and #1491),
  yielding `eded0daed` before this evidence update. All 31 branch commits were replayed;
  none was skipped. The only conflicts were two historical edits of `spec/CHANGELOG.md`.
  Resolution retains the base's benchmark entry and the branch's final format entry.
- Preservation invariant: replay must retain both sides' entries and all branch behavior.
  Of the branch's 62 changed files, 60 are byte-identical to the submitted head; only
  `spec/CHANGELOG.md` and `spec/CONV-conventions.md` gain the base's content. The vision
  sentence, format baseline, incomplete-frame decision table and FMT-401 twin, named
  `sirix-repair-format-gate` gap, repair matched pairs, discovery counters/bounds/positive
  controls/cold start/both backends and registrations, golden byte pins, postmortems and
  changelog/lesson fragments remain intact. No direct shared-file additions were restored.
- Path retrace: no production function, caller, test or executable configuration changed
  during conflict resolution. Both specification entries remain, and CONV-301 retains the
  discovery-budget registration alongside the base's new benchmark requirements.
- Verification: `./gradlew :sirix-core:classes :sirix-core:testClasses -PerrorProne
  --no-daemon` passed. The real fragment guard passed against the new base, and
  `git diff --check` passed. The preservation audit and full commit range comparison are
  local evidence in `build/ci-round10/rebase-audit.json` and `rebase-range-diff.txt`.

Fresh mutation commands, executed after the rebase, with the lane's scope and deadline
unchanged:

```bash
python3 .github/scripts/pr_mutation.py select \
  --base 31c9c16f1324bc6623bb17a64583e4763ee5ca92 \
  --head eded0daed94bb47b2d15d8c9ce472b3a33598379 \
  --output build/ci-round10/pr-mutation
ORG_GRADLE_PROJECT_errorProne=true python3 .github/scripts/pr_mutation.py run \
  --seconds 1320 --blocking --output build/ci-round10/pr-mutation
```

The fresh run exited **1**, with `sirix-core` state **timed out** and
`1320s module execution share exceeded`. Selection retained 24 changed sources, 82 exact
compiled targets, 40 test patterns and 604 changed lines, with no per-class limit. Baseline
coverage completed successfully in 204 seconds, producing 15 mutation units. Two minions
reported `TIMED_OUT`; neither the XML report nor the completion stream contained a finished
mutation result when the overall deadline terminated the run. The census contains 7,687
unique off-diff exclusions; exclusions have no verdict and do not establish any kill or
survivor count. Seven individually unmapped sources remain reported, with all targets kept.

The report also flags two inherited `BackupKt` dispositions because the local reproduction
compiled core, not the CLI's classes. This missing local prerequisite is not evidence that
those unchanged dispositions are stale in the hosted all-module build. No disposition was
edited on that basis.

The earlier hosted evidence (111 kills and 32 changed-line survivors) remains separate and
unresolved. This fresh unfinished run cannot establish whether those 32 survivors persist;
it does not invalidate them or authorize any disposition under CONV-202. No production or
behavior-test change is justified by the new evidence. No selectors, targets, sampling,
timeouts, associations or dispositions were changed. The local fresh artifacts are
`build/ci-round10/pr-mutation/{selection.json,results.json,summary.md}` and
`sirix-core/{pitest.log,census.jsonl,report/}` beneath that directory. PIT is **not green**.

The already rerun provider-cancelled macOS job required no action. No push, PR-body edit,
workflow trigger or pipeline-control command was performed in this phase.

## CI follow-up: rebase onto 7c9603baa and repeat the unchanged mutation lane

- Rebased submitted head `c7649e481d401f8e846d0d9616afb666a4496ad3` onto the
  requested current base, `7c9603baa5131f7aa51dd637aa9e537fa23c226e`, producing
  `96f179d24856b7829115b27e0b720eb14b69cb2e`. All 34 branch commits were replayed;
  none was skipped. Two conflicts in `LocalDatabase` combined the base's explicit
  derived-activation reasons and narrowed optional fallback with this branch's strict
  format refusal and failed-open ownership cleanup.
- Combined invariant: a foreign format version always propagates; explicit aggregate
  declarations propagate installation failures; opportunistic activation absorbs only
  the base's named unusable-store failures. Both callers retain the base's
  `DerivedActivation` enum, with the format exception caught before its superclass.
  The method documentation makes that exception explicit.
- Failing-path retrace: derived open refuses the foreign version before rebuilding,
  activation propagates it, and completion closes only a newly acquired session while
  preserving the original failure. Both direct and database-handle opens use completion;
  retry prepares the cached shared session again. Ordinary opens still register valid
  derived resources. Explicit declarations run the `DECLARED` initializer, propagating
  the named failures; optional activation still logs them and preserves source reads.
  No helper, branch or caller became unreachable.
- Immediately after the rebase, 63 of the branch's 65 changed files were byte-identical
  to the submitted head. Only `LocalDatabase` and the mutation-test associations combine base
  changes. The latter retains both the format tests and the base's declaration/history
  associations. The vision sentence, baseline work, incomplete-frame decision table and
  FMT-401 twin, named `sirix-repair-format-gate` gap, repair matched pairs, discovery
  counters/bounds/positive controls/cold start/both backends and registrations, golden
  pins, postmortems and fragments remain intact. No direct shared changelog or lesson
  addition was restored. Evidence: `build/ci-round12/{before.json,rebase-audit.json,
  rebase-range-diff.txt}`.
- Compilation passed in 4m 49s: `./gradlew :sirix-core:classes :sirix-core:testClasses
  :sirix-kotlin-cli:classes -PerrorProne --no-daemon`. This includes the CLI prerequisite
  that the previous local reproduction omitted. The real fragment guard passed against
  the new base; `git diff --check` passed. Log: `build/ci-round12/compile.log`.

Fresh mutation commands, after the rebase:

```bash
python3 .github/scripts/pr_mutation.py select \
  --base 7c9603baa5131f7aa51dd637aa9e537fa23c226e \
  --head 96f179d24856b7829115b27e0b720eb14b69cb2e \
  --output build/ci-round12/pr-mutation
ORG_GRADLE_PROJECT_errorProne=true python3 .github/scripts/pr_mutation.py run \
  --seconds 1320 --blocking --output build/ci-round12/pr-mutation
```

The production diff at selection includes the documentation-only clarification above;
no executable source changed after compilation or during mutation analysis.

The fresh run exited **1** at the unchanged **1,320-second** execution share with
`sirix-core` state **timed out** and `partial: true`. It retained all 24 changed sources,
83 exact compiled targets, 42 test patterns and 631 changed lines, with no per-class limit.
The base's activation enum accounts for the additional compiled target. Baseline coverage
passed in 223 seconds and produced 15 mutation units. The final partial stream preserved
30 changed-line results: **24 killed, six survived, zero uncovered**, with zero unattributed
results and zero disposition errors. The seven individually unmapped sources are still
reported; all targets remain in the union. The census records 7,690 off-diff exclusions,
with no malformed records or changed-line violations; exclusions are not verdicts.

The six retained survivors are in `FileChannelReader`: removal of `requireRootEnvelope`
in `getRevisionFileData`, removal of `RepairProof.requireChannels` in `healRevisionRecord`,
and four mutations in `requireCompatibleRevisionRoots` (two multiplication/division changes,
one addition/subtraction change and removal of `RevisionFileData.requireReadable`). These
are preserved **unresolved findings**, not accepted equivalents. The earlier hosted set of
32 survivors remains unresolved as a set: this incomplete run does not establish the
remaining identities' outcomes, explain away any survivor, or authorize dispositions under
CONV-202. No disposition, test association, selector, target, sample or timeout was changed.
PIT is **not green**.

Two thread-dump captures of each worker show different fixture operations at successive
captures, including database-ownership child-JVM work, during PIT/JUnit test discovery.
They show ongoing execution, not a completed mutation verdict or a proven cause of the
survivors. Captures: `build/ci-round12/pit-worker-*.txt`. The local runtime is the installed
GraalVM 25.0.3; hosted CI uses Temurin 25. No JDK was installed or changed.
Full fresh artifacts: `build/ci-round12/pr-mutation/{selection.json,results.json,summary.md}`
and its `sirix-core/{targets.txt,changed-lines.tsv,census.jsonl,pitest.log,report/}`.

Post-rebase regression verification passed in **6m 1s**: **668 tests**, zero failures or
errors, and two existing shared-arena policy skips (612 main-task tests and 56 isolated
startup/allocation/storage-epoch cases). The public behavior oracles remain FMT-401/FMT-402
and the base's declared-aggregate/fallback contracts; no new test or test oracle was added.
The selection exercises both refused and ordinary paths after combining the conflicts:

```bash
./gradlew :sirix-core:test \
  --tests 'io.sirix.access.FormatRepairBoundaryTest' \
  --tests 'io.sirix.format.*' \
  --tests 'io.sirix.access.InterruptedFirstCommitRecovery*' \
  --tests 'io.sirix.access.ResourceSessionFactoryTest' \
  --tests 'io.sirix.index.aggregate.DeclaredTemporalAggregateFailureTest' \
  --tests 'io.sirix.index.aggregate.DerivedIndexMaintainerHistoryTest' \
  --tests 'io.sirix.budget.*' \
  --tests 'io.sirix.index.projection.*WorkBudgetTest' \
  --tests 'io.sirix.access.trx.page.RefusedOverflowLeafTableTest' \
  -PerrorProne --no-daemon
```

Log and counts: `build/ci-round12/{regressions.log,test-counts.json}`. No work-budget bound
was changed. The specified base is an ancestor of HEAD; the full branch whitespace check
(`git diff --check origin/main`) passed. Vision check: the merge retains both the format
refusal/history route and the base's declared-failure semantics, adding no default or new
engine behavior. The PR body's Vision check was not edited. The already rerun macOS query
job required no action. No push, PR update, remote workflow trigger, pipeline-control
command, system installation or global configuration change was performed in this phase.
