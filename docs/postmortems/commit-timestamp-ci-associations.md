# Bug: Valid mutation associations rejected by a closed inventory assertion

- Fix / issue: PR #1475, failing Python mutation tooling check on `4045111873c`.
- Symptom: the reporter accepts the repository's valid association file, but its repository
  regression rejects the additional timestamp-output and RAM-storage associations.
- Trigger: run `test_pr_mutation*.py` after adding valid CONV-204 associations alongside the
  four historical release-contract pairs.
- Root cause: the regression asserted equality with the entire historical inventory. CONV-204
  permits additional valid mappings; preserving the former pairs is a per-pair invariant,
  not a restriction on the file's complete contents.
- Why tests missed it: the repository test passed while its inventory contained only those
  four pairs. Fixture tests already exercised extensible loading and selection.
- Regression test: `AssociationTest.test_repository_files_are_valid_and_keep_the_former_contract_pairs`
  failed before the correction with the actual expanded repository file. It now uses the real
  consumer's normalized mappings, validates the complete file, and checks each required pair
  individually. CONV-204 and the historical release contract are the independent oracles;
  invalid associations and missing or changed historical pairs must still fail.
- Scope and trace: parsing and `load_associations` already accept multiple valid entries;
  `test_scope` and compiler-entry selection tests already preserve mappings, heuristic matches,
  missing-test refusal and shared caps. Only the closed inventory assertion needed correction.
  No production logic, association data or mutation acceptance rule changes.
- Verification: before the correction, 156 focused tests ran with this sole failure. After
  correction, run the same CI coverage command and its unchanged 85% floor (results below).
- Lesson: [preserve required mappings without closing an extensible inventory](../LESSONS.md).

## Local verification

```bash
COVERAGE_FILE=build/reports/ci-associations/.coverage python3 -B -m coverage run \
  --branch --source=.github/scripts --omit='*/tests/*' \
  -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'
COVERAGE_FILE=build/reports/ci-associations/.coverage python3 -B -m coverage xml \
  -o build/reports/ci-associations/python-coverage.xml
COVERAGE_FILE=build/reports/ci-associations/.coverage python3 -B -m coverage report --fail-under=85
```

All **156 tests passed**, the unchanged coverage floor passed with **99%** branch coverage,
and coverage XML generation succeeded. `git diff --check` passed. The expected patch-application
errors from negative counterfactual fixtures did not fail the suite. Only a test assertion and
its documentation changed; production mutation tooling is unchanged.

CodeFactor's separate failure reports only `3 issues found.` in GitHub's commit status.
Its report URL returns HTTP 403 via direct HTTP and the browser, and GitHub supplies no
CodeFactor annotations or review comments. No speculative analyzer fix is included.

## Follow-up: timestamp mutation scope and revert coverage

- Fix / issue: PR #1475, PIT job `137226960313` in run `45028263363`.
- Symptom: core analysis exceeded its 1,200-second module share. Its partial XML also
  contains a surviving changed-line predecessor-seeding conditional in
  `StorageEngineWriterFactory`, and a boundary survivor in `CommitTimestamps`.
- Root cause: name-based selection chose the writer factory's executor-shutdown test
  and the writer's truncation tests, omitting the timestamp conformance and late-failure
  suites. Existing timestamp cases never reverted to older content before a regressing
  custom timestamp, so they did not distinguish the represented root from the committed head.
- Invariant and oracle (before repair): TXN-310 orders commits against the latest committed
  timestamp even after a content revert. With commits at `.000100Z` and `.000200Z`, reverting
  to revision 1 and supplying an earlier timestamp must produce `.000201Z`, retain the first
  two timestamps and resolve the new timestamp to revision 3. Cover JSON and XML, file-channel
  and memory-mapped storage, warm history/readers/lookups and cold reopen. The shared writer
  factory seeds this predecessor for ordinary and reverting writers; node/page commits and
  synchronous/asynchronous retries use the same factory and commit boundary.
- Regression checks: `revertUsesTheLatestCommittedTimestamp` executes that public contract;
  `test_timestamp_writers_select_the_commit_and_retry_contracts` invokes the real association
  consumer and checks the emitted PIT filters. The selector test failed for both entry points
  before adding the associations. No heuristic filter, cap, deadline or verdict was weakened.
- Related fixture correction: adding a valid reviewed disposition made
  `test_run_cli_skips_empty_selection_and_module_entrypoint_exits_zero` fail: its manifest was
  temporary, but its default repository was the checkout. Both direct and module-entrypoint
  calls now pass the temporary repository through `--root`. Empty-selection behavior is
  tested against owned input rather than depending on an empty repository disposition file.
- Equivalent boundary: the exact sampled `toEpochMicros` boundary identity is recorded in
  `.github/mutation-dispositions.toml`. At epoch second zero, the compensated and direct
  formulas both return the subsecond microseconds; no precision, overflow or allocation
  behavior differs. This is a per-mutant CONV-201 claim, not a blanket waiver.
- Timeout evidence: hosted coverage completed in 56 seconds, then mutation execution timed out.
  Local verbose reproduction completed coverage in 96 seconds; thread dumps showed integration
  suites executing again inside `JUnit5TestUnitFinder.findTestUnits` in the mutation minions.
  It was stopped after confirming that rediscovery cost, before changing compiled tests; this
  is not a completed or passing full-run result. Local logs and thread dumps are under
  `build/ci-pr-mutation/`. The focused timestamp run is recorded below separately.
- Remaining evidence: CodeFactor still returns HTTP 403 through direct HTTP and the browser.
  Its three issue details are unavailable. The partial RAM metadata mutant cannot be treated
  as clean evidence: this branch deliberately leaves the pre-existing RAM root-map defect
  to PR #1469, per the author's decision. No duplicate RAM fix or disposition is added.
- Lesson: associate cross-class contracts explicitly and check the temporal predecessor after
  a revert, rather than relying on a same-name test or ordinary forward commits.

### Focused verification

- All 157 Python mutation-tooling tests pass, with 99% branch coverage against the unchanged
  85% floor. The empty-selection CLI regression failed before its `--root` correction.
- The focused PIT run over `StorageEngineWriterFactory` and `CommitTimestamps` completed in
  67 seconds. The exact hosted predecessor-seeding mutant (index 136) is now killed by the
  public revert test. Across the five changed-line mutants, four are killed and one is dispositioned as
  equivalent; the three unchanged-line mutants remain advisory. XML is at
  `build/ci-pit-focused/mutations.xml`, log at `build/ci-pr-mutation/focused.log`.
- The read-only `dispositions --check --sampled --base 5b29020fb5114399cf66f0a82ec4136b9139d2fa
  --report build/ci-pit-focused/mutations.xml` check accepts the one exact equivalence entry.
  The focused report proves the added test passes without mutation and detects the seeded
  wrong behavior; it does not replace a complete full-PR mutation report.
- The ordinary timestamp suites pass all 32 cases (19 conformance, 13 late-failure), including
  all four JSON/XML/backend revert combinations. The changed Java test passes Spotless and
  `git diff --check` passes. Selection grows from 36 to 38 filters, adding exactly the two
  timestamp suites and removing none.
- A full bounded rerun under `build/ci-pit-full/` passed coverage collection in 128 seconds.
  Its partial core report reproduced the three killed utility mutants, the equivalent boundary,
  and the uncovered RAM metadata return at line 216. Core execution was explicitly stopped
  after confirming that blocked dependency; its incomplete result must not be read as clean.
  This does not establish that the original full-run timeout is fixed. The RAM defect remains
  assigned to PR #1469 and CodeFactor's diagnostics remain inaccessible.
- The full command exits 1: core is `error` (exit 143 after cancellation), query completes
  with no failing changed-line mutants. Because incomplete core reports are not admitted as
  mutation evidence, that run marks the new equivalence entry unconfirmed; the completed
  focused report and its successful disposition check above are the evidence for the entry.

## Follow-up: focused path-summary timestamp contract

- Fix / issue: PR #1475, PIT job `137263674580` in run `45040174645`.
- Symptom: core exceeded its unchanged 1,200-second execution share; XML was truncated.
  Query completed without changed-line failures. The incomplete core run could not confirm
  the existing `CommitTimestamps` disposition.
- Root cause (first selection): the path-summary timestamp getter had no named behavior suite. The selector's
  specified package fallback added 14 statistics suites, whose database commits are executed
  again by the JUnit 5 adapter during each mutation minion's discovery. Hosted initial coverage
  took 49 seconds. A local verbose reproduction and thread dump confirmed repeated integration
  execution inside `JUnit5TestUnitFinder`, before a mutant's selected test runs.
- Invariant and scope: VER-201 requires public readers to return the exact stored microsecond,
  including historical/current path summaries, JSON/XML and file-channel, memory-mapped and
  in-memory storage. Node-reader paths already have timestamp conformance coverage. The missing
  path-summary contract now has its own `PathSummaryReaderTest`, selected by the existing named
  match rule. All other source mappings, targets, sampling, caps and deadlines remain unchanged.
- Regression tests: `PathSummaryReaderTest.revisionTimestampsPreserveMicroseconds` commits at
  `.000100Z` and `.000101Z`, then checks the historical and current public path-summary readers
  against those explicit instants. VER-201 is the oracle; treating the timestamp as milliseconds
  or returning the head's timestamp for both readers would fail. The selector regression executes
  `test_scope` and asserts its emitted filters/resolution, under CONV-204's named-match rule.
  It failed without the new suite (14 package filters) and passes with it (one named filter).
  Full core selection drops from 41 filters to 28, replacing only this package fallback.
- Trace before verification: `prepare_analysis` still calls `test_scope`, which resolves the
  new exact test name before related/package candidates and emits its normal trailing wildcard.
  The class uses the same public session open/commit/close paths as the conformance suite; both
  model branches create/open the matching database, and both reader lifetimes end before session
  close. No production code, caller contract, helper or parameter changes.
- Lesson: give changed public timestamp readers a focused executable contract, so package fallback
  does not turn one getter change into repeated unrelated integration discovery.

### Separate the clock and history contracts

The first local rerun confirmed a second expensive selection: the existing exact
`DerivedIndexMaintainerTest` match executed all history/fold/truncation scenarios during
minion discovery, although this PR adds only two clock-regression methods there. Its
initial coverage completed, but the run was stopped after thread dumps confirmed repeated
history-suite discovery. This interrupted run is not a clean mutation result.

The invariant applies to both expensive selections: preserve every behavior assertion while
keeping the timestamp contract independently executable. The pre-existing history cases and
helpers now live in `DerivedIndexMaintainerHistoryTest`; the original exact class name owns
the two unchanged clock-regression methods and their mapping assertion. Three existing helpers
become package-visible so the focused suite can share their unchanged declarations, edits and
aggregate oracles. No inheritance, exclusions, caps, timeouts or selector changes are added.
The ordinary test task still discovers both suites. This is the same focused-contract correction
as the reader suite, applied to the other broad selection in this change.

Trace before verification: both clock methods create their own temporary resource, pause
maintenance, scope and close the clock mock, and use the same four shared helpers. Their
historical/current mapping assertions and clock/fold transitions are unchanged. Every caller
of those helpers in the history suite remains in that suite with the same arguments and bodies;
there are no removed cases or unused compatibility aliases. The named-match selector still
emits the exact clock-suite wildcard and all other source mappings remain unchanged. Run both
suites in the ordinary lane to verify the shared helpers and all historical callers.

### Verification of the focused contracts

- All 158 Python mutation-tooling tests pass. The unchanged CI coverage floor passes at 99%.
  Logs and coverage output are under `build/ci-current/python-coverage/`.
- The ordinary combined Java run passes all 79 cases: four clock cases, 69 history cases and
  six public path-summary cases. The hosted original maintainer report had 73 cases; the split
  retains all 73. `build/ci-current/split-tests.log` records the successful task and its required
  isolated forks. Core Spotless and `git diff --check` pass.
- Query test classes and CLI main classes compile. The latter are needed to validate the unchanged
  backup dispositions against compiled bytecode; missing local CLI output is not evidence of a
  stale repository disposition.
- The final full bounded reproduction uses the original base/head, 1,320-second total budget,
  per-class sample and all changed targets. Output is under `build/ci-current/final/`; earlier
  interrupted diagnostic attempts under `baseline`/`fixed` are not clean mutation evidence.
- CodeFactor still reports three issues without annotations; its PR report returns HTTP 403 via
  direct HTTP, the browser and the web reader. No guessed analyzer fix or suppression is included.

### Remaining check status

The final local core analysis still **timed out at its unchanged 1,200-second module share**.
Initial coverage completed in 105 seconds, and thread dumps show the narrowed clock suite and
other selected integration suites executing in mutation/discovery. The smaller selection and
passing behavior suites do **not** establish that the full PIT timeout is fixed. The partial XML
is incomplete and cannot confirm the existing equivalence entry under CONV-202. No disposition,
threshold, sampling limit or deadline was changed to treat this run as clean. Further full-run
completeness evidence is still needed; CodeFactor's three issue details are also still unavailable.

The final reporter exits 1: query analysis is complete with no failing changed-line mutants;
core is timed out and the one new utility disposition is consequently unconfirmed. Association
validation has no errors. The compiled CLI baseline dispositions are not stale.

```bash
./gradlew :sirix-core:test \
  --tests io.sirix.index.aggregate.DerivedIndexMaintainerTest \
  --tests io.sirix.index.aggregate.DerivedIndexMaintainerHistoryTest \
  --tests io.sirix.index.path.summary.PathSummaryReaderTest
./gradlew :sirix-core:spotlessJavaCheck
python3 .github/scripts/pr_mutation.py run --seconds 1320 --blocking --output build/ci-current/final
```
