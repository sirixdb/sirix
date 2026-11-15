# Bug: feature-open changes broke recovery and CI fixtures

- Fix / issue: PR #1426, CI run 43760238494.
- Symptom: core tests failed on Linux, macOS and Windows; PIT aborted before mutation analysis because its unmutated baseline failed.
- Trigger: open an existing derived resource with missing/malformed settings; construct the writer-cleanup test session; run the backup recheck regression before a test that replaces the global buffer manager; truncate a live memory-mapped store on Windows.
- Root cause:
  - Moving configuration deserialization outside the derived rebuild handler made damaged settings bypass the existing rebuild path. Compatibility must precede deletion even when settings cannot be read, but settings damage alone must not disable recovery of feature-free derived stores. The handler now probes with the derived store's prescribed pipeline only when no opening snapshot was obtained. When configuration and features were readable, it reuses that checked snapshot, including when the source and derived encryption pipelines differ.
  - The cleanup fixture mocked `UberPage` without its new non-null feature contract. A real revision-zero page supplies that contract and removes the obsolete helper.
  - Stubbing a static method under `CALLS_REAL_METHODS` executed `openJsonDatabase` during stub setup and abandoned the returned handle. Direct interception prevents that extra open while retaining real commit/backup execution.
  - The new rollback regression included live memory-mapped truncation on Windows, where mapped files cannot be shrunk. The test now uses the existing platform limitation: both backends on Linux/macOS and FILE_CHANNEL on Windows. It does not change storage behavior.
- Why tests missed it: earlier focused lifecycle tests covered damaged records/directories but not damaged settings; they omitted the base cleanup mock. The backup test passed by itself despite leaking a handle. The snapshot rebuild control asserted only maintainer existence, so PIT could remove validation without failing it; it now checks that the invalid revision-one derived snapshot is replaced by the empty source's revision-zero bootstrap. Linux alone cannot exercise Windows file locking.
- Regression tests / independent oracles:
  - `DerivedIndexMaintainerTest.unavailableDerivedStoreRebuildsWithoutLosingSourceHistory` and `incompleteDerivedDirectoriesRecoverAndCascadeWithTheirSource`: the source log's committed values 10 and 20 must remain queryable after derived rebuild. A disabled rebuild returns no derived maintainer and fails.
  - `encryptedSnapshotsRetainSourceProtectionAcrossFoldsAndReopen`: includes missing and malformed settings, intact/mismatched encryption, exact historical aggregates and round-trip decryption with the source key. Reprobing a known snapshot using the wrong pipeline fails this control.
  - `StoreFeaturesLifecycleTest.unsupportedDerivedDamageNeverTriggersRebuild`: missing/malformed settings join missing logs, missing revision records and invalid snapshots for both persistent backends and both incompat/read-only words. Independently patched, checksummed beacon fixtures must retain every file, directory and byte. Removing the destructive-boundary feature check permits deletion and fails.
  - `AbstractResourceSessionWriteFailureCleanupTest.listenerBindingFailureClosesUnregisteredWriterAndLeavesPermitReusable`: the original binding failure, suppressed close failure and reusable writer permit are the oracle; before the fixture fix, construction throws a null-feature exception instead.
  - `StoreFeaturesLifecycleTest.backupRechecksFeaturesAfterAcquiringItsWriter` followed by `HOTStructuralChildEvictionTest`: the backup refuses after a real feature commit and releases all handles, permitting the existing global-manager reinitialization. This sequence reproduced the handle leak before the fix.
  - `StoreFeaturesLifecycleTest.committedFeaturesSurviveRollbackPastTheirFirstUseAndNextCommit`: the store-level monotonicity contract requires the feature words to survive rollback and the next commit; Windows runs its supported FILE_CHANNEL case.
- Verification: focused before/after tests, the original CI PIT target/test selection, an exhaustive PIT run of `LocalDatabase.openDerivedResource`, and existing work budgets. Results and mutation accounting are recorded below.
- Lesson: see the [CI fixture/recovery lesson](../LESSONS.md). Earlier mutation evidence in `store-feature-pit-evidence.md` describes its historical revision; its redundant catch-guard justification does not apply to the damaged-settings guard added here.

## Results

The final production change passed 472 core cases (including all selected core work budgets), 332 query cases with five existing 100,000-cohort skips, and three MCP budget cases. After adding the stronger bootstrap-state assertion, both backend cases passed again. All **16/16** PIT mutants in `LocalDatabase.openDerivedResource` were killed, including removal of both feature guards and the previously surviving snapshot validation. No sampling limit was used in this focused run; other methods were explicitly excluded.

The original CI PIT baseline abort is repaired. A preliminary run with the original CI target/test selection completed analysis with 58 killed, 46 surviving and 86 uncovered mutants. **That broad sample is not clean**; it includes unrelated methods and misses lifecycle tests through the existing test-mapping heuristic. This fix does not change that heuristic, thresholds or scope. The existing broader [mutation accounting](store-feature-pit-evidence.md) remains relevant; this round supplies exhaustive method evidence for the recovery change, not a whole-PR mutation pass. macOS/Windows execution and the full remaining-core lane await the outer CI executor.

Raw reports, before-fix failures, test counts, source hashes and verification harnesses are in [the evidence manifest](evidence/store-feature-ci/manifest.json). The initial focused report records the validation-removal survivor; the final report records its kill after the persisted-state assertion was added.

Reproduce the focused checks from the repository root (JDK 25):

```bash
mkdir -p build/ci-fix/tmp
./gradlew --max-workers=2 -Dorg.gradle.jvmargs=-Xmx2g \
  -I docs/postmortems/evidence/store-feature-ci/focused.gradle.txt \
  -I docs/postmortems/evidence/store-feature-ci/pit-derived.gradle.txt \
  :sirix-core:ciFocused \
  --tests 'io.sirix.access.InterruptedFirstCommitRecoveryStoreFeaturesTest' \
  --tests 'io.sirix.access.trx.node.AbstractResourceSessionWriteFailureCleanupTest' \
  --tests 'io.sirix.index.aggregate.DerivedIndexMaintainerTest' \
  --tests 'io.sirix.index.hot.HOTStructuralChildEvictionTest' \
  --tests 'io.sirix.budget.*' \
  --tests 'io.sirix.index.projection.*WorkBudgetTest' \
  --tests 'io.sirix.access.trx.page.RefusedOverflowLeafTableTest' \
  :sirix-core:pitest -PprMutationModule=sirix-core \
  -PprMutationTargetsFile=docs/postmortems/evidence/store-feature-ci/targets.txt \
  '-PprMutationTests=io.sirix.access.InterruptedFirstCommitRecoveryStoreFeaturesTest*,io.sirix.index.aggregate.DerivedIndexMaintainerTest*' \
  -PprMutationReportDir=build/ci-fix/pit-derived-final
```

Query/MCP checks used the unchanged *Work budgets* commands in `docs/VERIFICATION.md`, with `-PtestHeapMin=256m -PtestHeapMax=2g` to bound the local forks. No budget bounds or CI workflow settings changed.
