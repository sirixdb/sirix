# Bug: directory-durability CI fixtures used different path identities

- Fix / issue: PR #1508, TXN-613 and TXN-616.
- Symptom: the macOS core lane reported 42 directory-durability failures, followed by a stress
  fixture refusing allocator reinitialization because a failed test had left a database open.
  The encryption test could not find its keyset; fault injections missed their intended directory;
  the crash model missed completed engine barriers. Sonar's reliability gate also rejected the
  mutable observer reference, and PIT reported nine undetected changed-line mutations.
- Trigger: a temporary directory reached through a symlink, such as macOS's `/var` alias. Database
  admission resolves it to its real path, while the fixture retained the alias. `Path.equals`,
  encryption-handler equality, and the crash model's root containment then disagreed about the
  same directory.
- Root cause: a filesystem fixture that observes engine barriers must share the engine's canonical
  path identity. Canonicalizing the fixture root fixes every encrypted path, failure injector and
  crash-model consumer together. The observer now uses an atomic reference for publication and
  exchange; callback implementations still own their synchronization.
- Why tests missed it: Linux's normal temporary directory had no alias. The mutation baseline
  also lacked empty-backup restoration, a warmed resource's inverse ID mapping after a removal
  retry, rollback's secondary deletion failure, and admission failing after a database session
  opened but before bootstrap acquired the user session. Retrying after external cleanup removed
  a pending tombstone's parent was also absent from the suite, as was last-close retention of a
  source's settings while its derived directory could not be renamed.
- Regression tests: `DirectoryEntryDurabilityTest` exercises those backup, removal, admission and
  recovery-ownership contracts through real databases, backups and restored documents.
  `DurableFilesTest.failedCreationRetainsBothBarrierAndCleanupFailures` checks the original force
  error and the suppressed deletion error for files and directories. A symlinked `TMPDIR` locally
  reproduces the macOS path mismatch without requiring a different operating system.
  `LocalDatabaseInMemoryLifetimeTest.lastCloseRetainsSourceProofUntilDerivedRenameCanComplete`
  covers JSON and XML: committed in-memory data is discarded, the source settings remain proof
  until derived retirement succeeds, and retry followed by recreation starts empty.
- Simplification: registration belongs to resource admission, so creation's duplicate bimap insert
  was removed. A retirement result now reports only whether the original public name remains;
  `pendingCleanup` already owns retained tombstones. Close/reopen coverage verifies that a failed
  tombstone barrier still retains and reports the cleanup. The preliminary absent-entry sweep is
  required: if external cleanup also removed the parent, there is no directory left to force.
  The retry test includes that case and requires successful closure and subsequent admission.
- Windows evidence: the query lane's sole failure was native `MoveFileExW` error 5 while retiring
  `corpus-a.jn`. Its query assertions passed. The log does not identify the handle or filesystem
  actor denying access, and the same query suite passes locally on Linux. No retry, sleep or silent
  durability fallback was added for an unproven cause; native Windows confirmation remains external
  to this Linux run.
- Vision check: these changes preserve immutable readable history and the strict filesystem-error
  contract. They introduce no setting or per-record work; redundant metadata work is removed.
- Lesson: canonicalize a fixture before constructing any path-bound collaborator or fault oracle;
  run it through an alias as well as a direct path. See the lesson fragment
  [directory-metadata-ci](../lessons.d/directory-metadata-ci.md).

## Windows rollback-failure fixture

- Fix / issue: PR #1508, TXN-616; Windows core-non-index job
  [140704231748](https://github.com/sirixdb/sirix/actions/runs/46057026533/job/140704231748).
- Symptom and trigger: both `failedCreationRetainsBothBarrierAndCleanupFailures` cases failed
  with no exception thrown. The job completed 12,336 tests with exactly these two failures.
- Root cause: the fixture injected a directory-open failure into `FileChannel.open`. Windows
  uses kernel32 for directory barriers, so the injection never ran and creation succeeded.
  Platform-independent rollback fixtures must inject at a boundary every platform executes.
- Fix: inject at `DurableFiles.syncDirectory` for file creation, each new directory ancestor and
  existing-directory retries. All other calls execute their real methods; platform barrier tests
  still exercise the real native implementation separately.
- Regression and oracle: the existing parameterized test invokes real creation and rollback,
  then checks the original barrier cause, the suppressed cleanup error, and the retained entry.
  TXN-616's failure contract supplies the oracle; replacing the barrier error with the cleanup
  error or discarding the latter must fail. The linked Windows run provides the failing evidence.
- Vision check: restores portable verification of strict durability errors without changing
  engine behavior, defaults, or path cost. Production sources are unchanged, so this correction
  introduces no production logic requiring mutation testing.
- Lesson: inject shared behavior failures at the shared boundary rather than an OS-specific
  implementation; verify each native implementation in its own platform tests. See
  [LESSONS.md](../LESSONS.md) and the directory-metadata CI lesson fragment.
- Local validation: the command below passed **36 tests with zero failures, errors or skips**,
  including both cleanup-error cases and all directory-creation retries. Its required isolated
  startup, allocation, ownership and identity dependencies also completed without failures
  (two policy-specific cases skipped). `:sirix-core:spotlessJavaCheck` and `git diff --check`
  passed. This is Linux evidence; native Windows confirmation belongs to the hosted rerun.

  ```sh
  ./gradlew :sirix-core:test --tests io.sirix.io.DurableFilesTest \
    --tests io.sirix.io.PosixDirectoryBarriersTest \
    --tests io.sirix.io.WindowsDirectoryBarriersTest -PtestHeapMin=256m -PtestHeapMax=1g
  ```

## Local validation

- Reproduced the macOS encryption failure on Linux with the original fixture and a symlinked
  `TMPDIR`: `createdEncryptedResourceAndItsKeysetSurviveACrash` failed with the reported missing
  keyset. With the canonical fixture root, the complete focused run below passed through that
  same alias: **194 tests, zero failures, errors or skips**. It includes the formerly blocked
  `MixedReadWriteUafStressTest` and both persistent backends' durability cases.
- With the preliminary absent-entry sweep removed, the external-cleanup retry case failed with
  `SirixIOException` naming the already-absent tombstone. Restoring the existing sweep made both
  retry cases pass in the full run.
- `ArrayContainsScopeDifferentialTest`: **11 passed**; its clean-JVM
  `BitemporalLoaderBudgetTest` dependency: **5 passed**.
- `:sirix-core:spotlessCheck` and `git diff --check` passed.

The alias is local test setup: `.ci-work/temp-link` points to its sibling `temp-real` directory.

```sh
mkdir -p .ci-work/temp-real
ln -s temp-real .ci-work/temp-link
TMPDIR="$PWD/.ci-work/temp-link" ./gradlew :sirix-core:test \
  --tests io.sirix.access.DirectoryEntryDurabilityTest \
  --tests io.sirix.io.DurableFilesTest \
  --tests io.sirix.io.PosixDirectoryBarriersTest \
  --tests io.sirix.io.WindowsDirectoryBarriersTest \
  --tests io.sirix.access.LocalDatabaseInMemoryLifetimeTest \
  --tests io.sirix.access.DatabasesInMemoryLifetimeTest \
  --tests io.sirix.access.MixedReadWriteUafStressTest :sirix-core:spotlessCheck
./gradlew :sirix-query:test --tests io.sirix.query.ArrayContainsScopeDifferentialTest
```

## Mutation evidence

The normal selection completed with 33 suites: **109 killed, zero survived, one uncovered**.
The uncovered line was derived-close source-proof retention, now covered for JSON and XML.
The follow-up completed with the ten conformance suites below: **110/110 killed**, with no
sampling, exclusions of changed mutants or new dispositions. Its six-field mutation identities
exactly equal the full selection's identities; hashes of all fourteen production source files
also match. Both runs targeted all 45 classes and all 771 changed lines, retaining the full census
of 3,458 off-diff mutants. Existing derived-history coverage remains necessary for normal
persistent-removal map cleanup; the new warmed retry case covers its absent-name sibling.

The follow-up used the full run's `targets.txt` and `changed-lines.tsv`, with absolute paths for
the PIT interceptor's map, and these `prMutationTests` patterns (each with a trailing `*`):

- `io.sirix.access.DirectoryEntryDurabilityTest`
- `io.sirix.io.DurableFilesTest`
- `io.sirix.io.PosixDirectoryBarriersTest`
- `io.sirix.io.WindowsDirectoryBarriersTest`
- `io.sirix.access.LocalDatabaseInMemoryLifetimeTest`
- `io.sirix.access.DatabasesInMemoryLifetimeTest`
- `io.sirix.access.FormatRepairBoundaryTest`
- `io.sirix.backup.BackupDerivedResourceTest`
- `io.sirix.index.aggregate.DerivedIndexMaintainerHistoryTest`
- `io.sirix.index.aggregate.DeclaredTemporalAggregateFailureTest`

No CI selection or budget was changed. The normal local wrapper additionally reported 44
pre-existing CLI/REST disposition classes as missing because those modules were not compiled in
this focused checkout. The hosted artifact reported zero disposition errors; those records were
left unchanged. The completed core reports, exact commands, identity comparison and source hashes
are retained locally under `build/ci-metadata-evidence/`.
