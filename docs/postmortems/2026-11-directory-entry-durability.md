# Bug: file contents were forced but their names were not

- Fix / issue: this pull request (TXN-616). Found by the FMT-209 settings-publisher review, whose
  Windows path skipped the directory force; the same gap was then found at every site that creates,
  renames or removes a name the engine relies on after a crash.
- Symptom: Sirix never made a directory entry durable, on any platform. A forced file whose
  name was not durable could vanish in a power loss, and a removed name could come back. After a
  crash:
  - a committed revision could lose its index catalogue `indexes/{N}.xml`, so a reopen read revision
    N with an older catalogue and the index created in N was gone;
  - a returned `DatabaseConfiguration.serialize` could revert to the previous settings;
  - a created database or resource could be missing after `createDatabase` or `createResource`
    returned true;
  - a removed resource or database could reappear, in full or half-deleted;
  - a backup or restore that had returned could be missing; its copied contents were never forced
    either.

  Page data and beacons were not affected: they are written in place into existing, preallocated
  files and forced. The resource settings file and the encryption keyset were also written without
  a content force.
- Trigger: any of those operations followed by a power loss before the filesystem wrote the
  directory's metadata on its own. A process kill alone is not enough: the page cache keeps the
  entries.
- Root cause: every durability argument in the engine was about file contents. `FileChannel.force`
  was taken as "durable", which covers the bytes and size of the file it is called on, not the entry
  naming it in its parent directory. POSIX requires a separate `fsync` of the directory for that;
  Windows needs `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` or `FlushFileBuffers` on a directory
  handle; Java exposes neither as a file operation. The catalogue comment promised "a crash cannot
  leave a committed revision without its {revision}.xml" on the strength of a content force alone,
  and TXN-602 had deferred choosing the marker's directory mechanism.
- Why tests missed it: the crash harnesses model content: `PowerLossSimulationTest` records channel
  writes and forces, and the SIGKILL harness keeps the page cache, so neither can lose a name. No
  oracle modelled a directory as its own durability unit.
- Regression test: `DirectoryEntryDurabilityTest` runs each operation under `DirectoryCrashModel`.
  The model keeps, for every directory, the entries present at its last completed barrier, and the
  entries present at the start when no barrier has completed since. It identifies files by file key,
  and it governs the contents of chosen files (settings, keysets, catalogues, backup copies), which
  keep only what their last force wrote. Each case reopens the materialized crash state cold. The 13
  original cases all failed on `main` for the stated reason: lost catalogue (2), reverted settings,
  missing database, missing resource (2), resurrected resource (2), resurrected database, missing
  backup, missing restore, and no catalogue barrier reached (2: the marker-loss case needs the barrier
  to mark its crash instant). The 3 cases added later in the same change (the encrypted keyset and the
  data-file rebuild on both backends) were not run against `main`. All 16 pass with the fix.
  `DurableFilesTest`, `PosixDirectoryBarriersTest` and `WindowsDirectoryBarriersTest` pin the
  platform calls TXN-616 names, the Windows ones through a fake kernel32 on every platform. Command:
  `./gradlew :sirix-core:test --tests io.sirix.access.DirectoryEntryDurabilityTest
  --tests 'io.sirix.io.*DirectoryBarriersTest' --tests io.sirix.io.DurableFilesTest`.
- Lesson: a durability claim names what it covers. A content force makes bytes durable, not a name
  to reach them by. See [Lessons](../LESSONS.md).

## Review follow-up: existing targets and failed barriers

- Symptom and trigger: an empty backup, restore or database target created before the operation
  could disappear after a crash. A removal or storage-file creation whose namespace change succeeded
  but whose directory force failed could then return success on retry without publishing that change.
- Root cause: existence and absence checks were treated as proof of an earlier successful barrier.
  Nested directory creation also created descendants before knowing whether the ancestor name was
  durable. A retry could therefore bypass an unpublished ancestor as well as an unpublished leaf.
- Correction: accepted existing targets force their parent; missing directories are created and
  published from the outside inward, deleting the just-created entry when publication fails.
  Both persistent backends use the same create-and-publish boundary for data and revisions files,
  which removes a failed creation before rethrowing. Database removal forces after every settings
  deletion attempt; absent-resource removal forces the resources directory and completes the same
  registration cleanup as a successful rename. Ordinary storage opens gain no barriers.
- Why the original tests missed it: targets were always absent and all engine-level directory
  forces succeeded. Failure coverage stopped at the platform primitive, without exercising a second
  operation against the namespace left by failure.
- Regressions and oracle: TXN-616 requires the successful operation's names to survive a crash.
  `DirectoryEntryDurabilityTest` now exercises existing targets for all three operations, failed
  settings deletion and resource rename followed by retry, and failed data/revisions creation on
  both backends. `DurableFilesTest` covers every ancestor barrier of a three-level directory chain
  and an existing target's failed barrier. Directory-channel open failures are injected at the
  filesystem boundary; `DirectoryCrashModel` independently materializes the names made durable.
  These cases reject skipping a barrier because the target exists or the removed source is absent.
  They were derived before the fix; a separate failing-before run was not performed because this
  review round permits only one focused verification after all fixes.
- Backup cancellation coverage: `completedCopyPreservesInterruption` injects cancellation after
  `DurableFiles.replace` returns, the durable publication boundary, rather than after the raw move
  but before its new directory barrier. Its successful-copy, preserved-interruption and restorable-copy
  assertions are unchanged. The other backup interruption cases retain their failure/quiescence
  boundaries.
- Focused verification: all 88 tests passed with
  `./gradlew :sirix-core:test --tests io.sirix.access.DirectoryEntryDurabilityTest
  --tests 'io.sirix.io.*DirectoryBarriersTest' --tests io.sirix.io.DurableFilesTest
  --tests 'io.sirix.backup.*' --max-workers=2`, excluding the unrelated isolated prerequisite
  test tasks. Mutation, broader tests and lint remain with the outer pipeline.
- Simplification: unused batch barriers were removed. Replacement and rename accept sibling names
  only, enforced before either platform changes a name. Platform tests reject both cross-directory
  operations, and retain successful sibling replacement and rename cases.
- Lesson: a namespace lookup reports current visibility, not durability. Exercise the second call
  after an injected barrier failure and verify its crash state, including every ancestor of a new
  directory chain.

### Vision check for the review corrections

Contract class: **restore** (TXN-616 and the recorded review decisions).

| Rule | Verdict | Evidence |
| --- | --- | --- |
| The advantages to protect | aligns | Crash regressions preserve resource storage and its names; versioning layout is unchanged. |
| History is the data model | aligns | Completed operations keep their data reachable after a crash; revision contents are unchanged. |
| Speed is part of correctness | aligns | Storage rollback runs only on creation failure; existing-file opens add no barrier. |
| A fast answer is the same answer | aligns | Both persistent backends use `DurableFiles.createFile` and the same retry tests. |
| The specification decides behaviour | aligns | TXN-616 states existing-target, retry and sibling-move guarantees before implementation. |
| One format, one route | aligns | Shared publication and sibling validation replace duplicate or unused paths; no format changes. |
| A library first | aligns | Changes remain in the embedded core; no service or process dependency is added. |
| Scope | aligns | Corrections are limited to durable metadata and its regression evidence. |
