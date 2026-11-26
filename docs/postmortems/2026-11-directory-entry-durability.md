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
- Regression test: `DirectoryEntryDurabilityTest` (13 cases) runs each operation under
  `DirectoryCrashModel`. The model keeps, for every directory, the entries present at its last
  completed barrier, and the entries present at the start when no barrier has completed since. It
  identifies files by file key, so content durability stays out of the oracle, and reopens the
  materialized crash state cold. On `main` all 13 failed for the stated reason: lost catalogue (2), reverted
  settings, missing database, missing resource (2), resurrected resource (2), resurrected database,
  missing backup, missing restore, and no catalogue barrier reached (2: the marker-loss case needs
  the barrier to mark its crash instant). All pass with the fix.
  `DurableFilesTest` (10 cases) runs the real barrier of each CI platform and pins the failure
  contract. Command: `./gradlew :sirix-core:test --tests io.sirix.access.DirectoryEntryDurabilityTest
  --tests io.sirix.io.DurableFilesTest`.
- Lesson: a durability claim names what it covers. A content force makes bytes durable, not a name
  to reach them by. See [Lessons](../LESSONS.md).
