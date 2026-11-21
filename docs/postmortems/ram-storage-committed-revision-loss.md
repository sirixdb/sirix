# Bug: in-memory storage lost committed revisions

- Fix / issue: [#1463](https://github.com/sirixdb/sirix/issues/1463); fixed by the commit that adds this write-up.
- Symptom: on an `IN_MEMORY` resource a node inserted and committed successfully was gone once
  the writer was reopened (`moveTo(key)` returned `false`). The session reported revision 0 after the
  commit, a reader of the same session saw nothing, a new resource session started empty, and
  `beginNodeReadOnlyTrx(revision)` on an older revision had no revision root to read. JSON and XML
  resources were both affected. Found by the lifecycle-fences work (#1353) as a no-fault control
  that failed on unchanged production code.
- Trigger: create a database and an `IN_MEMORY` resource, insert one value node, commit, close the
  writer and open another writer on the same session.
- Root cause: `RAMStorage` met none of the storage contract's ownership rules.
  1. It stored the writer's page instances themselves instead of their encoding. Two effects
     follow. `UberPageFormat.write` is what clears an uber page's `isBootstrap` flag, so a page
     that was never encoded stayed "bootstrap": every commit republished revision 0, and each new
     writer started from the bootstrap revision again. The writer's commit cleanup also closes and
     reuses the pages it wrote, and those were the store's only copies, so readers could be handed
     closed pages. `returnsSharedPages() == true` existed only to stop callers releasing these
     shared instances; it could not stop the writer.
  2. It never recorded revision roots: `readRevisionRootPage`, `getRevisionFileData` and the
     revision-timestamp lookups read a map that nothing filled, and the revision index stayed empty.
  3. Its state belonged to the `RAMStorage` instance, and each resource session builds its own
     storage. When the last user session closed, the database-owned shared session closed with it,
     and the next session (including the first one after `createResource`, whose bootstrap session
     had already closed) opened an empty store.
- Fix: the store now keeps exactly the payload a file-backed writer would append (page encoding,
  key-value leaf page cache, byte-handler pipeline, page hash set on the reference). It records each
  revision root and serves the uber page in the file storages' beacon framing. Reads go through
  `AbstractReader`'s checksum and decode path and return a fresh, caller-owned page, so
  `returnsSharedPages()` is `false` like every other backend. Page keys start at
  `IOStorage.DATA_REGION_START`, so they never collide with the beacon offsets the writer re-reads
  as pages on close. File-backed read and write paths are unchanged, and `truncateTo` still
  refuses.
- Lifetime, specified first as TXN-615 (captain decisions of 20 November 2026): the committed state
  lives with the open database instance.
  - Every writer, reader and session through any handle sees it. Only the last handle's close
    discards it, together with the resource's on-disk footprint.
  - A database reopened afterwards, in the same or a later process, holds none of its former
    IN_MEMORY resources; their names are free again.
  - If session teardown fails, the close keeps the instance, its ownership and its state for a
    retry. If only removing leftovers fails, the close still ends the instance.
  - Backups skip IN_MEMORY resources.
- Why tests missed it: no test committed through a real `IN_MEMORY` resource and read the value
  back; the only RAM tests drove `RAMStorage` directly with an uber page, or pinned the
  shared-instance behaviour itself (`DurableBatchReadFailureReleaseTest` asserted `assertSame` on a
  re-read page). The class carried a `// FIXME: broken` comment.
- Regression test: `RAMStorageCommittedRevisionTest` (JSON and XML: reopened writer, same-session
  reader, new session, later commit leaves earlier revisions unchanged, revert, a 1,500-commit
  history read back through a new session, recreated resource starts empty). All eight cases of its
  first four methods failed on main (`expected <1> but was <0>` and `moveTo` false); all pass after
  the fix.
  `RAMStorageContractTest` pins the storage-level contract, including a key-value leaf page that the
  writer closes after the write and that still reads back intact under the empty, Deflate and LZ4
  pipelines. The TXN-615 acceptance criteria are derived from the spec text in
  `LocalDatabaseInMemoryLifetimeTest`, `DatabasesInMemoryLifetimeTest` (with forked-JVM abrupt-exit
  and small-heap memory checks) and `BackupManagerInMemoryResourcesTest`. 22 of their 28 cases
  failed before the lifetime change; the other 6 guard behaviour the code already had. Run with
  `./gradlew :sirix-core:test --tests 'io.sirix.io.ram.*' --tests '*InMemory*'`. The mutation
  results are in the PR notes.
- Found while implementing TXN-615: a plain recursive delete of a resource directory can remove
  every file (settings included) and then fail on the directory itself, when its parent is not
  writable. The leftover then has no settings proving it was in-memory state, so it has to be
  treated as persistent and shows up again. The failed-cleanup acceptance criterion (AC12) caught
  it. Retirement now renames the directory to a reserved tombstone name first, which is atomic:
  either the name is free at once, or nothing changed and the proof is intact.
- Lesson: a storage backend's correctness is defined by the round trip (commit, reopen, read the
  value) through the public API, not by its own methods returning without error. A "shared pages"
  exception in an ownership contract needs the *writer's* side checked too. Cleanup that must stay
  retryable has to keep its own proof until the last step; see [LESSONS.md](../LESSONS.md).

## Review follow-up: retirement and read scratch

- Symptoms and triggers: a valid 220-character resource name could not be renamed on a filesystem
  with 255-byte filenames; a successful rename followed by permission-denied deletion incorrectly
  let final close succeed; a partially deleted tombstone without settings blocked backup/restore;
  each RAM page read through the byte-array decoder allocated automatic native staging memory.
- Root causes: retirement tied garbage names to user-name length and represented both name release
  and footprint removal as one boolean; tool preflight treated retired garbage as a resource;
  RAM decoding used the cold byte-array bridge rather than the allocator scratch boundary.
- Fix: use bounded `$retired-<uuid>` names and return the actual remaining path. Creation can
  proceed after a rename, cleanup reports the tombstone, and only a failed derived rename keeps
  its source proof. Skip tombstones at shared tool preflight and backup reporting. Decode RAM
  payloads inside a READ scratch lease, retaining the byte-handler fallback and page ownership.
- Why tests missed it: ordinary names, failed renames and complete settings did not exercise these
  independent failure boundaries. Value assertions alone also cannot expose native staging churn.
- Regression oracles: TXN-615 clauses 3/5 require removal and empty recreation even with long names;
  clause 7 requires a failed final close after successful rename and failed deletion, with admission
  released and cleanup retryable; clause 10 requires persistent content to survive backup/restore
  despite incomplete tombstones. Tests are named in TXN-615's review-regressions entry. Existing
  RAMStorageContractTest and RAMStorageCommittedRevisionTest retain their ownership/value oracles
  unchanged. Focused verification is recorded by the review phase; mutation and broader budget
  gates remain with the outer executor.
- Lesson: distinguish name release from garbage deletion, probe only actual resources, and use the
  allocator's bounded scratch for hot decoding; see [LESSONS.md](../LESSONS.md).
