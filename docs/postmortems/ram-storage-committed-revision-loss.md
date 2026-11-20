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
  as pages on close. The committed state is a per-resource store keyed by resource path and
  resource UUID. It outlives sessions and database handles, is dropped when the resource is removed
  (`LocalDatabase` calls `RAMStorage.discardUnder`; removing a database removes each resource
  first), and is replaced when a resource with another identity appears at the same path.
  `truncateTo` still refuses. File-backed read and write paths are unchanged.
- Why tests missed it: no test committed through a real `IN_MEMORY` resource and read the value
  back; the only RAM tests drove `RAMStorage` directly with an uber page, or pinned the
  shared-instance behaviour itself (`DurableBatchReadFailureReleaseTest` asserted `assertSame` on a
  re-read page). The class carried a `// FIXME: broken` comment.
- Regression test: `RAMStorageCommittedRevisionTest` (JSON and XML: reopened writer, same-session
  reader, new session, reopened database, later commit leaves earlier revisions unchanged, revert,
  recreated resource starts empty). All eight cases of its first four methods failed on main
  (`expected <1> but was <0>` and `moveTo` false); all pass after the fix.
  `RAMStorageContractTest` pins the storage-level contract, including a key-value leaf page that the
  writer closes after the write and that still reads back intact under the empty, Deflate and LZ4
  pipelines. `LocalDatabaseInMemoryRemovalTest` and `DatabasesInMemoryRemovalTest` pin the release
  on removal. Run with `./gradlew :sirix-core:test --tests 'io.sirix.io.ram.*'`. PIT over every
  changed line: 82 killed, 2 survived. Both survivors are unobservable: the revision table's growth
  factor (`<<` to `>>`) still fits every revision through `Math.max(revision + 1, ...)`, and the
  hash on the revision-root read reference only checks a private immutable array no API can corrupt.
- Lesson: a storage backend's correctness is defined by the round trip (commit, reopen, read the
  value) through the public API, not by its own methods returning without error. A "shared pages"
  exception in an ownership contract needs the *writer's* side checked too; see
  [LESSONS.md](../LESSONS.md).
