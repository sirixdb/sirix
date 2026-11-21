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

## Review follow-up: failed cleanup admission and proof

- Trigger and symptoms: forced database removal after denied tombstone deletion left a closed
  instance registered and its lock held; source removal or hidden-name creation could release
  settings before retiring a derived sibling; a denied initial directory listing exposed old RAM
  resources once permissions returned; retaining source settings on derived-rename failure also
  retained decoded source caches.
- Root causes: forced close bypassed the ended-instance ownership-release boundary; derived
  ordering and cache discard were enforced only on some retirement paths; a failed scan was logged
  without preserving the fact that resource admission had not yet been established.
- Fix: forced removal uses the same ownership release as final-handle close when the instance has
  ended. Shared retirement retires the derived sibling before releasing its source name. Shared
  state discard clears caches and RAM even while proof is retained. A pending-scan flag gates the
  four resource-admission paths until scanning succeeds, without rescanning live state thereafter.
- Regression oracles: TXN-615 clauses 7/8 and AC9/AC12 require reopen and empty recreation after
  failed forced removal; clause 6 requires old RAM names to remain absent on each admission path
  after an unreadable initial scan. Clauses 7/8 require a derived original name to be retired before
  source-name reuse, even after a previous failed rename. Tests use public API results, committed
  content, POSIX permissions and persisted directory fixtures; the latter model the reserved derived
  namespace and its readable source settings, not implementation text.
- Why tests missed it: tests exercised final-handle close and successful initial scans, without
  crossing forced removal, source/derived deletion order or scan-permission transitions.
- Test limitation: no new public-only test directly measures decoded-cache retention while source
  settings remain. That guarantee is checked at the shared discard boundary; internal counters or
  cache object assertions would not satisfy the requested public-behavior constraint. Mutation and
  broader verification remain assigned to the outer executor.
- Lesson: every lifecycle exit must release ended ownership, preserve proof until its dependents
  retire, and treat incomplete discovery as pending admission; see [LESSONS.md](../LESSONS.md).

## Review follow-up: retained sessions and close retries

- Symptoms and triggers: retaining closed sessions retained the RAM store's complete payload and
  revision arrays after its registry entry was removed. A failed directory scan or unreadable
  settings prevented cache release. Failed renames left discarded names addressable by ID.
  Repeated close returned without retrying leftover deletion.
- Root causes: registry lifetime was mistaken for object reachability; cache release depended on
  rediscovering identities from disk; name/ID removal depended on rename success; ended handles
  discarded the authority needed to finish their own cleanup.
- Fix: remove each RAM store from the registry before clearing its arrays, after users quiesce.
  Remember known RAM resource configurations at session admission, including derived storage;
  final teardown clears their caches independently of discovery. Shared logical discard removes
  name/ID mappings even while settings remain. Remember exact pending cleanup paths and original
  proof UUIDs, and retry only those paths under lifecycle admission. Retry is disk-only, checks
  incarnation identity, preserves derived-before-source ordering, and never clears a newer store.
- Regression oracles: TXN-615 AC13's existing small-heap child now has additional modes that keep
  closed sessions reachable through the entire workload, after both resource removal and final
  close (`closedSessionsDoNotRetainCommittedHistories`). Clauses 5/7/8 require both metadata lookup
  directions to forget hidden resources (`discardedHiddenResourcesHaveNoNameOrId`). Clause 7
  requires repeated close to report still-denied cleanup and succeed after permissions return,
  for failed renames, failed tombstone deletion and failed directory listing. The newer-instance
  regression commits and reads a replacement value while an old handle retries close.
- Why tests missed it: the memory child retained open databases as its control, but did not retain
  closed sessions; permission tests reopened the database instead of retrying the ended handle.
- Test limitation: the failed-scan regression verifies known footprint cleanup and the ended-handle
  behavior publicly. It does not directly measure native cache retention for missing directories
  or unreadable settings; those release paths are reviewed against the remembered identity map.
  Mutation and broader budget validation remain assigned to the outer executor.
- Lesson: remove registry ownership and retained payloads together; separate logical state release
  from filesystem discovery and keep retry authority bound to old identities, not reusable names.

## Review follow-up: publication and inaccessible known paths

- Symptoms and triggers: root writing appended a timestamp-index entry before catalogue persistence
  could fail. Retrying the same revision appended another entry, so an in-between timestamp selected
  the failed attempt and an exact retry timestamp returned a nonexistent revision. Separately, a
  resources directory with read permission but no search permission could be listed while child
  existence checks returned unknown; final close silently dropped those known cleanup targets.
- Root causes: physical root serialization was mistaken for commit publication, and `Files.exists`
  was mistaken for a two-valued absence test at the cleanup boundary.
- Fix: stage revision locators separately, prefer committed records on reads, and append the index
  and committed locator once at uber-beacon publication. Preserve physical reads of provisional
  roots for storage clients, without exposing them through timestamp lookup. Clear both record
  arrays on store discard. Written root locators remain available throughout publication so a
  physical reader crossing a committed-array resize never loses its fallback locator. Retain known
  cleanup paths unless `Files.notExists` proves absence.
- Regression oracles: TXN-503 and VER-202 require failed commits to contribute no timestamp entry.
  `RAMStorageCommittedRevisionTest.failedCommitDoesNotPublishATimestampEntry` uses a directory at
  the revision catalogue's output path to make public JSON/XML commits fail after root writing;
  rollback/retry checks timestamps 100/200/250/300/400, historical values, history and absent revision
  3. No new injection seam or implementation-shape assertion is needed. TXN-615 clause 7 requires
  denied traversal to make close fail and repeated close to finish once permissions return;
  `failedTraversalRemainsPendingUntilRepeatedClose` exercises POSIX read-without-search permission.
- Why tests missed it: successful histories never revisited an unacknowledged revision number,
  and existing directory permission tests denied listing or writing rather than search permission.
- Lesson: publish commit metadata at the commit boundary, and distinguish an unobservable path
  from a path proven absent. Mutation and broader gates remain assigned to the outer executor.
