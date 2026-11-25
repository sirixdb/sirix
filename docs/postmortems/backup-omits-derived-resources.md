# Bug: backups omit the hidden derived resources

- Fix / issue: [#1382](https://github.com/sirixdb/sirix/issues/1382); spec
  [TXN-613](../../spec/TXN-transactions-durability.md#txn-613--backups-include-derived-resources).
- Symptom: `BackupManager.backupDatabase` produced a backup directory with no
  `resources/<name>$derived` entry. A database restored from it had no temporal-aggregate data:
  queries that had been served from the aggregate index fell back to a full scan until the
  maintainer rebuilt the index from the source log, and the restore reported nothing about it. The
  summary, the CLI output and `docs/BACKUP.md` all claimed a complete backup.
- Trigger: back up any database with a resource that declares a temporal aggregate
  (`BackupDerivedResourceTest.createSourceDatabaseWithTemporalAggregate`): a projection index, a
  `TemporalAggregateDeclarations.declare` on it, and a drained maintainer. The backup's
  `derivedResources()` is empty and `resources/records$derived` does not exist in the target.
- Root cause: the backup enumerated resources with `Database.listResources()`, whose contract is the
  *public* listing — it deliberately filters out `DerivedIndexMaintainer.isDerivedResourceName`
  entries, because a derived resource is internal and must not appear to callers. The backup used
  that listing as if it were "every resource whose files have to be copied". Those are two different
  sets: hiding a resource from callers says nothing about whether its bytes are part of the
  database's durable state. The restore side had the mirrored gap: its copy pass walked the whole
  tree, so a derived resource *present* in a backup was restored, but its verification pass used the
  same public listing and so verified and reported only the visible resources.
- Why tests missed it: `BackupRestoreTest` built its fixture with
  `ResourceConfiguration.newBuilder(name).build()` — no valid-time configuration, hence no derived
  resource to lose. Every assertion was over the resources the test had created by name, so no test
  ever asked "is anything else on disk that a backup must carry?". The derived-index tests, in turn,
  never went through backup or restore. Neither side's oracle could see the other's omission.
- Regression test: `BackupDerivedResourceTest` (sirix-core) and
  `BackupRestoreTemporalAggregateTest` (sirix-query). Fails-before arm: the fixed `BackupManager`
  with only its two behavioural decisions reverted (`quiesceDerivedResource` returning "no derived
  resource", the restore's derived pass skipped), compiled into a separate output directory ahead of
  the build output, so both arms differ in behaviour alone. Before: 0/4 pass —
  `derivedResources=[]`, no `records$derived` in the backup, a truncated derived store restores
  clean, no refusal while maintenance is held. After: 4/4 pass.
- Lesson: see [Lessons](../LESSONS.md) (see the [fragment convention](../../changelog.d/README.md)).

## Copy-boundary and restore-verification follow-up

- Symptom: a hidden sibling rebuilt after the database metadata copy can reuse an allocated ID on
  the next create; a paused truncation can leave derived mappings newer than the copied source;
  damaged restart metadata can be silently rebuilt during restore.
- Trigger: remove the derived sibling before backup; or pause a caught-up maintainer and truncate
  its source; or corrupt the derived temporal-aggregate page while retaining its revision root.
- Root cause: the database allocation counter was copied before resource membership settled, a
  history lock was treated as proof of a valid frontier, and restore validated only the root rather
  than the metadata the next open consumes. These are three incomplete copy-boundary invariants.
- Fix: reconcile the target counter against all copied settings; refuse pending rewinds under the
  existing lock order; validate structure, frontier and mappings with a tool-opened detached derived
  reader and private caches before opening any source. Both backup and restore check the source-head
  bound. Private caches ensure verification reads the copied bytes rather than warm source pages.
- Why tests missed it: the original tests used stable metadata, did not truncate a paused source,
  and damaged the whole derived file rather than a referenced metadata page below an intact root.
- Regression tests: `BackupDerivedResourceTest.backupReservesIdsForRebuiltDerivedResources`,
  `backupRefusesAPendingDerivedRewind`, `restoreRefusesDerivedMappingsBeyondTheSourceHead`,
  `restoreRefusesDamagedDerivedRestartMetadata`, `restoreRefusesIncompleteDerivedStructure`.
  Their independent oracle is TXN-613: unique future IDs, mappings bounded by the copied source,
  and rejection without repair of unreadable derived state. Counterfactual execution and mutation
  evidence remain for the outer verification phases. The review round passed all 8 tests in
  `:sirix-core:test --tests io.sirix.backup.BackupDerivedResourceTest`, with attached startup,
  allocation, storage-epoch and disabled-cache test tasks excluded.
- Vision check: preserves durable historical derived state and refuses unverifiable copies, matching
  “History is the data model” and “A fast answer is the same answer”; introduces no new default.

## Verification cache pressure ownership

- Symptom: an online backup or restore leaves allocator pressure evicting closed verification
  caches instead of the live manager's caches, risking allocation stalls or exhaustion.
- Trigger: verify a copied derived resource while another buffer manager owns the process-wide
  allocator pressure handler, then request pressure eviction after the verifier closes.
- Root cause: `BufferManagerImpl` construction combined private cache creation with implicit global
  handler registration. The short-lived verifier needed the caches but did not own that registration.
- Fix: the verification factory constructs isolated caches without registering a pressure handler;
  the shared backup/restore verifier uses that factory. Ordinary constructors retain their existing
  registration behavior, and verification never swaps or restores global handlers.
- Why tests missed it: backup tests checked copied bytes and corruption refusals, but never exercised
  the live cache's pressure eviction after verification or while its temporary caches remained open.
- Regression: `BackupPressureHandlerTest` checks real live metadata cache eviction through the
  installed pressure callback after backup, successful restore and failed restore, plus while
  verification caches remain open and after they close. Its independent oracle is TXN-613's live
  cache ownership invariant; the wrong handler leaves the live page cached. No timing bound is used.
  Focused execution passed all 4 cases plus the existing 8 `BackupDerivedResourceTest` cases in
  `:sirix-core:test`, with only those two classes selected and attached startup, allocation,
  storage-epoch and disabled-cache tasks excluded. Counterfactual and mutation runs belong to the
  outer verification phases.
- Vision check: preserves the existing allocation-pressure path and private verification caches,
  supporting “Speed is part of correctness” without changing live-manager defaults.
