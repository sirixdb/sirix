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
