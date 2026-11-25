# Backup & Restore

Persistent SirixDB resources use append-only files: a copy of the database directory taken
while no resource writer is active preserves their committed history. IN_MEMORY state is excluded
under [TXN-615](../spec/TXN-transactions-durability.md#txn-615--in-memory-state-lives-with-the-open-database).
The `io.sirix.backup.BackupManager` API automates this safely for a *live* database from within
its owning process. The `backup`/`restore` CLI verbs use the same API.
Sirix has no streaming or incremental backup tool.

Backup and restore apply the strict [store feature policy](DISK_FORMAT.md#uberpage-store-feature-words)
before opening resources or creating a target, including hidden derived resources in the preflight.
Retirement tombstones are not resources and are ignored by that preflight.

## Backup (CLI)

```bash
sirix-cli --location /path/to/database backup /path/to/backup-dir
# verbose summary (per-resource revision + bytes):
sirix-cli -l /path/to/database backup /path/to/backup-dir -v
```

- The target directory must not exist yet (or be empty). On any failure the partial target is
  removed again.
- Works for JSON and XML databases (the type is read from `dbsetting.obj`).

### What a backup contains

Per backed-up resource, its whole directory: `data/sirix.data`, `data/sirix.revisions`,
`ressetting.obj`, `indexes/` and `update-operations/`, plus the database-level `dbsetting.obj` and
`keyselector/`.

It also contains each backed-up resource's hidden `<name>$derived` resource, when it has one. That
is a resource of its own holding the temporal-aggregate data the public resource listing hides; a
backup without it restores a database whose aggregate queries silently fall back to a full scan
([TXN-613](../spec/TXN-transactions-durability.md#txn-613--backups-include-derived-resources)).
`BackupSummary.derivedResources()` names them, and the CLI prints them (`Derived resources
(temporal aggregates): …`) even without `-v`.

Not contained: the IN_MEMORY resources named in `BackupSummary.notBackedUp()` (and their derived
state) under TXN-615, retirement tombstones, and the runtime files listed below.

### What it guarantees

- **Per-resource point-in-time consistency.** For each persistent resource copied, the backup acquires the
  resource's writer lock (the same semaphore `beginNodeTrx` uses, shared across every open
  handle to that resource in the JVM) and holds it while that resource's files are copied. Every
  mutation of those files is made by a read-write transaction, and a read-write transaction cannot
  exist without that semaphore — so while the backup holds it nothing is writing them. (Not every
  mutation happens inside `commit()`: a `KEEP_OPEN_ASYNC_FLUSH` or `KEEP_OPEN_ASYNC_COMMIT`
  transaction flushes pages to `sirix.data` from a background thread *before* its commit. Closing
  such a transaction awaits its pending flushes, so the semaphore still bounds every write.) Since
  the storage is append-only with checksummed dual uber-beacons, the copied pair
  (`sirix.data`, `sirix.revisions`) is byte-stable and represents exactly the most recent
  committed revision (reported in the summary).
- **Consistent derived resources.** A hidden derived resource is written by the database's own
  maintainer thread rather than by a caller's transaction, so its writer semaphore stays taken for
  that maintainer's lifetime and is useless as a copy barrier. The backup instead holds the
  maintainer's *history lock* — the lock every derived publication, aggregate fold, summary fold and
  rewind takes — across both halves of the resource, so the source and its derived store are copied
  from one state the database never left. A derived store legitimately lags its source; the restored
  copy resumes from its own durable frontier (TXN-612) and catches up. A pending truncation rewind
  causes a refusal naming the derived resource. The durable frontier and mappings are checked against
  the copied source head during backup and restore; a later mapping is rejected.
- **Fresh verification.** Verification reads the copied bytes with private revision metadata and
  page caches. A failed copy can be retried at the cleaned target without reusing the previous
  copy's offsets, hashes, revision index or superblock validation. An existing nonempty store must
  have a committed beacon; intact revision-0 records and pages cannot substitute for missing beacons.
- **Live cache ownership.** Verification uses private caches without changing the live allocator
  pressure handler, during either successful or failed backup/restore.
- **Resource IDs.** The target metadata reserves every copied resource ID, including hidden siblings
  created or rebuilt after the initial database metadata copy.
- **Crash-recovered state.** Acquiring the writer lock first runs the engine's leftover-commit
  recovery (stale `.commit` marker truncation), so the backup never captures a torn in-flight
  commit.
- **Full history.** SirixDB never rewrites old revisions, so the backup contains *all*
  revisions up to the backup point, per resource.
- **Independence.** Commits made to the original after the backup do not affect the backup.
- **Skipped resources.** `BackupSummary.notBackedUp()` lists the IN_MEMORY resources omitted
  under TXN-615. The CLI prints these names even without `-v`; their files and settings are
  excluded along with their derived state.
- Runtime files are skipped: the database `.lock` file, the per-resource intent-log
  (`log/`, including the `.commit` marker) and atomic-write temporaries (`*.tmp*`).

### Caveats

- Invoke `BackupManager` within the running server for an online backup. Before using the
  standalone CLI, close the server's database handles: the CLI cannot open a database owned by
  another process. See the [database ownership rules](operations.md#10-known-limitations-and-operational-caveats).
- If another transaction holds a resource's writer lock, the backup **fails fast** (after the
  ~5 s lock-acquisition timeout) rather than copy a moving target. Retry when the writer is
  done, or use shorter transactions.
- An interrupted backup defers the interrupt status until its writers, sessions and database handle
  are closed and its partial target is removed. It restores the caller's interrupt status at the
  outer boundary, including on failure; cancellation does not retain database ownership.
- If a derived resource's maintenance does not become quiescent within the same ~5 s, the backup is
  **refused** with a message naming that resource, rather than copying a store that is mid-build.
  Retry once its temporal-aggregate maintenance has caught up.
- Visible resources created after the initial resource listing are not included. A hidden derived
  sibling created or rebuilt while opening a listed source is included with that source. Avoid
  creating/removing resources during a backup.
- The backup briefly blocks writers per resource for the duration of that resource's file copy
  (readers are unaffected).

## Restore

```bash
sirix-cli --location /path/to/backup-dir restore /path/to/new-database
```

`--location` points at the **backup**; the positional argument is the directory the database is
restored to (must not exist yet or be empty). The restore:

1. validates the backup has the database directory structure,
2. copies it to the target, excluding runtime files and IN_MEMORY leftovers under TXN-615,
3. **verifies** the result. The hidden derived resources go first: a derived resource is internal and
   has no user session, so each one is opened through the tool storage path — strict feature
   compatibility, directory structure, the uber beacon, the revision-root page and the durable
   frontier/mapping metadata. Mappings beyond the copied source head are rejected. They are
   verified before the visible resources because opening a visible resource installs its derived
   maintainer, which rebuilds a derived store it cannot read — repairing exactly the damage this pass
   exists to report. Each visible resource is then opened with a read-only transaction at its most
   recent revision. Together this exercises superblock validation, beacon checksums, the checksummed
   revision slots and the page-checksum chain on the root page,
4. **reports** what it restored: `BackupSummary.resources()` for the visible resources and
   `BackupSummary.derivedResources()` for the hidden derived ones,
5. on *any* failure deletes the partial target, so you never end up with a half-restored
   database.

The restored database is fully independent of the original and can live at any path:
`dbsetting.obj` is re-bound to the actual directory on first open.

### Duplicate database-id re-keying

Every database carries a persisted id (used to key the global page caches). A restored copy
necessarily starts with the *same* id as its original. This is handled automatically: if both
directories are opened in the same JVM, the second one is transparently re-keyed to a fresh id
on open (its `dbsetting.obj` is rewritten, with a log message). A restored copy can therefore
run alongside its original without any manual step.

## Cold-copy alternative (stopped server)

If the SirixDB process is stopped (or the database is guaranteed to have no active writer), a
plain recursive copy is an equally valid backup of its persistent resources — no tooling required:

```bash
cp -a /path/to/database /path/to/backup-dir     # or rsync -a
```

A cold copy includes the hidden `$derived` resources along with everything else, and may include
the runtime files the CLI skips (`.lock`, `log/.commit`, `*.tmp*`). `restore` accepts such copies
too — or simply point the server at the copied directory. Do **not** cold-copy while a writer is
active: a commit could interleave with the copy and leave the data/revisions pair inconsistent; use
`BackupManager` within the owning process for an online backup instead.

Copying IN_MEMORY settings does not preserve their committed state; their lifetime and
leftover cleanup follow TXN-615.

Filesystem snapshots (LVM, ZFS) are safe only if the snapshot is atomic across all files of
the resource. ext4 + LVM is fine; per-file snapshots are not.
