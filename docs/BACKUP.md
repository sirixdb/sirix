# Backup & Restore

SirixDB databases are directories of append-only files, which makes backups simple: a copy of
the database directory taken while no commit is in flight is a complete, valid backup. The
`backup`/`restore` CLI verbs (and the underlying `io.sirix.backup.BackupManager` API) automate
this safely for a *live* database.

## Online backup (CLI)

```bash
sirix-cli --location /path/to/database backup /path/to/backup-dir
# verbose summary (per-resource revision + bytes):
sirix-cli -l /path/to/database backup /path/to/backup-dir -v
```

- The target directory must not exist yet (or be empty). On any failure the partial target is
  removed again.
- Works for JSON and XML databases (the type is read from `dbsetting.obj`).

### What it guarantees

- **Per-resource point-in-time consistency.** For each resource, the backup acquires the
  resource's writer lock (the same semaphore `beginNodeTrx` uses, shared across every open
  handle to that resource in the JVM) and holds it while that resource's files —
  `data/sirix.data`, `data/sirix.revisions`, `ressetting.obj`, `indexes/`,
  `update-operations/` — are copied. Since all file mutations happen inside `commit()` under
  that lock and the storage is append-only with checksummed dual uber-beacons, the copied pair
  (`sirix.data`, `sirix.revisions`) is byte-stable and represents exactly the most recent
  committed revision (reported in the summary).
- **Crash-recovered state.** Acquiring the writer lock first runs the engine's leftover-commit
  recovery (stale `.commit` marker truncation), so the backup never captures a torn in-flight
  commit.
- **Full history.** SirixDB never rewrites old revisions, so the backup contains *all*
  revisions up to the backup point, per resource.
- **Independence.** Commits made to the original after the backup do not affect the backup.
- Transient files are skipped: the database `.lock` file, the per-resource intent-log
  (`log/`, including the `.commit` marker) and atomic-write temporaries (`*.tmp*`).

### Caveats

- The writer lock is **JVM-local**. Run the backup either embedded in the process that owns the
  database (e.g. from the server itself) or while no other process has the database open.
  SirixDB assumes single-process access to a database directory in general.
- If another transaction holds a resource's writer lock, the backup **fails fast** (after the
  ~5 s lock-acquisition timeout) rather than copy a moving target. Retry when the writer is
  done, or use shorter transactions.
- Resources created while the backup is running are not included (the resource list is
  snapshotted once at the start). Avoid creating/removing resources during a backup.
- The backup briefly blocks writers per resource for the duration of that resource's file copy
  (readers are unaffected).

## Restore

```bash
sirix-cli --location /path/to/backup-dir restore /path/to/new-database
```

`--location` points at the **backup**; the positional argument is the directory the database is
restored to (must not exist yet or be empty). The restore:

1. validates the backup has the database directory structure,
2. copies it to the target,
3. **verifies** the result by opening the restored database and every resource read-only at its
   most recent revision (this exercises superblock validation, beacon checksums, the
   checksummed revision slots and the page-checksum chain on the root page),
4. on *any* failure deletes the partial target, so you never end up with a half-restored
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
plain recursive copy is an equally valid backup — no tooling required:

```bash
cp -a /path/to/database /path/to/backup-dir     # or rsync -a
```

A cold copy may include the harmless transient files the CLI skips (`.lock`, `log/.commit`,
`*.tmp*`). `restore` accepts such copies too — or simply point the server at the copied
directory. Do **not** cold-copy while a writer is active: a commit could interleave with the
copy and leave the data/revisions pair inconsistent; that is exactly the case the online
`backup` verb exists for.
