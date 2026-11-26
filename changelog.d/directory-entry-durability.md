---
section: Fixed
---
- **A power loss can no longer lose a name the engine relies on.** File contents were forced but
  their directory entries never were, on any platform. After a crash, a committed revision could
  lose its index catalogue, returned settings replacements could revert, created databases,
  resources and backups could be missing, and removed resources or databases could come back. One
  shared helper, `DurableFiles`, now provides the directory barrier: a directory force on Linux,
  `F_FULLFSYNC` on macOS, and on Windows `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` or
  `FlushFileBuffers` on a directory handle, through FFM downcalls. Each site states its guarantee in
  [TXN-616](spec/TXN-transactions-durability.md#txn-616--directory-entry-durability). An ordinary commit
  adds no system call; a commit that writes an index catalogue adds one directory force. Resource removal now renames
  the resource to a hidden tombstone before deleting it, so a crash leaves the intact resource or
  nothing. The resource settings file, the encryption keyset and backup copies are now forced. A
  filesystem that rejects a directory force fails the operation instead of skipping the barrier
  ([write-up](docs/postmortems/2026-11-directory-entry-durability.md)).
