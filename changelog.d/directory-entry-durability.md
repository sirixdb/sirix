---
section: Fixed
---
- **A power loss can no longer lose a name the engine relies on.** File contents were forced but
  their directory entries never were, on any platform. After a crash, a committed revision could
  lose its index catalogue, returned settings replacements could revert, created databases,
  resources and backups could be missing, and removed resources or databases could come back.
  The directory-entry guarantees, platform barriers and failure handling are owned by
  [TXN-616](spec/TXN-transactions-durability.md#txn-616--directory-entry-durability).
  The resource settings file, the encryption keyset and backup copies are now forced
  ([write-up](docs/postmortems/2026-11-directory-entry-durability.md)).
