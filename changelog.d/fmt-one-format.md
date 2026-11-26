---
section: Changed
---
- **1.0 is the only on-disk format.** Other superblock layout, page-envelope and persisted
  `binaryEncoding` versions are refused with an "unsupported format version" error naming
  the version found, before mutation on ordinary open and the repair/recovery paths enforced
  by [FMT-401](spec/FMT-disk-format.md#fmt-401--10-format-baseline). Settings and superblocks
  must carry the same nonzero resource UUID, and revisions records must use the 32-byte stride;
  zero-UUID and zero-stride acceptances are removed. History-preserving export and import of
  1.0 databases remain available under
  [FMT-402](spec/FMT-disk-format.md#fmt-402--history-preserving-export-and-import), including
  its recorded release gaps. Repair discovery remains partial: complete envelope scanning
  before destructive repair is deferred to `sirix-repair-format-gate`. Explicit truncate
  checks the target and its bounded root window before beacon publication; FILE_CHANNEL
  recovery permits a lost revisions header with valid tail-log evidence.
