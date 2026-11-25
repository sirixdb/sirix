---
section: Fixed
---
- **Discarding a native segment really discards it on macOS.** `MemorySegmentAllocator.resetSegment`
  documents that the next read observes zeros and the resident size drops, but the POSIX
  implementation issued `MADV_DONTNEED`, which Darwin treats as advice: it neither zeroed nor
  released anonymous pages, so the previous contents stayed readable and a caller could not tell a
  silently skipped discard from a successful one. macOS now uses `MADV_FREE_REUSABLE`; Linux keeps
  `MADV_DONTNEED` ([write-up](docs/postmortems/2026-11-compact-image-decode-arena.md)).
