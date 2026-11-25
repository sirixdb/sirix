---
section: Removed
---
- **`MemorySegmentAllocator.resetSegment` and the virtual-memory layer's `discardToZeros`.** Their
  documented contract — the next read observes zeros and the resident size drops, with the address
  still valid — cannot be honoured on macOS: the POSIX implementation issued `MADV_DONTNEED`, which
  Darwin treats as advice that neither zeroes nor releases anonymous pages, and the releasing
  variant (`MADV_FREE_REUSABLE`) only marks pages reclaimable, so the old bytes stay readable until
  the kernel takes them. Nothing in the engine called either method — recycled frame slots are
  deliberately left dirty — so the API is gone rather than carrying a promise one platform silently
  broke ([write-up](docs/postmortems/2026-11-compact-image-decode-arena.md)).
