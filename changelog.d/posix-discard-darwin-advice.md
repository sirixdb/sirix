---
section: Removed
---
- **`MemorySegmentAllocator.resetSegment` and the virtual-memory layer's `discardToZeros`.** Neither
  method had an engine caller, and the POSIX implementation could not guarantee the documented
  zeroing on macOS. Recycled frame slots remain deliberately dirty; the
  [write-up](docs/postmortems/2026-11-compact-image-decode-arena.md#outcome-the-pool-is-withdrawn-and-the-defect-stays-open)
  owns the platform rationale.
