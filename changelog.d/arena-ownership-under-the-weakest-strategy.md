---
section: Fixed
---
- **Serializer-local confined arenas and the LZ4 staging buffer are now pinned by tests that can
  fail.** Two reported resource-ownership leaks no longer exist on `main` — the scoped LZ4
  decompression path stages heap input in pooled allocator scratch inside the `try` that returns it,
  and a confined writer region table closes its own arena — but neither invariant had a lane that
  could notice a regression: the confined-arena assertion passes under the JVM's deterministic
  arena strategy whichever close path the code takes. A new per-JVM lane runs it under the `auto`
  strategy a GraalVM native image uses, the decompression path's failure accounting is asserted
  exactly-once per failed read, and the two comments that still described the old ownership were
  corrected
  ([write-up](docs/postmortems/2026-11-resource-ownership-lz4-staging-and-confined-arenas.md)).
