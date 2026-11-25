---
section: Changed
---
- **Compact HOT-leaf images reuse native storage.** Their packed bytes come from
  a reusable native block the decoded page owns and returns on close, instead of one automatic arena
  per decoded fragment — which cost an arena object whenever escape analysis did not remove it, plus
  `Cleaner` bookkeeping that followed GC timing. A warmed lease and release now allocates no heap. A
  block is its image rounded up to the pool's class granularity, and that
  rounded size is what the fragment cache charges; when the allocator admits no block, the decode
  falls back to its former GC-owned image. The pool's slabs share the allocator's permanent
  commitment ceiling with page frames, so an idle slab is given back under memory pressure — after
  the page caches have evicted and only up to what the refused allocation is short of. Read results
  are unchanged
  ([write-up](docs/postmortems/2026-11-compact-image-decode-arena.md)).
