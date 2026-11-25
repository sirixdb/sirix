---
section: Changed
---
- **Compact HOT-leaf images reuse native storage.** Their packed bytes come from
  a reusable native block the decoded page owns and returns on close, instead of one automatic arena
  per decoded fragment — which cost an arena object whenever escape analysis did not remove it, plus
  `Cleaner` bookkeeping that followed GC timing. A warmed lease and release now allocates no heap. A
  block is its image rounded up to the pool's class granularity, and that
  rounded size is what the fragment cache charges; when the allocator admits no block, the decode
  falls back to its former GC-owned image. The pool's slabs are bounded by the share that
  already bounds what the HOT fragment cache's compact half retains — the cache these blocks hold
  storage for — and charge nothing of the page-frame budget, so cached compact images cannot starve
  a page frame; a full pool refuses and the decode takes that same fallback. Storage comes back when
  the cache evicts an image: its block returns to the pool and the next decode reuses it. Read
  results are unchanged
  ([write-up](docs/postmortems/2026-11-compact-image-decode-arena.md)).
