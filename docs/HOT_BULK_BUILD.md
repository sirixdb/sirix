# HOT Bulk Construction — the reasoning record

Why the HOT bulk path is shaped the way it is: the canonicity verdict that decides how bulk
construction can be gated, the memory arithmetic that decided how the projection slot sink
persists, and the designs deliberately left for a v2. This document holds only reasoning that
has no other owner — an argument a future reader or implementer needs and that the source
cannot carry.

It does **not** describe shipped behavior:

- Which index families the parallel load maintains, what it refuses, and what that costs:
  [`BULK_IMPORT.md`](BULK_IMPORT.md).
- The post-pass projection build's write path, including the bulk slot sink it engages:
  [`PROJECTION_INDEX_DEEP_DIVE.md`](PROJECTION_INDEX_DEEP_DIVE.md) §6.
- Local safety invariants of the mechanisms below — the leaf-fit bound's overflow argument
  (`HOTBulkBuilder.fitsLeafPage`), the fresh-subtree registration contract
  (`AbstractHOTIndexWriter.registerFreshSubtree`), and the epoch transplant's
  tree-state-independence (`ProjectionIndexHOTStorage.adoptBulkSlotAccumulation`): the class
  javadoc owns each, and is the copy to trust.

Evidence probe for §1:
`bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInsertionOrderShapeProbe.java`.

---

## 1. Shape canonicity — bulk output is canonical, the incremental trie is not

This is the finding that determines what a bulk-construction gate is allowed to assert.

- **`HOTBulkBuilder.build(S)` is deterministic in the key set.** Same sorted entries in, same
  structure out, regardless of the order the pairs were fed to the loader — verified empirically
  (`shape bulk==bulk2 (determinism): true` for both probe key patterns) and by code: a pure
  two-phase recursion whose only inputs are the sorted arrays. With a fixed page-key allocator
  start, page keys are deterministic too (allocation order is the DFS order of construction).
  **Byte-identity between two bulk builds of the same entries is therefore a valid gate.**
- **The live incremental trie is neither canonical nor order-independent.** The same 20 000-key
  set through the production `doIndex` in ascending vs descending order: 40 leaves / 3 indirects
  / height 2 vs 40 leaves / 20 indirects (a 6-deep chain); every shape pair unequal except
  asc == shuffled, a post-consolidation coincidence. The code says so itself — the periodic
  consolidation sweep exists because incremental insertion over-partitions into under-full frozen
  leaves an insert never re-routes to, and it packs only toward `CONSOLIDATION_TARGET`
  (¾ of `HOTLeafPage.MAX_ENTRIES`), not toward the bulk cut.
- **Consequence — a bulk-vs-incremental gate must be semantic, never byte-identity.** Byte
  identity is reserved for bulk-vs-bulk reproducibility. Equivalence against the incremental
  result is asserted as identical point reads, identical range-cursor sequences, and — for the
  projection — the census differential (`readAllRowGroupsFromColumnSegmentSlots` validates the
  exact ordered descriptor set and every segment hash, so a lost, duplicated or misplaced slot
  fails loudly), plus zero findings from the invariant oracle `HOTMalformedSubtreeDetector`,
  which both construction routes must pass.
- **The sharp form of the question** — does `spliceBulkBuiltRoot` produce trees byte-identical to
  incremental insertion of the same keys, and do its existing users prove that or merely tolerate
  divergence? **No, and they tolerate it.** The probe drove the real
  `createBulkLoader → flush → spliceBulkBuiltRoot` path against the real `doIndex` on identical
  key sets: interior structure differs even with canonical leaf packing (bulk 40 leaves /
  9 indirects vs incremental-ascending 40 / 3 — an SMHP fanout-32 frontier against
  integrate/pull-up shapes). The family's end-to-end gate, `JsonCASIndexBuildTest`, asserts
  postings-set equality ("postings … must not depend on how the index was built") plus a
  zero-self-heal witness during bulk builds; no structural or byte assertion exists anywhere in
  the family's tests, and per the verdict above none should.
- **All four construction routes are read-equivalent** (20 000 keys × 2 key patterns × point
  lookups — identical postings). That is the property the gates encode.

**Open, predating this campaign:** the incremental path in descending order leaves **stale
`height` fields** (nested indirects all claiming `h=1`). Harmless while readers route by
invariants, but `HOTIncrementalInsert.integrate`'s `parent.height > B.height` decision consumes
heights, so any future "bulk-build the tail, integrate incrementally" hybrid must fix this first.

---

## 2. Why the projection slot sink accumulates with a cap, not one splice at the end

`spliceBulkBuiltRoot` refuses a non-empty tree, so a bulk sink can only serve a build that never
reads its own persisted tree — and the obvious shape, accumulate the whole build and splice once,
is what the arithmetic below rejects.

Three premises make the arithmetic the binding constraint rather than visibility or flushing:

- **Flush epochs never relieve a build in progress.** The async-flush epochs
  (`TransactionIntentLog.snapshot`/`cleanupSnapshot`) attempt only `KeyValueLeafPage`s; every HOT
  page is pinned for the life of the transaction. The relief valve is the bounded pinned-trie
  spill (`NodeStorageEngineWriter.isPinnedTrieSpillPageEligible`) — and during
  `HOTBulkBuilder.build` none of the pages are TIL-registered yet, so **the built pages are
  unconditionally resident until the splice**, on top of the loader arena.
- **A leaf owns a 64 KiB slot segment regardless of fill** (`HOTLeafPage` ctor →
  `allocator.allocate(DEFAULT_SIZE)`), so built-page cost is per page, not per byte.
- **Visibility is already safe.** The load-time rider stale-marks the index for the whole load
  (`ProjectionBulkLoad` writes `ProjectionIndexMetadata.staleTombstone()` into slot 0), so
  intermediate revisions never serve from a half-built projection. Accumulating longer buys no
  correctness problem — only memory.

Arithmetic for the dominant lane (order labels, 8-byte keys + ~30-byte payloads; slot-loader arena
≈ 70 B/entry — key + payload block bytes + a 24 B index; built tree ≈ 64 KiB / 512 entries =
128 B/entry):

| entries per splice | arena | built pages | total transient | vs a ≤ 2 G transient rule |
|---|---|---|---|---|
| 1 M | 70 MB | 128 MB | ~0.2 GB | fine |
| 8 M | 560 MB | 1.0 GB | ~1.6 GB | fine — the chosen cap |
| 30 M | 2.1 GB | 3.8 GB | ~5.9 GB | FAIL |
| 100 M | 7.0 GB | 12.5 GB | ~19.5 GB | FAIL outright |

- **(a) Whole-build splice-once — rejected.** It fails from roughly 15 M entries, and a projection
  build is routinely an order of magnitude past that.
- **(b) Bulk only the first window, per-entry afterwards — rejected as the primary shape.** The
  win degenerates to the first window's share of the build (≈ 1 % at 100 M).
- **(a-bounded) accumulate-with-cap plus a graceful drain — chosen.** The accumulator runs while
  the tree is virgin, capped at `BULK_SLOT_MAX_ENTRIES` / `BULK_SLOT_MAX_ARENA_BYTES`
  (8 M entries / 512 MB, `ProjectionIndexHOTStorage`). A build that ends under the cap sorts,
  folds last-writer-wins and splices once — the full win, which covers every fresh build up to
  the cap. A cap trip splices the accumulated prefix (legal: the tree is still virgin) and falls
  through to the per-entry path. The floor is the property that makes the cap safe to pick
  aggressively: **the drain is the production write path in its best-case key order**, so the
  bounded shape is never worse than not accumulating at all.

The cap is a memory bound, not a tuned constant — move it only against the table above.

---

## 3. Designs deliberately not built

- **Right-edge append splice (removes the cap).** Both dominant lanes ascend strictly across the
  whole build, so general subtree merge is never needed: chunk-wise, pop the rightmost leaf
  (≤ 512 entries), bulk-build `R(leaf ∪ chunk)`, attach on the right spine at the I11-chosen depth
  (the first spine node whose block bits are all more significant than `msdb(oldMax, newMin)`),
  path-copy, and propagate height/partials through the existing Stage-3c machinery. Transient
  memory is bounded at chunk size, so the full bulk win holds at any scale. Per §1 the gate is
  semantic + detector: the appended shape need not equal `bulkBuild(S_union)`.
- **Wiring the load-time rider to the slot sink.** `ProjectionBulkLoad` still persists every slot
  through the per-entry path; only the post-pass `ProjectionIndexBuilder` engages the accumulator.
  The blocker is concrete: the rider's auto-commit windows would lose an accumulated window at
  commit, so it needs a pre-commit finalize hook to splice at each window boundary. Until that
  exists, wiring it would trade a bounded win for a correctness hazard. The same missing hook is
  why any auto-committing load cannot accumulate past its first window: after that commit the
  tree is no longer virgin, and `spliceBulkBuiltRoot` refuses it.
- **A residency cap for index-everything definitions.** The PATH/CAS/NAME loaders
  (`AbstractHOTBulkIndexLoader`) hold the whole entry set until `flush()` — roughly
  `n × (key bytes + 20)` — which is the price of a single-pass canonical construction and is fine
  for the selective definitions measured in [`BULK_IMPORT.md`](BULK_IMPORT.md). A definition that
  indexes *everything* over a 100 M-row corpus has no bound at all, and no per-family flush can be
  interposed because a family is one tree: the loader would have to spill its arena, or the family
  would have to be deferred to the post-pass builder. Unsolved, and the reason index-everything
  definitions at that scale are not a supported shape.
- **A sorted-merge reattach for side references.** `AbstractHOTIndexWriter.reattachSegmentRefs` is
  `O(refs × leaf lookup)` — fine per rebuild, and fine for the deferred attaches the slot sink
  replays after its splice, but a whole 100 M-row projection tree bulk-built with populated side
  maps needs a sorted merge walk instead.

---

## Appendix A — per-entry HOT slot-write call sites

The per-entry slot writes a fresh projection build performs, and the key pattern each produces —
the input any bulk sink has to cover. Every site funnels into
`ProjectionIndexHOTStorage.writeSlotValue` / `putBlob` — one `prepareLeafOfTree` descent, a
branch-escape dispatch, a `leaf.put`, and an update-or-split cascade per call — unless noted. Each
slot key reaches the trie as its sign-flipped 8-byte big-endian image (`slotKeyBytes`), so the
namespaces below are ordered by the same comparison the trie uses. "Append-ordered" means keys are
non-decreasing across the calls of one fresh build. Symbols, not line numbers: these resolve by
grep and survive edits.

| Site | Key formula | Order in a fresh build | Bulk candidate |
|---|---|---|---|
| Row-group descriptor slot — `ProjectionIndexHOTStorage.putRowGroupAsColumnSegmentSlots`, from `ProjectionIndexBuilder`, `ProjectionBulkLoad` and `ProjectionIndexChangeListener.writeRowGroup` | `rowGroupId << 16` (slot kind 0) | rowGroupId contiguous from 1 in emit order → append-ordered | yes |
| Column-segment slots — `writeChangedColumnSegmentSlots`, per segment | `(rowGroupId << 16) \| (columnSegmentId + 1)` | ascending within a group, groups ascend → append-ordered | yes |
| Segment overflow pages — `putSegmentPage` | side-map key `(ownerSlotKey << 16) \| columnSegmentId`, attached to the OWNING LEAF, not the trie | follows the owning slot | attach after the build (`reattachSegmentRefs`) |
| Order-directory slots — `ProjectionStructuralOrderDirectory.HotSlotStore.put` → `putStructuralOrderSlot` | `2^50 + nodeKey`, ONE SLOT PER RECORD | strictly ascending in a fresh build (the in-order append lane mints them) | **yes — ~95 % of all per-entry insertions, the primary target** |
| Record locator — `ProjectionRecordLocator.put` → `putRawSlot` | `Long.MIN_VALUE \| recordKey` (sign-bit namespace) | update/move path (rebalance); low volume in a fresh build | low priority |
| Fences — `ProjectionIndexFences` (`putBlob`) | `CHUNK_SLOT_BASE + chunkId` (2^42 namespace), `ORDER_HEADER_SLOT` | chunkId ascending, header last → append-ordered | yes |
| Bloom chunks — `ProjectionBloomChunks` (`putBlob`) | per-column bloom block + chunk slots (2^43 namespace) | per-column ascending chunks | yes |
| Set-summary chunks — `ProjectionSetSummaryChunks` | `slotKey(column)` (2^44 namespace) | per column | yes |
| Metadata blob — `ProjectionIndexBuilder`, `ProjectionBulkLoad`, the change listener | slot 0 | once, strictly last | trivial |
| PATH/CAS/NAME entries — `AbstractHOTIndexWriter.doIndex` via the family builders | serializer-specific composite keys | traversal order (not sorted; the loader sorts) | already bulk for virgin trees (`create*BulkLoader`) |
