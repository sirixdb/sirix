# HOT Bulk Construction for Secondary Structures — Design (Campaign #76, Phase 1)

**Status:** design + measured evidence; nothing here is committed. Companion probe:
`bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInsertionOrderShapeProbe.java`.

**Scope v1:** fresh subtrees only — no merge into populated trees, no tombstone
migration, no cross-revision fragment interplay beyond what `registerFreshSubtree`
already guarantees.

---

## 0. Premise audit — what already exists (and what the campaign premise got wrong)

The campaign premise assumed bottom-up bulk construction for HOT structures had to be
designed from scratch. The tree at `226ceaac8` already contains most of the machinery:

| Piece | Where | State |
|---|---|---|
| Bottom-up builder (`R(S)` + SMHP compression) | `io.sirix.index.hot.HOTBulkBuilder` | **EXISTS**, formally modeled (`docs/HOT_FORMAL_FOUNDATION.md` Thm 1, `HOTFormalModelTest`), property-tested |
| Accumulate–sort–fold–build loader | `io.sirix.index.hot.AbstractHOTBulkIndexLoader` (+ `HOTBulkIndexLoader` for CAS/NAME, `HOTLongBulkIndexLoader` for PATH) | **EXISTS**; block-arena keys, int-permutation sort, exact-size entry list |
| Root splice for an empty tree | `AbstractHOTIndexWriter.spliceBulkBuiltRoot` (AbstractHOTIndexWriter.java:1136) | **EXISTS**; refuses non-empty trees |
| TIL adoption of a fresh HOT subtree | `AbstractHOTIndexWriter.registerFreshSubtree` → `registerFreshPage` (AbstractHOTIndexWriter.java:4613–4661) | **EXISTS**; post-order `log.put(ref, PageContainer(page,page))`, `setCompleteDump(true)` on fresh leaves |
| Bulk-load engagement for PATH/CAS/NAME | `PathIndexBuilder`/`CASIndexBuilder`/`NameIndexBuilder` constructors | **EXISTS**, but only when the builder starts against an **empty** tree (create-index-over-existing-revision) |

Two premise corrections:

1. **Adoption is not `KeyedTrieWriter.prepareLeafOfTree` for HOT.** That pair is the
   *document-index* (bit-decomposition radix trie over `IndirectPage`s) adoption used by
   `ParallelBulkJsonImporter`. HOT subtrees enter the TIL through
   `registerFreshSubtree`, which is the exact HOT analog: children registered before
   parents, each page its own `(complete==modified)` container, fresh leaves marked
   complete-dump so commit emits full first fragments and readers never chase a chain.
   Nothing new needs designing here — v1 reuses it unchanged.

2. **HOT pages are never epoch-flushed.** The async-flush epochs
   (`TransactionIntentLog.snapshot()`/`cleanupSnapshot()`) attempt only
   `KeyValueLeafPage`s. Every HOT page (leaf and indirect) is **pinned**
   (TransactionIntentLog.java:337–397, `PINNED_GENERATION`): one stable TIL identity for
   the life of the transaction, serialized at final commit — or earlier by the bounded
   **pinned-trie spill** under memory pressure
   (`TransactionIntentLog.capturePinnedSpillCandidates`,
   `NodeStorageEngineWriter.isPinnedTrieSpillPageEligible`,
   NodeStorageEngineWriter.java:2092–2119). "Flush-eligible under the async-flush
   epochs" therefore translates to **spill-eligible**, see §5.

What does **not** exist — the actual campaign gap. The campaign is therefore NOT
"invent a bottom-up builder"; it is **extend and route the existing
`AbstractHOTBulkIndexLoader` family** into the write populations that bypass it:

- The **projection family** (column-segment slots, order-directory slots, record
  locator, fences, Bloom chunks, global-dictionary records) writes per-entry through
  `ProjectionIndexHOTStorage.writeSlotValue` (ProjectionIndexHOTStorage.java:3397):
  descent (`prepareLeafOfTree`) + branch-escape dispatch + `leaf.put` +
  update-or-split cascade, once per slot. `ProjectionIndexHOTStorage` is a sibling
  subclass of `AbstractHOTIndexWriter` that never touches the loader family
  (`spliceBulkBuiltRoot`'s only caller is `AbstractHOTBulkIndexLoader.flush`:275;
  the loaders' only consumers are the PATH/CAS/NAME builders gated on
  `isEmptyTree()`). `ProjectionBulkLoad` keeps the *builder* alive across
  auto-commits (process-global ACTIVE map keyed `resourceKey#defId`,
  ProjectionBulkLoad.java:90; `DEFAULT_ROW_GROUP_PUBLISHER` is a method ref to
  `putRowGroupAsColumnSegmentSlots`, :79) but still persists through the per-entry
  path — it batches BUILD state, not WRITES.
- **The dominant per-entry population is the order-label directory**: one slot per
  record under `2^50 + nodeKey`, strictly ascending — ~95 % of all per-entry HOT
  insertions in a fresh build, and already perfectly loader-shaped. (The
  consolidated `bulkproj` worktree — NOT this base — already routes post-pass label
  minting through an in-order label lane; this design assumes that lane exists and
  must not re-fix it.)
- The **parallel importer refuses PATH/CAS/NAME/valid-time maintenance**
  (ParallelBulkJsonImporter.java:215–218 on `bulkHasPrimitiveIndexes()`, =
  `indexController.hasAnyPrimitiveIndex()` via JsonNodeTrxImpl.java:1202–1204; the
  non-parallel sink honors the same signal — WtxBulkRecordSink.java:40); lifting it
  wants per-family accumulation in workers + one flush into the EXISTING
  `create*BulkLoader()` per family at load end.
- The existing `HOTBulkBuilder` **under-packed leaves by ~7×** on realistic
  dense/strided key sets (§3; fixed in this worktree) — feeding it 100M-scale
  projection slots without the packing fix would have multiplied page count, TIL
  residency, and I/O.

---

## 1. Shape canonicity — the finding that fixes the gate design

Full evidence in the phase report; operative summary:

- **`HOTBulkBuilder.build(S)` is deterministic in the key set** — same sorted entries in,
  same structure out, regardless of the order pairs were fed to the loader. Verified
  empirically (probe: `shape bulk==bulk2 (determinism): true` for both key patterns) and
  by code (pure two-phase recursion; the only inputs are the sorted arrays).
  With a fixed page-key allocator start, page keys are deterministic too (allocation
  order is the DFS order of construction), so **byte-identity between two bulk builds
  of the same entries is a valid gate**.
- **The live incremental trie is NOT canonical and NOT order-independent.** Same
  20 000-key set through the production `doIndex` in ascending vs descending order:
  40 leaves/3 indirects/height-2 vs 40 leaves/20 indirects (a 6-deep chain);
  every shape pair unequal except asc==shuffled (post-consolidation coincidence).
  The code says so itself: the periodic consolidation sweep exists because "the
  incremental insert over-partitions … under-full frozen leaves an insert never
  re-routes to" (AbstractHOTIndexWriter.java:3384–3390), and it packs only toward
  `CONSOLIDATION_TARGET = 384` (¾ of `MAX_ENTRIES`), not toward the bulk cut.
- Consequence: **the v1 gate is layered, not byte-identical to the incremental
  result** (§6). Byte-identity is reserved for bulk-vs-bulk reproducibility; the
  bulk-vs-incremental gate is semantic (reads + census) plus the invariant oracle
  (`HOTMalformedSubtreeDetector`), which both construction routes must pass with zero
  findings.
- **The sharper form of the question** ("does `spliceBulkBuiltRoot` produce trees
  byte-identical to incremental insertion of the same keys, and do its existing users
  prove that or merely tolerate divergence"): **No, and they tolerate it.** The probe
  drove the REAL `createBulkLoader → flush → spliceBulkBuiltRoot` path against the
  REAL `doIndex` on identical key sets: interior structure differs even after the
  packing fix (bulk 40 leaves/9 indirects vs incremental-ascending 40/3 — SMHP
  fanout-32 frontier vs integrate/pull-up shapes). The loader family's only
  end-to-end user gate, `JsonCASIndexBuildTest`, asserts postings-set equality
  ("postings … must not depend on how the index was built", :190–192) plus a
  zero-self-heal witness during bulk builds (:176–180) — no structural or byte
  assertion exists anywhere in the family's tests.
- Additional measured facts the design leans on: (a) all four construction routes are
  **read-equivalent** (20 000 keys × 2 patterns × point lookups — identical postings);
  (b) the incremental path in descending order leaves **stale `height` fields**
  (nested indirects all claiming `h=1`) — a latent divergence source for
  `integrate`'s `parent.height > B.height` decision, noted as a separate follow-up,
  not in v1 scope.

---

## 2. The construction algorithm (as implemented, with the v1 packing fix)

Input: a strictly-ascending, duplicate-free `(key, payload)` stream (the loader
guarantees this: block-arena accumulation, one parallel int-sort by
`(compositeKey, nodeKey)`, group-fold per distinct key —
AbstractHOTBulkIndexLoader.java:223–277).

Phase A — `buildR` (HOTBulkBuilder.java:191): Binna's binary Patricia trie by MSDB
recursion over the sorted array. `O(n · keylen)` worst case, no allocation beyond the
recursion frames and `RNode` records.

Phase B — `bulk` compression (HOTBulkBuilder.java:268):

- **Leaf cut:** a subtree of ≤ `HOTLeafPage.MAX_ENTRIES` (512) entries is speculatively
  packed into one `HOTLeafPage` (64 KiB slot heap, `DEFAULT_SIZE`); byte overflow
  recurses into the branch. A leaf page is always a *complete `R(S)` subtree* —
  the invariant that makes multi-entry pages routable (foundation §3.2).
- **Interior assembly:** greedy SMHP frontier expansion to ≤ 32 children
  (`buildIndirect`), sparse-path partial encoding, SingleMask when the disc bits fit an
  8-byte window, MultiMask otherwise; span node ≤ 16 children, multi node ≤ 32
  (`assembleIndirect`, HOTBulkBuilder.java:439).

### 2.1 The v1 packing fix — stop the frontier below the page boundary

**Measured defect:** `buildIndirect` expands the block frontier by always splitting the
*largest* frontier branch until 32 children exist. The expansion does not stop at
branches whose whole key group would fit a leaf page, so page-fitting `R(S)` subtrees
get split across several frontier slots, and their fragments recurse into *further*
32-way expansions. Dense/strided 20 000-key sets build **280 leaves (min fill 16, mean
~71)** where the highest-fitting-subtree cut yields **40 leaves (fill ≈ 512)** — the
shape both the class javadoc ("cut a HOTLeafPage at the highest R(S) subtree whose key
group fits a page") and foundation §3.2 *claim*. Exact arithmetic of the observed
tree: root expands 20 000 keys into 24×512-blocks + 8×1024-blocks; each 1024-block
re-expands into 32 leaves of 32 → 24 + 256 = 280 leaves, 1 + 8 = 9 indirects — the
probe's numbers exactly.

**Fix (IMPLEMENTED in this worktree, uncommitted):** during frontier expansion, a
frontier node is *non-expandable* when its key group fits a leaf page
(`BulkContext.isExpandable`/`fitsLeafPage` — count ≤ `MAX_ENTRIES` and a conservative
byte upper bound `Σ(4 + keyLen + valueLen) ≤ HOTLeafPage.DEFAULT_SIZE`, early-exit,
allocation-free; the bound charges the full key, the page stores only the
post-prefix suffix, so a passing group can never overflow the real page —
`tryBuildLeaf` stays the authoritative check and the recursion the fallback).
Licensed by the model: "any frontier is invariant-correct by Theorem 1"
(HOTBulkBuilder.java:266) — the fix only *chooses a different frontier*, all
invariants and the determinism argument survive verbatim. Height cannot increase:
stopping expansion earlier only removes interior levels.

**Measured after the fix:** dense/strided 20 000-key builds go 280 → **40 leaves**
(fill 32..512, exactly the highest-fitting-subtree count), bulk-vs-bulk determinism
still byte-stable, and the full HOT gate battery is green (§6 G4 list, all run).

**Blast radius:** `HOTBulkBuilder.build` also serves `splitLeafPage`'s halves
(HOTIncrementalInsert.java:154–155 via `buildHalf`), `rebuildSubtree`
(AbstractHOTIndexWriter.java:3499), and the malformed-subtree self-heal — live-tree
shapes shift wherever a half or rebuilt subtree spans multiple pages. All shapes remain
invariant-clean; gates in §6 cover the change. Three test scenarios manufactured their
preconditions FROM the old fragmentation (full 32-child roots out of small random sets;
mergeable BiNode pairs on fresh bulk output — now impossible by construction) and were
repaired to force those shapes by key design / by a real `splitLeafPage` fold:
`HOTIntegrateTest` (grouped keys, pair-overflow constraint `256 < perGroup ≤ 512`),
`StraddleCanonicityProbe` (grouped keys), `HOTIndirectPageSplitFaithfulTest`
(split-then-merge round trip), `HOTBulkBuilderTest` V5 (600-key byte-position groups
keep the MultiMask span at 28 bytes), and `BetaIsDiscBitRoutingProbe` (a depth-0
blind spot: the probe descended a stale root Page variable after a fold replaced the
ROOT — unreachable pre-fix because canonical roots were always full and full nodes
divert to the Q4 decomposition; base-tree baseline confirmed green pre-fix, the
node-local routing printout proved the primitive correct, and the repaired probe now
verifies 13/13 depth-0 folds route 100 % strict — coverage the old shapes never had).

---

## 3. Integration seams (v1 targets, in payoff order)

### Seam 1 — importer-side PATH/CAS/NAME materialization (the refusal lift)

`ParallelBulkJsonImporter` currently refuses secondary-index maintenance. The
EXISTING loaders make the lift mechanical; the per-family entry shape each loader
needs, and where a worker can produce it:

| Family | Loader `add(...)` shape | Worker-side availability |
|---|---|---|
| PATH | `HOTLongBulkIndexLoader.add(long pcr, long nodeKey)` | nodeKey: assigned by the worker; PCR: needs the path-summary node for the record's path — available where the importer maintains the summary; otherwise a coordinator-side replay of collected (pathStep, nodeKey) tuples. OPEN DEPENDENCY: verify at which point the importer's summary is consistent enough to resolve PCRs. |
| CAS | `HOTBulkIndexLoader<CASValue>.add(CASValue key, long nodeKey)` (key = PCR + typed atom) | atom text + node kind known at token time in the worker; type coercion needs the index definition (coordinator-owned, read-only — shareable). Same PCR dependency as PATH. |
| NAME | `HOTBulkIndexLoader<QNm>.add(QNm, long nodeKey)` | trivially known at token time. |

1. Workers collect the tuples per chunk into primitive-friendly buffers.
2. Coordinator (single-threaded, owns the trx) drains them into
   `create*BulkLoader()` — feed order is free, the loader sorts — and calls
   `flush()` once at load end, before the single commit. `spliceBulkBuiltRoot` +
   `registerFreshSubtree` do the adoption; commit serializes.
3. Precondition: the index tree is virgin in this revision. GUARANTEED here — the
   importer refuses non-fresh resources (ParallelBulkJsonImporter.java:219–221), so
   `isEmptyTree()` holds at load end by construction.

No new trie API is needed. What is new: the worker-side accumulation plumbing, the
PCR-availability answer, and the memory budget (§5.2) — pairs for 100M rows are
resident until flush.

### Seam 2 — projection fresh build: batch the slot writes

A fresh projection build (post-pass `ProjectionIndexBuilder` or load-time
`ProjectionBulkLoad`) emits, per row group: one descriptor slot + one slot per column
segment, keys `(rowGroupId << 16) | slotKind` — row-group ids assigned contiguously
from 1 (ProjectionIndexHOTStorage.java:49–56) — then fences (2^42 namespace), Bloom
chunks (2^43), metadata blobs. Row-group slot keys are therefore **append-ordered by
construction** during a fresh build; fences/Bloom/metadata arrive in their own
ascending namespaces afterward. This is exactly the loader's ideal input.

The dominant sub-population is the **order-label lane** (~95 % of fresh-build
insertions, one slot per record at `2^50 + nodeKey`, strictly ascending) — assume the
in-order lane exists (see §0) and target it first: strictly-ascending small-payload
slots are the loader family's ideal input.

**Loader-contract gap.** `AbstractHOTBulkIndexLoader` is postings-shaped: it carries a
`long nodeKey` per entry and its `toEntry` FOLDS a key's node keys into one chunked
`NodeReferences` run (AbstractHOTBulkIndexLoader.java:303–320). Projection slots are
REPLACE-semantics opaque `byte[]` payloads. So the projection cannot consume the
concrete class as-is; the v1 seam is a **sibling in the same family** (share the
block-arena accumulation, the int-permutation sort, the exact-size entry list, and the
`spliceBulkBuiltRoot` splice; swap the per-entry parallel array from `nodeKeys[]` to a
packed payload position/length, payload bytes in the same block arena):
`HOTBulkSlotLoader` — engaged by `ProjectionIndexHOTStorage` when the tree is empty
(fresh build), routing `writeSlotValue`/`putBlob` into accumulation instead of N
descents, spliced once at build end. Constraints that shape it:

- **Replace-not-OR-merge slot semantics** (branch-escape guard javadoc,
  AbstractHOTIndexWriter.java:1746–1759): the accumulator must key-dedupe with
  last-writer-wins, not fold postings — its own `toEntry` (keep the last payload).
- **Read-your-writes during the build**: `putRowGroupAsColumnSegmentSlots` reads the
  prior descriptor (`getBlobIfReadable`, ProjectionIndexHOTStorage.java:617 — null on
  a fresh build) and the fence/Bloom rewrite paths read earlier chunks. While
  accumulating, reads must be served from the accumulator (a key-indexed view) or the
  sink is restricted to sites verified write-only for the fresh build. This is the
  main v1 engineering risk on Seam 2.
- **Side maps / OverflowPages** (`putSegmentPage`): segment references hang off the
  leaf that *physically holds the owning slot*. Attach **after** the build via the
  existing `reattachSegmentRefs` discipline (used by every rebuild path), which
  resolves owner slots against the finished leaves.
- **Tombstones cannot occur in a fresh build** (nothing to delete) — in-scope for v1.
- **Auto-commit windows:** the non-parallel bulk load commits every
  `-Dsirix.autoCommit.nodes` nodes; after the first commit the tree is no longer
  virgin and v1 must fall back to the per-entry path for subsequent windows (v2:
  subtree-level merge — out of scope). The parallel importer's single-commit shape
  avoids this entirely, so Seam 2 lands cleanly there first.

### Seam 2a — measured perf gate + the streaming-vs-splice-once decision (phase 2)

**Perf gate (ProjectionBulkVsPerEntryBench, production paths both arms, interleaved
reps, best-of; witnesses untimed: sampled read-back equality + detector-clean):**

| shape | entries | perEntry ns/entry | bulk ns/entry | ratio |
|---|---|---|---|---|
| label (2^50+k, 30 B) | 1 M | 1 506.6 | 159.6 | **9.44×** |
| label | 10 M | 1 986.1 | 159.4 | **12.46×** |
| segslot ((rg<<16)\|slot) | 1 M | 1 463.9 | 129.5 | **11.30×** |
| segslot | 10 M | 2 199.0 | 156.5 | **14.05×** |

Per-entry cost GROWS with scale (deeper descents, more split cascades); bulk stays
flat ≈ 130–160 ns/entry. Gate threshold (≥3×) passed with 3–4.7× margin.

**The decision.** `spliceBulkBuiltRoot` refuses non-empty trees while the build writes
continuously. Two clarifications change the option space:

- **Flush-epoch rotation is NOT the obstacle**: epochs never touch HOT pages (§0.3 —
  pinned region); the tree becomes non-empty through the build's own writes and real
  auto-COMMITS. The rider stale-marks the index for the whole load
  (`ProjectionBulkLoad` metadata `staleTombstone`, :302), so intermediate revisions
  never serve from the half-built projection — accumulation is visibility-safe; the
  binding constraint is memory.
- **A leaf owns a 64 KiB slot segment regardless of fill** (`HOTLeafPage` ctor →
  `allocator.allocate(DEFAULT_SIZE)`, HOTLeafPage.java:458), and during
  `HOTBulkBuilder.build` none of the pages are TIL-registered, so the pinned-trie
  spill cannot relieve them — the BUILT PAGES are unconditionally resident until the
  splice, on top of the loader arena.

Arithmetic (label lane, 8 B keys + 30 B payloads; slot-loader arena ≈ 70 B/entry
(key+payload block bytes + 24 B index); built tree ≈ 64 KiB / 512 entries =
128 B/entry):

| entries per splice | arena | built pages | total transient | vs ≤2 G rule |
|---|---|---|---|---|
| 1 M | 70 MB | 128 MB | ~0.2 GB | fine |
| 8 M | 560 MB | 1.0 GB | ~1.6 GB | fine — the chosen cap |
| 30 M | 2.1 GB | 3.8 GB | ~5.9 GB | FAIL |
| 100 M | 7.0 GB | 12.5 GB | ~19.5 GB | FAIL outright |

- **(a) whole-build splice-once — REJECTED at scale** (fails from ~15 M entries).
- **(b) bulk only the first window — REJECTED as primary** (rider win ≈ A/total ≈ 1 %
  at 100 M; post-pass degenerates to (a)).
- **CHOSEN v1 — (a-bounded): accumulate-with-cap + graceful drain.**
  `HOTBulkSlotLoader` accumulates while the tree is virgin, capped at 8 M entries /
  512 MB arena bytes. Build ends under the cap → sort + last-writer-wins fold +
  splice-once (full 9–14× win — covers every fresh build ≤ 8 M slots). Cap trip, any
  read of projection state, a side-ref attach, or transaction rollover → **drain**:
  replay the folded entries ascending through the per-entry path and disable
  accumulation. Never worse than today; bounded memory by construction; the drain is
  the production write path in its best-case key order.
- **(c/d) v2 design (not implemented): right-edge append splice.** Both lanes ascend
  strictly across the whole build, so general subtree-merge is never needed:
  chunk-wise, pop the rightmost leaf (≤512 entries), bulk-build R(leaf ∪ chunk),
  attach on the right spine at the I11-chosen depth (first spine node whose block
  bits are all more significant than `msdb(oldMax, newMin)`), path-copy, propagate
  height/partials via the existing Stage-3c machinery. Bounded at chunk size, full
  bulk win at any scale; gate is semantic + detector (per §1, the appended shape need
  not equal `bulkBuild(S_union)`).

### Seam 2b — v1 implementation (this worktree, uncommitted)

- **`io.sirix.index.hot.HOTBulkSlotLoader`** — the slot-store sibling of the loader
  family: long-key + opaque-payload block-arena accumulation, `Long2IntOpenHashMap`
  membership/last-ordinal, permutation sort, last-writer-wins fold, splice through the
  production `spliceBulkBuiltRoot`. Standalone (the abstract loader is `sealed` and
  postings-shaped).
- **`ProjectionIndexHOTStorage`** integration, four hooks:
  - `beginBulkSlotAccumulation()` — engages only on a VIRGIN tree (the positive
    witness the read-through contract rests on);
  - `writeSlotValue` head — accumulate; a capacity refusal splices the prefix (legal:
    the tree is still virgin) and falls through per-entry;
  - `readSlotValueForWrite` head — **read-through**: an accumulated payload is
    authoritative (zero-length = tombstoned), a miss falls to the (empty) tree; this
    is what keeps the order-label walk (reads of earlier-minted labels) and the
    row-group prior-descriptor probes (reads of absent keys) from forfeiting the win;
  - `putSegmentPage` / `getSegmentPageBytes` / `removeSegmentPage` — side-page
    attaches against accumulated owner slots are **DEFERRED** (payload retained, the
    exact `new OverflowPage(bytes)` ownership contract; 512 MB budget; last-writer-wins
    by refKey; read-through mid-build) and run through the production `putSegmentPage`
    right after the splice. Without deferral the first big column segment would have
    tripped the splice on row group 1 and forfeited the label lane.
  - `resetTree` discards accumulator + pendings; `finalizeBulkSlotAccumulation()`
    splices + attaches, safe in `finally` (persists exactly the per-entry prefix
    semantics on failure).
- **Wired call site:** `ProjectionIndexBuilder.buildAndPersist` (the post-pass fresh
  build) — `begin` right after `resetTree`, `finalize` in a wrapping `finally`. The
  load-time rider (`ProjectionBulkLoad`) is intentionally NOT wired in v1: its
  auto-commit windows need a pre-commit finalize hook or the accumulated window is
  lost at commit — v2 work, documented, not hand-waved.
- **Witness:** `ProjectionBulkSlotAccumulationTest` — the same operation sequence
  (50 000 label writes with interleaved read-backs, a re-put, a tombstone, an inline
  blob, a REFERENCED 200 KB blob) through both arms; asserts read-identity
  in-transaction (pre- AND post-splice), after a REAL commit (trie-navigated reads +
  side-page bytes), zero malformed subtrees both arms, and the construction-route
  witness (`bulkSplicedEntryCount` 50 002 vs 0).
- **Perf gate:** `ProjectionBulkVsPerEntryBench` (see the table above).

### Seam 2c — consolidation into the epoch-riding post-pass (bulkproj)

The consolidated `buildAndPersist` rides the writer's async-flush epochs
(`BulkBuildEpoch` + mid-build `asyncFlush()` at log boundaries — the bounded-retention
fix) and mints labels through the in-order lane. Two additions make the accumulator
compose with that shape:

- **Epoch transplant** — `BulkBuildEpoch.rebind` constructs a fresh storage per epoch;
  `ProjectionIndexHOTStorage.adoptBulkSlotAccumulation` moves the loader, the deferred
  side attaches (insertion order preserved into the empty fresh map), the byte
  accounting and the witness counter onto it BEFORE the root re-seed, so the re-seed's
  read sees the accumulated root label through the new storage's read-through. The
  accumulator holds only un-materialized writes for the same still-virgin tree, so it
  is tree-state-independent and moves wholesale. (In practice accumulation keeps the
  log so small that boundaries rarely trip — measured live entries ≈ 9 mid-build vs
  the 64-entry epoch bound — so the transplant is the safety net for corpora whose
  DOCUMENT-side entries still trip rotations.)
- **Post-splice drain** — the finalize-splice registers the whole tree in one burst
  AFTER the last in-loop boundary check, which the bounded-retention witness
  (`ProjectionPostPassBoundedRetentionTest`) correctly caught: mid-build retention
  IMPROVED (≈9 live entries vs the 64 bound) but the end-of-build measurement saw the
  un-rotated burst (161/152 live). The success path therefore finalizes explicitly
  (idempotent; the outer `finally` stays the exception backstop) and drains through
  the SAME rotation mechanism the in-loop checks use — a progress-guarded
  `asyncFlush()` loop; the pinned-trie spill empties it bottom-up (complete-dump
  leaves first, interiors once their children are durable). Witness green again
  (≤ 64 live at return, both scenarios).

### Seam 3 — order-directory / locator / dictionary

Same pattern as Seam 2 (dense or strided ascending keys during a fresh build; the
inventory table in the phase report lists each site's exact key formula and loop).
These ride the same sink; nothing structurally new.

---

## 4. TIL adoption — reuse `registerFreshSubtree`, verbatim

`spliceBulkBuiltRoot` (empty-tree check → `HOTBulkBuilder.build` → root
`setPage` → `registerFreshSubtree`) is the v1 adoption for whole trees, and
`registerFreshSubtree` alone for subtree splices. Its invariants, which any caller
must preserve (all hold for bulk output by construction):

- post-order registration (children before parents — `log.put` nulls the in-memory
  page on the reference);
- stop at shared subtrees (`ref.getLogKey() >= 0 || ref.getKey() >= 0`) — moot for a
  fresh build, load-bearing for future merge work;
- every fresh leaf `setCompleteDump(true)` → full first fragment at commit, no
  fragment chain for readers;
- failure paths retire the unregistered fresh suffix (`closeUnregisteredFreshChildren`,
  `recoverFromRegistrationFailure`) — off-heap leaf segments never leak.

---

## 5. Epoch interplay and memory

### 5.1 Spill eligibility — dense bulk pages are eligible by construction

`isPinnedTrieSpillPageEligible` (NodeStorageEngineWriter.java:2092):

- `HOTLeafPage`: eligible iff `!wouldEmitSparseFragment()` — a fresh complete-dump
  leaf with no committed predecessor never emits sparse, so **bulk-built leaves are
  eligible immediately** — and `allSideReferencesDurableAndUnclaimed()` (projection
  side refs must be durable first; ordinary index leaves have none);
- `HOTIndirectPage`: eligible iff every child reference is already durable — so a
  bulk subtree spills strictly bottom-up, leaves → interiors, exactly the order the
  spill's round-robin cursor discovers them.

This closes the brief's question: the sparse-fragment exclusion
(`sparse-fragment-spilled-standalone-loses-clean-entries`) cannot bite a fresh bulk
subtree; only CoW'd leaves with committed ancestry emit sparse.

### 5.2 Memory budget

A bulk build holds, simultaneously: the loader arena (`n × (keyBytes + 20)` — the
class javadoc's figure), the `R(S)` recursion (transient), and the finished pages
(pinned until commit or spill). For Seam 1 at 100M rows this is the dominant new cost
and must be measured before enabling by default; the pinned-trie spill bounds the
page half, the arena is freed at `flush()` (`releaseBuffers`). Mitigation if the
arena binds: per-family flush at epoch boundaries into *separate index numbers* is NOT
possible (one tree per family), so v1 either sizes for residency or defers the family
to the post-pass builder. Honest open point, not hand-waved.

---

## 6. Gate plan (per the canonicity finding)

- **G1 — bulk determinism (byte gate).** Build the same sorted entry list twice with
  allocators started at the same value; assert identical structure AND identical
  serialized page bytes. Valid because build order, page-key assignment, and layout
  choice are all deterministic (§1). Probe already covers the structural half.
- **G2 — semantic equivalence vs the per-entry path (the replacement gate).** For each
  converted call site: build once per-entry, once bulk, same inputs; assert identical
  point reads (`get`), identical range-cursor sequences, and — for the projection —
  the census differential: `readAllRowGroupsFromColumnSegmentSlots` validates the
  exact ordered descriptor set and every segment hash, so a lost/duplicated/misplaced
  slot fails loudly. Byte-identity is **not** asserted against the incremental tree
  (proven order-dependent, §1).
- **G3 — invariant oracle.** `HOTMalformedSubtreeDetector` reports zero malformed
  subtrees on every bulk output (already the production self-heal oracle; also the
  existing `HOTFormalModelTest` V1–V4 batteries stay green).
- **G4 — packing gate (for §2.1) — RUN, all green.** Dense/strided 20 000-key builds
  produce the highest-fitting-subtree leaf count (measured 40, was 280). Battery run
  one class per JVM: `HOTBulkBuilderTest` 5/5 (incl. 1575 adversarial sets, 0
  violations), `HOTFormalModelTest` 3/3, `StraddleCanonicityProbe` 3/3,
  `HOTIntegrateTest` 4/4, `HOTMalformedSubtreeDetectorTest` 11/11,
  `HOTLeafPageSplitFaithfulTest` 3/3, `HOTIndirectPageSplitFaithfulTest` 11/11,
  `HOTDescentAnalysisTest` 4/4, `HOTIncrementalSplitSegmentRefTest` 2/2,
  `HOTInsertionOrderShapeProbe` 1/1, and end-to-end `JsonCASIndexBuildTest` 6/6
  (bulk-loader postings == change-listener postings at DB level). Every failure on
  the way was a coverage-generator shape dependence, reviewed and repaired — never a
  silenced assertion (§2.1 blast-radius list).
- **Perf gate (campaign-level).** Per-family build time at 1M/10M rows, per-entry vs
  bulk; TIL pinned-entry count; peak RSS. No mechanism ships on benchmark-only
  evidence (`benchmark-mechanisms-must-be-generally-applicable`): the loaders/builder
  are general-purpose index machinery, the projection sink must be, too.

---

## 7. Risks / open questions

1. **Packing fix changes live shapes** through `splitLeafPage`/`rebuildSubtree` —
   any hidden exact-shape dependence (tests or readers) surfaces in G4. Readers route
   by invariants, so this is expected-quiet, but unverified until run.
2. **Loader arena at 100M** (§5.2) — measure before default-on.
3. **Auto-commit windows** break the empty-tree precondition for Seam 2's
   non-parallel path; v1 falls back per-entry after the first window. v2 needs
   subtree merge (append-region splice), deliberately out of scope.
4. **Stale incremental heights** (probe finding, §1) predate this campaign but
   interact with any future "bulk-build the tail, integrate incrementally" hybrid:
   integrate's intermediate-node decision consumes heights. Fix separately.
5. **Projection side-ref reattach at bulk scale** — `reattachSegmentRefs` is
   O(refs × leaf lookup); fine per-rebuild today, needs a sorted merge walk if a
   whole 100M projection tree is bulk-built with populated side maps.

---

## Appendix A — per-entry HOT write call sites (fresh-build inventory)

All sites below funnel into `ProjectionIndexHOTStorage.writeSlotValue` /
`putBlob` (each = one `prepareLeafOfTree` descent + branch-escape dispatch +
`leaf.put` + update-or-split cascade) unless noted. "Append-ordered" = keys
non-decreasing across the calls of one fresh build.

| Site | Key formula | Order in a fresh build | Bulk candidate |
|---|---|---|---|
| Row-group descriptor slot — `putRowGroupAsColumnSegmentSlots` (ProjectionIndexHOTStorage.java:596→640), called from `ProjectionIndexBuilder` + `ProjectionBulkLoad` (ProjectionBulkLoad.java:80) + `ProjectionIndexChangeListener.writeRowGroup`:3090 | `rowGroupId << 16` (slotKind 0), sign-flipped 8-byte BE | rowGroupId contiguous from 1 in emit order → append-ordered | YES |
| Column-segment slots — `writeChangedColumnSegmentSlots` per segment | `(rowGroupId << 16) \| (columnSegmentId + 1)` | ascending within a group; groups ascend → append-ordered | YES |
| Segment overflow pages — `putSegmentPage` (ProjectionIndexHOTStorage.java:3452) | side-map key `(ownerSlotKey << 16) \| columnSegmentId`, attached to the OWNING LEAF, not the trie | follows the owning slot | attach-after-build via `reattachSegmentRefs` |
| Order-directory slots — `ProjectionStructuralOrderDirectory.HotSlotStore.put` → `putStructuralOrderSlot` (ProjectionStructuralOrderDirectory.java:142) | `2^50 + nodeKey`, ONE SLOT PER RECORD | strictly ascending in a fresh build (assume the in-order label lane, §0) | **YES — ~95 % of all per-entry insertions; the primary target** |
| Record locator — `ProjectionRecordLocator.put`:95 → `putRawSlot(slotKey(recordKey))` | `slotKey(recordKey)` | update/move path (rebalance); low volume in a fresh build (not fully traced) | LOW PRIORITY |
| Fences — `ProjectionIndexFences` (`putBlob` at :350/:355/:362/:904/:917) | `CHUNK_SLOT_BASE + chunkId` (2^42 namespace), `ORDER_HEADER_SLOT` | chunkId ascending; header last → append-ordered | YES |
| Bloom chunks — `ProjectionBloomChunks` (`putBlob` at :136/:684/:706/:853/:938) | `bloomBlockSlotKey(column)` + chunk slots (2^43 namespace) | per-column ascending chunks | YES |
| Set-summary chunks — `ProjectionSetSummaryChunks` :54/:164/:388 | `slotKey(column)` | per-column | YES |
| Metadata blob — `ProjectionIndexBuilder`:898, `ProjectionBulkLoad`:302, listener :2313/:3139 | slot 0 | once at end | trivial |
| PATH/CAS/NAME entries — `AbstractHOTIndexWriter.doIndex` via builders | serializer-specific composite keys | traversal order (not sorted) | ALREADY BULK for virgin trees (`create*BulkLoader`); importer-side lift = Seam 1 |

Answers to the three campaign questions:

- `ProjectionIndexHOTStorage` uses **no** bulk loader today (zero references to
  `createBulkLoader`/`spliceBulkBuiltRoot` in `io.sirix.index.projection`).
- The parallel importer's refusal: `ParallelBulkJsonImporter.refuseUnsupportedShape`
  (ParallelBulkJsonImporter.java:215–218) throws on `bulkHasPrimitiveIndexes()`; it
  also requires a FRESH resource (:219–221), so at load end every index tree is
  virgin — the bulk loaders' empty-tree precondition holds by construction.
- `ProjectionBulkLoad` keeps the real `ProjectionIndexBuilder` machinery alive across
  auto-commit windows and streams full leaves through
  `putRowGroupAsColumnSegmentSlots` — i.e., it batches BUILD state, not WRITES; every
  persisted slot still pays the per-entry descent.
