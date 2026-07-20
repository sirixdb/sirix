# Projection Index Storage Redesign — "Unbundle the Leaf Value"

**Status:** design proposal / analysis
**Scope:** the on-HOT storage layout of the columnar projection index, its row-group
sizing, per-column layout, update/delta granularity, and directory structure.
**Non-goals:** the vectorized scan kernels (already SuperWord-vectorized and
allocation-free), the JSONiq surface, the catalog/discovery model, and the
versioning *machinery* itself (HOT already rides `VersioningType` — see §2.4).

---

## 0. TL;DR

The projection index bundles an entire 1024-row row group — **all columns, fully
bit-packed — into a single ~2.5 KB HOT slot value**. That one decision drives every
scaling and update-amplification limitation the index has:

- the storage row group is pinned to the **SIMD batch size** (1024), ~120× smaller
  than a real columnar store's storage group;
- **all columns share one value**, so there is no column pruning on read and no
  column-local update;
- an update **re-encodes the whole touched leaf** (no sub-leaf delta);
- big inline values cap HOT fan-out at **~26 entries/page** instead of 512,
  producing the ~100 M-row ceiling.

This document proposes four independent, composable changes — **(A)** decouple the
storage row group from the SIMD batch, **(B)** per-column column chunks, **(C)**
finer delta patches on update, **(D)** a directory / K-leaves-per-page structure —
grounded in how DuckDB and ClickHouse solve the same problems (§5). None of them
touch the temporal versioning guarantees, which are the index's reason to exist.

---

## 1. Background: what the projection index is

A projection index is a **columnar secondary index over a versioned JSON/XML document
tree**. It extracts declared fields of every record under a root path into
column-oriented pages that the vectorized executor scans instead of walking the
document tree. Unlike a standalone OLAP store, it is:

- **fully bitemporal** — queryable AS OF any past revision, reconstructed by
  copy-on-write page fragments (`SLIDING_SNAPSHOT` by default);
- **incrementally maintained** inside each commit, via the index-controller listener
  lifecycle;
- **a secondary index**, not the primary store — the document tree remains the source
  of truth.

That combination is not something DuckDB or ClickHouse target (§5.4), and it is the
constraint that rules out simply copying their storage engines wholesale.

---

## 2. Current design (measured)

### 2.1 The two "leaf" concepts

| Concept | Class | Unit | Size |
|---|---|---|---|
| Columnar row group | `ProjectionIndexLeafPage` | 1024 records, column-major | ~20 KB raw / **~2.5 KB compact** |
| HOT storage page | `HOTLeafPage` | up to 512 key→value entries | 64 KB off-heap |

A `ProjectionIndexLeafPage` holds **`MAX_ROWS = 1024`** records, stored **column-major**
(struct-of-arrays): all 1024 values of column 0 contiguous, then column 1, etc. — with
frame-of-reference numerics, per-leaf string dictionaries, and presence bitmaps. This
is the SIMD scan unit.

### 2.2 The encode → chunk → store pipeline

```
ProjectionIndexLeafPage (1024 rows)
   serialize()                     → ~20 KB raw column-major bytes
   ProjectionIndexLeafCodec.encode → ~2.5 KB compact (FOR + dict + bitpack)
   split into CHUNK_SIZE=4096 chunks → usually 1 chunk (2.5 KB < 4 KB)
   store as HOT entries:  key = PathKeySerializer((leafIndex<<8) | chunkIdx)
                          value = the chunk bytes (inline)
```

- Composite key is sign-flipped 8-byte big-endian so unsigned byte comparison
  preserves `(leafIndex, chunkIdx)` order (chunks contiguous, leaves ascending).
- `chunkIdx` occupies the low 8 bits → **`MAX_CHUNKS_PER_LEAF = 256`** → 1 MB compact
  ceiling per leaf.
- **Because a compact leaf (~2.5 KB) is under `CHUNK_SIZE` (4 KB), the typical leaf is a
  single chunk — one HOT entry ≈ one whole row group.**
- Slot 0 holds `ProjectionIndexMetadata` (root path, field paths, column names, column
  kinds, leaf count, build revision, per-leaf `(firstRecordKey,lastRecordKey)` fences,
  `FLAG_STALE`).

Measured compact ratio (bench leaf, 3 columns, 1024 rows):
`raw 21,286 B → compact 2,489 B ≈ 11.7% (8.6×)`, i.e. ~2.4 B/row.

### 2.3 Capacity math — the ceiling

A `HOTLeafPage` is 64 KB (`DEFAULT_SIZE`) and caps at 512 entries (`MAX_ENTRIES`), but
**value size hits first**:

```
entry ≈ 2 (suffixLen) + ~7 (suffix) + 2 (valueLen) + ~2489 (value) ≈ 2.5 KB
64 KB / 2.5 KB ≈ 26 entries/page   ← not 512
```

So one HOT page holds **~26 row groups (~27 k records)**. Contrast: the RBTree indexes
(PATH/CAS/NAME) store one small `RBNode` per entry and fill toward the 512 cap as
intended. The projection is the outlier — value-size-bound, not entry-count-bound —
and that is the ~100 M-row scale ceiling.

### 2.4 Versioning is already unified (not a problem to fix)

An earlier misreading suggested the chunk-sharing was a bespoke workaround. It is not.
HOT reads route through `NodeStorageEngineReader.loadHOTPage →
loadHOTLeafPageWithVersioning → VersioningType.combineHOTLeafPages`, which dispatches
**all four strategies** (`FULL` direct; `DIFFERENTIAL`/`INCREMENTAL`/`SLIDING_SNAPSHOT`
via `mergeHOTFragmentsByKey` with tombstone shadowing). `HOTLeafPage` carries a
slot-granular `dirtyBitmap` (mirrors `KeyValueLeafPage.preservationBitmap`),
`completePageRef`, `completeDump`. So the projection already gets real page-fragment
sliding-snapshot versioning at slot granularity, identical to the document tree.

**Caveat (worth a separate audit):** HOT collapses the three non-FULL strategies into a
single uniform by-key merge, relying on the loader to bound fragments per strategy,
whereas `KeyValueLeafPage` uses per-slot in-window preservation bitmaps. The outcomes
should match; the mechanism is thinner. Not addressed here.

### 2.5 Update path today

`ProjectionIndexChangeListener.applyIncremental` locates the touched leaves via the
slot-0 fences, then for each: re-reads **all 1024 rows** from the transaction's current
state, rebuilds the leaf, and `writeLeaf → storage.put(slot, encode(serialize()))`
**rewrites the whole leaf** (all its chunks — `updateValueRange` always marks the slot
dirty; there is no content comparison). So:

- **Only the touched leaves are rewritten** (great — not the whole index), located in
  one slot-0 read.
- But the granularity is the **whole 1024-row leaf** (~2.5 KB), even for a one-field,
  one-row change. There is no column-local or row-local delta.

---

## 3. Problem statement

Four coupled limitations, all traceable to "one bit-packed all-columns row group = one
HOT slot value":

1. **Storage group == SIMD batch (1024).** The scan-batch size is masquerading as a
   storage row-group size. A storage row group of 1024 is tiny: poor compression
   amortization, high per-group metadata, and many HOT entries.
2. **No column pruning.** `sum(age)` still reads `active` and `dept` bytes because all
   columns live in one value.
3. **Whole-leaf update amplification.** One row's field change re-encodes ~2.5 KB.
4. **Fan-out ceiling (~26 entries/page → ~100 M rows).** Big inline values starve the
   HOT trie's fan-out.

---

## 4. Proposals

The four are independent and composable. Each states the change, the win, the cost, and
the interaction with the CoW/versioning machinery.

### (A) Decouple the storage row group from the 1024 SIMD batch

**Change.** Keep 1024 as the *vectorized scan batch* (the kernel still processes 1024
values per pass), but make the *storage row group* a multiple `G × 1024` (e.g. G = 16 →
16,384 rows; G = 120 → DuckDB's 122,880). A stored leaf becomes G column-segments per
column, still scanned 1024 at a time.

**Win.**
- Better compression: FOR bases / dictionaries amortize over 16 k–120 k rows, not 1 k.
- Fewer HOT entries and less slot-0 metadata (fences per group, not per 1024).
- Directly relieves the fan-out ceiling (fewer, though larger, entries) even before (D).

**Cost.**
- A stored group no longer fits one 4 KB chunk → multi-chunk (or a page of its own,
  see (D)). Read reassembly spans more chunks.
- Update amplification *rises* per touched group unless paired with (B)/(C): touching
  one row now re-encodes a 16 k-row group, not a 1 k-row one. **(A) must ship with (C)
  or block-local encoding**, or it regresses updates.

**Precedent.** DuckDB explicitly separates the 2,048-value execution vector from the
122,880-row storage row group for exactly this reason (§5.1).

### (B) Per-column column chunks (PAX / Parquet column-chunk layout)

**Change.** Store each column of a row group as its **own HOT entry**, keyed
`(leafIndex, columnIdx)` (plus a `(leafIndex, header)` entry for the shared record keys,
presence bitmaps, and fences). This is the Parquet row-group/column-chunk decomposition.

**Win.**
- **Column pruning on read** — the always-on benefit. A scan reads only the column
  entries it touches; wide projections with narrow queries stop paying for unread
  columns. This *is* the columnar access pattern the index exists to exploit.
- **Column-local updates** — an in-place value edit re-encodes only the changed column
  (~1/Ncol of the leaf), not all columns.
- Per-column codec choice becomes natural (§C, §5.3).

**Cost.**
- **Entry multiplication:** `Ncol × groups` entries (mitigated by smaller per-entry
  size; each column chunk is a fraction of the leaf).
- A shared **header entry** per group for record keys / presence / fences.
- **Reassembly** for all-column reads: Ncol lookups per group instead of 1.
- Column-local updates help **in-place value edits only** — inserts/deletes change the
  group's row count, so every column re-packs regardless.

**Precedent.** Both DuckDB (column segments) and ClickHouse (`.bin` per column) store
columns independently. This is table stakes, not novelty.

### (C) Finer delta patches on update (ClickHouse "patch parts" shape)

**Change.** Instead of re-encoding a whole leaf/column on update, write a small
**delta fragment** describing only the changed rows, applied on read by the existing
fragment-merge (`combineHOTLeafPages`). Attribute the change to its `(group, column)`
at **listen time** via the mutated node's path (the listener already resolves the
record → leaf; extend to resolve the path → column), so no byte-diffing is needed.

**Win.**
- Update cost ∝ number of changed rows, not leaf size. This is the single biggest
  update-amplification lever, and it is **structurally what ClickHouse patch parts do**
  (§5.2) and what Sirix's differential/sliding-snapshot fragments are already shaped
  for.
- Composes with (A): a big storage group no longer means big update cost.

**Cost / hard parts.**
- The compact codec is **not** update-local today: `encode` recomputes each column's FOR
  base and bit-width from all current values, and `writeLeaf` regenerates the payload
  from scratch — so a single value edit can re-pack a whole column (base/width/dictionary
  shift), meaning a "delta" can cascade. **(C) needs a block-local, byte-aligned encoding**
  (fixed row-blocks each with their own FOR base/width, byte-aligned) so a row edit
  touches only its block. This is the real codec work.
- Read now merges base + patch fragments (already the versioning model, so incremental).

### (D) Directory / K-leaves-per-page

**Change.** Move the big bytes out of HOT slot values. A HOT slot holds a tiny
**directory** — `(chunkIdx/columnIdx → PageReference)` — and the actual column/leaf
bytes live in their own CoW-versioned pages. Tunable **K = leaves (or column chunks)
per data page**: K = 1 is pure pointer; K = 26 ≈ today.

**Win.**
- HOT becomes **entry-count-bound (~512/page)** again → scale past 100 M. Directory
  collapses ~26× (e.g. 100 M rows: ~191 directory pages vs ~3,756 inline pages).
- **Selective reads:** a pruned leaf's page is never loaded (with inline packing, needing
  one leaf still pulls the whole 64 KB page).
- Clean per-leaf/per-column CoW at standard page granularity.

**Cost (the real one).**
- **Recombination reads more pages.** With pure pointers, hydrating a leaf = merge the
  directory chain (~R fragments) + merge that leaf page's own chain (~R) ≈ R reads/leaf,
  vs today's R/26 reads/leaf (one page's chain yields 26 leaves). Full cold hydrate does
  **~26× more page I/O ops for the same bytes**, plus per-page fixed overhead × more
  pages.
- Two-level versioning: the directory page is CoW'd whenever any child ref swings; you
  must **reuse unchanged `PageReference`s** to preserve sharing.

**The K dial balances it.** K = 8 (pack 8 leaves/column-chunks per data page) keeps most
scan locality (~K× fewer recombination reads than pure pointers) while still lifting the
ceiling and enabling K-granularity pruning. K is the exact inline-vs-pointer knob.

**Mitigation for the read cost.** Hydrate is **decoded once per revision into a bounded
cache** and already has a **parallel path** (`readAllParallel`/`getMany`), so the
extra I/O ops are amortized and parallelizable, not paid per query. The regression is
worst for cold, unpruned, full-table scans — the least common analytical shape.

---

## 5. Prior art — DuckDB and ClickHouse

### 5.1 DuckDB (MVCC columnar, single file)

- **Row group = 122,880 rows**, stored as **column segments** (one per column), each
  independently compressed. **Execution vector = 2,048 values.** The storage group and
  the execution batch are deliberately different sizes — directly validating (A) and (B).
- **Lightweight compression** chosen per segment: FOR, RLE, bit-packing, dictionary,
  **FSST** (strings), **ALP/Patas** (floats). Inspectable via `PRAGMA storage_info`.
- **Updates:** in-place ACID on fixed-size blocks via MVCC version chains, merged at
  checkpoint. Blocks are reused to avoid fragmentation. (Sirix can't adopt in-place
  mutation without giving up immutable time-travel — see 5.4.)

### 5.2 ClickHouse (MergeTree, LSM-like)

- **Granule = 8,192 rows** (`index_granularity`); a sparse primary index stores one
  entry per granule. Each column is a separate `.bin` file. Data lives in **immutable
  parts**; background **merges** combine them.
- **Codecs:** LZ4/ZSTD plus specialized `Delta`, `DoubleDelta`, `Gorilla`, `T64` — strong
  for time-series and floats.
- **Updates:** classic mutations rewrite whole parts (heavy). **Patch parts** (v25.7,
  2025) model an UPDATE as a tiny delta written like a lightweight insert and applied
  during merge — up to ~1,700–2,400× faster than mutations, designed for ≤~10% of rows.
  **This is conceptually Sirix's differential/sliding-snapshot fragment**, and it is
  exactly the shape proposal (C) generalizes to per-row/column deltas.

### 5.3 Scorecard

| Dimension | DuckDB | ClickHouse | Sirix projection (today) |
|---|---|---|---|
| Storage row group | 122,880 | 8,192/granule | **1,024** |
| Exec/SIMD batch | 2,048 | 8,192 block | 1,024 |
| Column layout | column segments | `.bin` per column | **all columns in one value** |
| Compression | FOR/RLE/dict/FSST/ALP | Delta/DoubleDelta/Gorilla/T64/ZSTD | FOR/dict/bitpack/presence; **floats rejected** |
| Update granularity | MVCC in-place block | **patch parts** (row-level delta) | **whole 1024-row leaf** |
| Scale | billions | billions | **~100 M** |

### 5.4 What neither does — Sirix's actual edge

Neither persists **all historical versions for arbitrary time-travel**: ClickHouse
merges old parts away; DuckDB GCs MVCC versions. Sirix keeps **every revision
queryable**, with the projection as an **incrementally-maintained secondary index over a
mutable versioned document tree**. That is a different problem — bitemporal analytical
indexing — and it is why the CoW-fragment machinery is load-bearing and why in-place
mutation (DuckDB) is off the table. The lesson is **not** "become DuckDB"; it is "borrow
the storage-layout ideas that are compatible with CoW fragments" — which is precisely
(A)–(D).

---

## 6. Recommended sequencing

Ordered by value/effort, each independently shippable and reversible:

1. **(A) Bigger storage groups, decoupled from the SIMD batch** — highest leverage for
   compression + fan-out, smallest conceptual change (a sizing constant + multi-chunk
   read). **Must ship with (C) or block-local encoding** so update cost doesn't regress.
2. **(B) Per-column column chunks** — unlocks column pruning (the columnar payoff) and
   sets up per-column codecs; moderate change (keying + a header entry + reassembly).
3. **(C) Block-local encoding + delta patches** — the real codec work, but the biggest
   update-amplification win and the enabler for large groups.
4. **(D) Directory / K-per-page** — take on only when the fan-out ceiling actually binds
   (>100 M), tuning K to trade recombination reads against scan locality.

A pragmatic first milestone is **(A)+(B) with a modest G and K** — bigger per-column
column chunks in moderately-packed data pages — which captures compression, pruning, and
most of the scale headroom before committing to the codec surgery in (C) or the pure
directory in (D).

---

## 7. Risks & open questions

- **Codec locality (C).** Block-local FOR is the crux; without it, "delta" updates
  cascade through a re-packed column. Prototype the block encoding first and measure
  re-pack containment on representative edits.
- **Header-entry consistency (B).** Record keys / presence / fences are per-group; a
  torn write between the header entry and column entries must not yield an inconsistent
  leaf. The CoW/TIL transaction boundary should cover this, but it needs an invariant.
- **Recombination cost (D).** Benchmark cold full-hydrate page-I/O-op count at several K
  before committing; confirm the parallel hydrate path scales as assumed.
- **Versioning-parity audit (2.4).** Independently verify HOT's uniform by-key merge
  matches `KeyValueLeafPage`'s per-slot in-window semantics across all four strategies.
- **Float support.** Adopting ALP/Gorilla-style float columns is orthogonal but removes a
  real functional gap (floats are currently rejected outright).
- **Migration.** Any layout change needs a format-version bump and a read path for the
  old inline layout (or an offline rebuild), since old revisions must remain readable.

---

## 8. Explicitly out of scope

- The scan kernels (done: two-pass SuperWord-vectorized, allocation-free, ~1.2 ns/row).
- The versioning *machinery* (HOT already rides all four `VersioningType` strategies).
- Catalog/discovery, JSONiq surface, REST serving gate.
- The `IndexDef`↔`ProjectionIndexMetadata` definitional overlap (a separate cleanup: the
  root path / field paths / column kinds in slot 0 are derivable from the catalogued
  `IndexDef`; only the build state — fences, leaf count, build revision, stale flag — is
  genuinely leaf-side).

---

*Sources for the prior-art section: DuckDB storage/format & lightweight-compression
docs; ClickHouse "How we built fast UPDATEs" series and the Lightweight UPDATE / patch
parts documentation.*
