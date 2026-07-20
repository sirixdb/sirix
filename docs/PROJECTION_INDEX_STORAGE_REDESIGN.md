# Projection Index Storage Redesign

Status: **design, reviewed** (task #57's preferred resolution) — checked against
the as-built code on `main` after PR #1116 (compact codec), #1117
(IndexController lifecycle + revision-scoped catalog), #1120 (incremental
maintenance, per-leaf fences), and #1122/#1128 (production hardening, REST
serving). A four-lens code review (page/commit machinery, query/serving,
build/maintenance, float+FSST integration) has been folded into this revision;
statements below that correct earlier drafts are marked *(corrected)*.

Scope of the redesign:

1. **Segmented storage**: lift payload bytes out of HOT slot values into
   dedicated copy-on-write **column-segment pages**, addressed from a tiny
   per-leaf descriptor + reference side map.
2. **`NUMERIC_DOUBLE` columns** (ALP-encoded) — floats stop being rejected;
   in scope, not future work.
3. **FSST-compressed string dictionaries** — reusing the in-repo
   `io.sirix.utils.FSSTCompressor`; in scope, not future work.

Authoritative source for the current layout (class javadoc is the current
documentation of record): `ProjectionIndexHOTStorage`,
`ProjectionIndexLeafPage`, `ProjectionIndexLeafCodec`,
`ProjectionIndexMetadata` (all under
`bundles/sirix-core/src/main/java/io/sirix/index/projection/`).

---

## 1. Current storage architecture (as built, interim)

### 1.1 Page hierarchy

Projection indexes plug into the same secondary-index machinery as
CAS/PATH/NAME:

```
RevisionRootPage
  └─ projectionIndexPageReference
       └─ ProjectionIndexPage            (PageKind.PROJECTIONPAGE, byte 16)
            ├─ ref[defId 0] ─► HOT sub-tree (HOTIndirectPage* → HOTLeafPage*)
            ├─ ref[defId 1] ─► HOT sub-tree
            └─ ...                       one CoW-versioned sub-tree per IndexDef
```

- `IndexType.PROJECTION` (byte 10); leaf records travel as
  `NodeKind.PROJECTION_INDEX_LEAF` (byte 44).
- `ProjectionIndexPage` mirrors `CASPage`/`PathPage`/`NamePage`: a
  `ReferencesPage4 → BitmapReferencesPage → FullReferencesPage` delegate keyed
  by `IndexDef#getID()`, plus per-index bookkeeping (`maxNodeKeys`,
  `maxHotPageKeys`, `currentMaxLevelsOfIndirectPages`).
- Projection maintenance is **JSON-only by construction**: only
  `JsonIndexController` creates the listener; the XML controller inherits the
  null default. JSON numbers cannot be NaN/Infinity — relevant to §2.6.

### 1.2 Logical slot layout inside one definition's sub-tree

| leafIndex | Content |
|---|---|
| 0 | `ProjectionIndexMetadata` payload (`PIXM` magic, version, stale flag, `leafCount`, `buildRevision`, per-leaf record-key fences, root path, column shapes) |
| 1..N | one compacted `ProjectionIndexLeafPage` payload per logical leaf (≤ 1024 rows each) |

`leafCount` in the metadata **bounds every read**: a rebuild that shrinks the
projection leaves stale payloads at higher slots, which are never consumed.
`parse()` returns `null` (→ rebuild) for missing magic or unknown version, and
throws only on structural corruption.

### 1.3 Sub-leaf chunking (the actual HOT entries)

Each logical payload (metadata or leaf) is split into fixed-size chunks
(`CHUNK_SIZE`, default 4096, `-Dsirix.projection.chunkSize`), each chunk its own
HOT entry under a composite key:

```
rawKey = (leafIndex << 8) | (chunkIdx & 0xFF)      // chunkIdx in the low byte
hotKey = PathKeySerializer.serialize(rawKey)       // sign-flipped 8-byte BE
```

- Sign-flipping preserves `(leafIndex, chunkIdx)` tuple order under unsigned
  byte comparison, so chunks of one leaf sort contiguously.
- `MAX_CHUNKS_PER_LEAF = 256` caps one leaf at 1 MB serialized; leafIndex
  effectively has 55 usable bits.
- A class-load guard rejects any `CHUNK_SIZE` whose single-entry footprint
  cannot fit an empty `HOTLeafPage` (`DEFAULT_SIZE` = 64 KB).
- **Tombstones:** the HOT trie has no per-entry delete, so a shrinking re-put
  writes zero-length chunk values over the stale tail. Readers treat a missing,
  empty, or partial (`< CHUNK_SIZE`) chunk as end-of-payload.

Because each chunk is a separate HOT slot, unchanged chunk slots are shared
across revisions by the HOT fragment merge: a one-row change rewrites only the
chunk(s) whose bytes changed, at 4 KB granularity.

### 1.4 Payload encodings

Three self-describing formats, all carrying magic + version:

- **Raw scan form** (`ProjectionIndexLeafPage.serialize()`, `PIX1` presence
  tail, tail version 1): flat little-endian primitive arrays (header at
  offsets 0/4/8/16/24: rowCount, columnCount, first/lastRecordKey, kinds;
  record keys; per-column zone maps + bodies), then the mandatory presence
  tail (per-column flags with `UNREPRESENTABLE` (0x01) / `NON_INTEGRAL` (0x02)
  bits, per-column presence bitmaps, tailLength, version, magic).
  `deserialize()` rejects tail-less payloads as corrupt —
  integrality/presence provenance is never fabricated.
- **Compact persisted form** (`ProjectionIndexLeafCodec`, `PIXC` magic,
  version 1): delta/FOR record keys, frame-of-reference bit-packed numerics
  (widths > 56 fall back to aligned raw-64), packed dict-ids, marker-byte
  presence. Decodes **byte-identically** back to the raw form.
- **Metadata form** (`ProjectionIndexMetadata`, `PIXM` magic, version 1).

The redesign keeps the raw scan form as the in-memory kernel target and keeps
the compact codec's per-column encodings, but restructures the *persisted
grouping* into per-column segments (§2.3) and adds two new column encodings
(§2.6, §2.7).

### 1.5 Write and read paths (current) *(corrected)*

- **Build:** `ProjectionIndexBuilder.buildAndPersist` walks the record set and
  buffers **every encoded leaf on the heap** (`List<byte[]>`) — ~240 MB of
  leaf byte[]s at the 100 M-row / 97 k-leaf scale, violating the class's own
  streaming javadoc. `persist` then writes **slot 0 first** (fences computed
  via `ProjectionIndexLeafCodec.recordKeyRange` over the buffered leaves),
  then leaves at slots 1..N via the heap `put` chunking path. The off-heap
  `putFromSegment`/`scratchSegment`/`serializeIntoSegment` HFT path has **zero
  production callers** (one test) — it is dead code today. Slot-0-first vs
  -last is crash-irrelevant (all writes ride one CoW commit), but note the two
  writers disagree: incremental maintenance writes slot 0 **last**.
- **Incremental maintenance:** `ProjectionIndexChangeListener.applyIncremental`
  reads slot-0 fences (one read), two-pointer-merges dirty record keys against
  ascending zone maps, **re-extracts each touched leaf entirely**, appends new
  records at the tail (record keys are monotone, so appends always classify
  cleanly), rewrites slot 0 with new `leafCount`/`buildRevision`/fences.
  Degradation ladder: unattributable changes (subtree moves, > 100 k dirty
  records via `-Dsirix.projection.maxIncrementalRecords`, non-ascending zone
  maps over the quadratic-scan guard, any inconsistency `return false` path)
  → same-commit `rebuildFully()`; double unexpected failure → stale tombstone
  (corruption valve). A commit that creates a self-nested record-set shape
  makes the rebuild rung throw (`assertNoNestedRootPcrs`) and lands in the
  valve — by design, but previously undocumented.
- **Hydrate:** `readAll` → depth-2-parallel HOT scan; chunk fragments of one
  leaf can be dispersed across HOT sub-trees after splits, so accumulation
  places chunks at absolute offsets and merges fragments chunk-wise
  (`LeafChunkAccumulator`).
- **Query side:** the catalog decodes all leaves once per (resource, defId,
  buildRevision) into a Caffeine `DATA` cache of raw-form heap `byte[]`s
  wrapped in a `ProjectionIndexRegistry.Handle`; **all kernels consume whole
  hydrated leaf payloads** via VarHandle reads on heap arrays. A
  MemorySegment/FFM kernel port was measured **+4.5 % wall** and reverted —
  heap `byte[]` is the kernels' performance identity, which constrains §4.

### 1.6 Why this is interim: the SLIDING_SNAPSHOT contract violation

`docs/ARCHITECTURE.md` promises O(1) writes per **record** under
`SLIDING_SNAPSHOT`. The projection's natural record is a ~32-byte row, but the
smallest CoW-shareable unit is a 4 KB chunk. Carrying 4 KB values in HOT slots
also means one 64 KB `HOTLeafPage` holds only ~15 chunk entries, forcing deep
trie splits at scale — the source of two historic failure families (both fixed
and regression-guarded): grow-overwrite chunk drops and stale-swizzle
use-after-close during deep split cascades.

Second defect: **byte-offset chunks compose badly with bit-packed encodings**.
The compact codec's streams have data-dependent lengths, so a value change
that alters a column's FOR width or grows its dictionary shifts every
downstream byte — "rewrite only chunks whose bytes changed" degenerates toward
"rewrite every chunk from the edited column onward".

---

## 2. Redesign

### 2.1 Why segments, not byte chunks — lessons from Parquet, DuckDB, ClickHouse

| | Batch unit | Column unit | Stats / pruning | Dictionaries | Validity | Updates |
|---|---|---|---|---|---|---|
| **Parquet** | row group (~128 MB) | column chunk → data pages (~1 MB) | min/max/null-count per page and column chunk; page skipping via column/offset index | separate dictionary page per column chunk | definition levels per page | immutable; rewrite files |
| **DuckDB** | row group (122,880 rows) | per-column segments (~256 KB blocks) | per-segment zone maps; encoding picked per segment (FOR, bit-pack, dictionary, FSST, ALP) | per-segment | validity mask separate from values | MVCC update/delete vectors over immutable segments |
| **ClickHouse** | part → granules (8,192 rows) | one file per column (wide parts) + mark files | sparse primary index; per-granule-range skip indexes | LowCardinality encoding | separate null maps | background merges; mutations rewrite parts |

Transferable rules:

1. **Chunk on column boundaries, never byte offsets.** A byte-offset slice of
   a bit-packed stream has no independent meaning: no query can read column
   `c` without fetching every chunk, and no writer can touch a value without
   risking the byte-shift cascade (§1.6). Semantic segments make both
   directions columnar.
2. **Stats live above the data.** All three systems prune I/O from metadata
   without touching data pages. The leaf **descriptor** (§2.3) carries the
   raw-form header (rowCount, columnCount, kinds, record-key fences — every
   kernel needs these, and `countRows` needs nothing else) plus per-segment
   byteLen/contentHash and per-column min/max/flag bits. *(corrected)* Two
   honest qualifications: (a) per-leaf record-key fences are used only by
   maintenance today — no query kernel predicates on record keys, so
   fence-based query pruning is a possible follow-up, not a redesign benefit;
   (b) because the catalog caches decoded leaves per buildRevision,
   descriptor-level stats save **cold-open / first-touch I/O and cache
   footprint**, not steady-state query CPU — the steady-state win requires
   the per-segment cache restructure in §2.5/P5.
3. **Dictionaries are separable.** Splitting a string column's local dict
   (segment `3c+2`) from its id stream (`3c+1`) lets dictionary-union
   count-distinct read dictionary segments plus the (tiny) presence bitmaps —
   *(corrected)* not "only DICT": `distinctPresentStrings` is
   presence-dependent (the interned-`""`-phantom vs real-empty-string
   disambiguation scans packed ids when a leaf has an empty-string dict entry
   and missing rows). Still a large win over hydrating whole leaves.
4. **Presence/validity is a first-class, tiny stream.** Each column's
   presence bitmap sits at the *head* of its column segment (self-contained
   column reads); ALL_PRESENT/ALL_MISSING flag bits are mirrored into the
   descriptor. A presence-only (EXISTS-style) predicate kernel does not exist
   today and would be new functionality.
5. **Encodings are chosen and described per segment** — each segment is
   self-describing (magic + version + encoding byte).
6. **Do NOT copy their update stories.** Parquet is immutable, ClickHouse
   mutations rewrite parts, DuckDB layers MVCC delta vectors over fat
   (256 KB) segments. Sirix already has page-granular CoW with cross-revision
   structural sharing; at 1024-row leaves the segment recode is cheap.
   Revisit delta vectors only if update-heavy soaks show recode churn
   dominating (§11).

Deliberate divergence: the "row group" stays at `MAX_ROWS = 1024` — the small
group is load-bearing for incremental maintenance (touched leaves are
re-extracted wholesale) and matches the SIMD kernels' 1024-bit masks.

### 2.2 Building blocks that already exist *(corrected — this is the largest revision)*

**The OverflowPage chain is a working end-to-end template for reference-bearing
values, not merely a rejected alternative.** Verified against code:

- `KeyValueLeafPage` stores oversized record bytes as `OverflowPage`s hung on
  `PageReference`s in a **side map** (`Map<Long, PageReference> references`),
  *outside* slot bytes, with no TransactionIntentLog entry.
- `KeyValueLeafPage.commit(...)` **overrides** the default `Page.commit`
  (which skips NULL-logKey refs) and descends into every overflow ref;
  `NodeStorageEngineWriter.commit(PageReference)` has an
  `instanceof OverflowPage` branch that writes the page immediately, and
  `FileChannelWriter.write` assigns the durable key **= file offset** at
  write time during the recursive descent — strictly before the parent page's
  bytes are produced (`PageKind.KEYVALUELEAFPAGE` refuses to reuse cached
  serialization while any overflow key is NULL).
- The parent serializer persists overflow refs as **bare 8-byte keys** (no
  hash, no fragment list); read-back re-resolves by key; unchanged refs are
  carried into the next revision by the versioning combine — i.e. §3's
  "share prior segment by reference" already exists for overflow values.
- Rollback safety comes from "**never write before commit**", not from TIL
  membership — OverflowPages are written only inside commit; on rollback they
  were simply never written.

`HOTLeafPage` already has the same side map (`pageReferences`) with
accessors — but it is inert: no `commit` override, never serialized by
`PageKind.HOT_LEAF_PAGE`, and dropped by `mergeHOTFragmentsByKey` and
`copy()`. Wiring it up is the delta, not building the machinery.

**The `ChunkDirectory`/`BitmapChunkPage` machinery is genuinely stranded and
unsafe as-is**: `combineBitmapChunks` has zero production callers; a
`ChunkDirectory` ref reaching the writer today would miss every branch and
silently persist `key = -1`. Its serializer has **no magic/version byte** and
truncates db/resource ids to shorts. We therefore define a **new** descriptor
wire format (§2.3) rather than "growing" that one; un-stranding the CAS bitmap
path with the same hooks is a follow-up (§11).

**FSST already exists and is production-wired** for PAX string storage:
`io.sirix.utils.FSSTCompressor` (symbol-table build/serialize, escape coding,
batch decode, gating heuristics: `MIN_COMPRESSION_SIZE=32`,
`MIN_SAMPLES_FOR_TABLE=64`, `MIN_TOTAL_BYTES_FOR_TABLE=4096`,
`MIN_COMPRESSION_RATIO=0.15`), plus the pre-encoded-literal compare pattern in
`ColumnarStringFilter`. §2.7 reuses all of it.

**Order-preserving double encoding already exists**:
`CASKeySerializer.encodeNumericOrderPreserving` (sign-flip for positives,
all-bits-flip for negatives, NaN canonicalized) with its exact inverse. §2.6
reuses the transform.

### 2.3 Target layout: descriptor slots + side-map refs + segment pages

```
HOTLeafPage                     one slot per logical projection leaf
  slot key   = PathKeySerializer.serialize(leafIndex)
  slot value = LeafDescriptor ("PIXD", version 1):
                 rowCount, columnCount, kinds[], firstRecordKey, lastRecordKey,
                 segCount, per segment:
                   segmentId          // 0=KEYS, 3c+1=BODY(c), 3c+2=DICT(c)
                   byteLen            // exact segment length
                   contentHash        // XXH3-64 of segment bytes
                   min, max, colFlags // BODY segments only (see §2.6 for doubles)
  side map   = (leafIndex << 8 | segmentId) → PageReference   // refs, NOT in slot
                   │
                   ▼
        ProjectionSegmentPage (new PageKind)
        payload = one length-prefixed encoded segment; OverflowPage-shaped
        (getReferences/commit/getOrCreateReference throw); identified by
        file offset like OverflowPage — NO logical page key, NO fragment
        chain (whole-page last-writer-wins), CoW-shared by reference.
```

Design decisions, with rationale:

- **Refs live in the HOT leaf's existing side map, not inside slot bytes.**
  *(corrected)* This is the cheaper of the two options and the earlier draft
  never considered it: the commit walker never parses slot values; the
  descriptor stays a pure-data value; the refs section serializes like
  KeyValueLeafPage's overlong entries. The composite `(leafIndex << 8 |
  segmentId)` key reuses the existing composite-key encoder. Consequence:
  `segmentId` must fit 8 bits → `3·columnCount + 2 ≤ 255` caps a projection
  at **84 columns** — validated at creation (§5.1-10).
- **Segment pages have offset identity, like OverflowPage.** They consume no
  logical page key, need no fragment chains (LWW whole-page rewrites), and
  the `maxHotPageKeys` extension and its crash-replay hazard from the earlier
  draft **evaporate**. *(corrected)*
- **`contentHash` does double duty**: (a) the `writeSegment` no-op test —
  maintenance re-encodes a column and compares hash+byteLen against the prior
  descriptor entry *without reading the prior segment bytes*; on match the
  prior `PageReference` is carried forward untouched; (b) integrity on read —
  overflow refs carry no checksum today (a real gap we choose not to
  inherit); a hash/length mismatch at segment load is corruption → fail soft
  (§4).
- **Header residence** *(corrected)*: rowCount/columnCount/kinds/fences live
  in the descriptor — every kernel needs them, `countRows` is answered from
  descriptors alone, and the raw-form assembly has a complete header source.
- **Empty leaf ≠ tombstone** *(corrected — real hazard found in review)*:
  maintenance can legitimately persist a **live zero-row leaf** (every row of
  a touched leaf deleted; slots stay contiguous). Representation: a
  zero-length slot value is the tombstone (absent leaf); a descriptor with
  `rowCount = 0, segCount = 0` is a live empty leaf. The catalog's
  truncated-store check counts descriptors, so mid-store empty leaves do not
  trip fail-soft.
- Metadata slot 0 keeps its `PIXM` payload as a single segment under the same
  mechanism; the stale tombstone remains a *valid tiny PIXM payload with the
  stale flag*, distinct from both empty-leaf and absent states.

### 2.4 What must be built — precise hook list *(corrected)*

The flush-ordering work lives in the commit path, **not** in
`combineRecordPages` (combine functions run at read/CoW-prepare time; the
earlier draft mislocated this):

1. `ProjectionSegmentPage` — new `PageKind` entry, OverflowPage-shaped
   contract (leaf of the commit recursion; structural accessors throw).
2. `HOTLeafPage.commit(StorageEngineWriter)` override mirroring
   `KeyValueLeafPage.commit` — descend into side-map refs.
3. `NodeStorageEngineWriter.commit(PageReference)` — extend the OverflowPage
   branch to `ProjectionSegmentPage` (write immediately, key = offset).
4. `PageKind.HOT_LEAF_PAGE.serializePage/deserialize` — persist the side-map
   refs section (bare 8-byte keys, overlong-entries style). Must interact
   correctly with the **sparse dirty-entry emit**: a slot whose ref was
   newly resolved must be included in the emitted fragment, and a
   ref-only-dirty slot must not force spurious fragment growth.
5. `VersioningType.mergeHOTFragmentsByKey` + `HOTLeafPage.copy()` — carry and
   merge side-map refs across fragments (today: dropped).
6. `PageReference.getPage()` closed-page-as-cache-miss guard extended to the
   new kind (the stale-swizzle fix is `instanceof HOTLeafPage`-specific).
7. `ProjectionIndexHOTStorage` rewrite — descriptor + segment API:
   `put(leafIndex, leaf)`, `putSegment(leafIndex, segmentId, bytes)`,
   `getSegment`/`getSegmentBytes`, `get(leafIndex)` (assembled raw form),
   `readAll`/`readOne`; keys become plain `leafIndex`; shrink = descriptor
   entry removal + slot-value tombstone for whole-leaf deletes.
8. **Codec segmentation** — per-segment `encodeSegment`/`decodeSegment`
   (KEYS / BODY(c) incl. presence head + flags / DICT(c)), each
   self-describing (magic, version, encoding byte), plus an assembler that
   reconstructs the raw scan form **byte-identically**. The provenance probes
   (`probeNumericNonIntegral`, `probeSparseEvidence`) are restructured
   **column-scoped** — today they validate via whole-payload tail-boundary
   equality, which ceases to exist; their fail-closed contract transfers to
   per-segment (magic/version/length/hash) validation.
9. **Catalog / Handle / kernel restructure** (the query-side work the earlier
   draft under-scoped; details in §4 and P5).

### 2.5 What this buys — with honest scoping *(corrected)*

- **HOT slots shrink from ~4 KB chunks to a descriptor** (~60–150 B for 3
  columns with stats). One 64 KB `HOTLeafPage` holds hundreds of leaves
  instead of ~3–15 chunk entries; the trie gets shallower by orders of
  magnitude; deep split cascades become rare; scale ceiling moves well past
  100 M records. Directory-entry cost stays honest because segment refs are
  bare 8-byte keys with no fragment lists (LWW pages need none).
- **Columnar I/O on cold paths immediately**: hydrate-per-buildRevision and
  first-touch reads fetch only needed segments; `countRows` and
  zone/presence/flag pruning are descriptor-only. **Steady-state columnar
  wins arrive with the P5 cache restructure** (per-column heap arrays in the
  Handle), because kernels run over cached heap bytes either way.
- **Update containment, precisely scoped**: a one-row, one-column *in-place
  value update* rewrites one BODY segment (+ DICT iff the dictionary grew) +
  one descriptor slot. **Deletes and appends do not get this containment**: a
  row delete changes `rowCount` and re-encodes every segment of the leaf; a
  tail append dirties KEYS + every BODY (+ any DICT that interned) — bounded
  at one ≤1024-row leaf, same bytes as today, and new-leaf spill writes
  O(columnCount) fresh segment pages, not O(1). Untouched-column
  byte-identity under re-extraction is **confirmed deterministic** (append-
  only first-occurrence dict interning, order-stable replay, per-leaf flag
  derivation) — the no-op share is sound for the update case.
- **Full rebuilds become share-friendly**: `rebuildFully` routed through the
  hash-compare no-op shares every unchanged leaf's segments by reference —
  today a rebuild rewrites every chunk. This materially softens the
  `MAX_INCREMENTAL_RECORDS` cliff (threshold worth re-measuring afterwards).
- **Real deletes** replace zero-length chunk tombstones; chunk dispersal,
  `LeafChunkAccumulator`, gap probes, the `CHUNK_SIZE` knob, and the 1 MB
  leaf cap all disappear.
- **Streaming build**: the builder's only reason to buffer all leaves was
  computing fences before its slot-0-first write; under §3 it holds one leaf
  in memory and accumulates two fence longs per leaf (~240 MB → ~2 MB heap at
  100 M rows), finally matching its javadoc.

### 2.6 NUMERIC_DOUBLE columns via ALP — in scope

Today floats are rejected at exactly one site
(`CreateProjectionIndex.mapType`: "use long, boolean, or string"), while the
builder's `mapTypeToColumnKind` silently truncates `DEC/DBL/FLO` → longs — an
asymmetry only reachable from non-user paths. The redesign closes the gap
end-to-end, **decoupled from the long-integrality machinery** (the
`NON_INTEGRAL` flag and `numericColumnIsIntegral` gates remain long-only):

- **Creation**: `mapType` accepts `double|float` → `Type.DBL`/`Type.FLO`
  (and `decimal` → `Type.DEC`); `mapTypeToColumnKind` (the declared single
  source of truth, also consumed by the listener's shape validation and the
  catalog's hydrate check) maps `DEC/DBL/FLO` → new
  `COLUMN_KIND_NUMERIC_DOUBLE = 3`. Every exhaustive kind-byte switch grows a
  branch: `ensureCapacity`/`appendRow`/`serialize`/`deserialize`, codec
  branches, `columnDataOffFor`/`leafDataEnd`/`evaluateLeafMask` offset walks,
  listener `defKinds`, catalog shape check, metadata kind byte.
- **Extraction**: `rtx.getNumberValue()` already returns the exact stored
  `Number` (JSON number nodes hold Double/Float/Integer/Long/BigInteger/
  BigDecimal via a value-type tag). Double columns store
  `doubleValue()` with a new per-column flag
  `COLUMN_FLAG_NOT_VALUE_EXACT = 0x04` set when the conversion rounded
  (BigInteger/BigDecimal not exactly representable) — same sticky-flag
  pattern as `NON_INTEGRAL`, giving value-exact consumers the same fail-
  closed gate. `Double`/`Float`/`Integer`/`Long` convert exactly. Non-finite
  values cannot arise from JSON (§1.1); defensively, NaN/±Inf →
  `UNREPRESENTABLE`. The page-level fused-long fast path returns its
  `Long.MIN_VALUE` sentinel for float/double slots, so extraction uses the
  node/`Number` route — as it already does.
- **Value representation**: the raw-form values array stores the
  **order-preserving transform** of the double bits
  (`CASKeySerializer.encodeNumericOrderPreserving`: positives XOR sign bit,
  negatives XOR all bits). Rationale: every existing compare surface —
  zone-map folding in `appendRow`, `zoneSkip`, `evalNumericBytes`,
  `evalBetween`, the `min=Long.MAX_VALUE` empty-column sentinel — is a
  signed-long compare, and transformed doubles are order-isomorphic to
  signed longs. Predicate literals are transformed once at plan time
  (GT/LT/GE/LE/EQ/BETWEEN all survive a monotone bijection), so **the
  predicate kernels and zone maps work unchanged**. The executor's
  `OP_FP_CMP`/`OP_DEC_CMP` threshold-rewrite gates for long columns are
  bypassed: double columns serve double-literal predicates natively.
- **Aggregates**: a `conjunctiveAggregateNumericDouble` kernel variant with a
  `double` sum/min/max accumulator (inverse-transform per matching row —
  branch-free two-op decode), surfaced as `Dbl` (xs:double) instead of
  `Int64` in the executor's stats-to-sequence step; `avg` is double division.
  Determinism: per-leaf partials merged in ascending leafIndex order (same
  discipline as the existing parallel driver); compensated summation is an
  option behind a flag, not default.
- **ALP segment encoding** (`BODY(c)` encoding byte): per leaf-column vector
  (≤1024 values — ALP's native vector size): sample-select decimal exponents
  `(e, f)`; encode `i = round(v·10^e / 10^f)`, verify bit-exact round-trip,
  else record the value in a verbatim exceptions list; FOR + bit-pack the
  integers (reusing the codec's `BitWriter`). Fallback **ALP-RD** for
  non-decimal data: split bits into a small left-parts dictionary + verbatim
  bit-packed right parts. If neither profits → raw-64. All lossless; encoding
  byte selects the branch; decode reproduces exact bits (then transform for
  the raw form). No ALP implementation exists in-repo — this is new code, but
  self-contained in the codec.
- **Zone maps for double columns** are stored in the transform domain in
  both the descriptor and the raw form — signed-long compares remain valid,
  and the `min > max` "no present value" sentinel stays unambiguous (the
  transform maps no finite double to `Long.MAX_VALUE`; NaN is excluded as
  unrepresentable).

### 2.7 FSST-compressed string dictionaries — in scope

FSST applies to the **persisted `DICT(c)` segment only**; the raw scan form
keeps plain UTF-8 dictionary bytes. This placement is forced by review
findings: **kernels compare dictionary entries as raw bytes pervasively**
(`evalStringEqBytes` literal resolution, `probeCanonicalDict`, dense remaps,
`distinctPresentStrings` dedupe, FNV hashing, `new String(...)`
materialization). Decompressing at segment decode (hydrate/cache-fill) means
**zero kernel changes**.

- Encode: when a dictionary passes `FSSTCompressor`'s existing gates
  (dictionaries are typically 8–50 entries — most will take the RAW header
  path; high-cardinality columns are the target), train a per-segment symbol
  table, store table + compressed entries; encoding byte distinguishes
  RAW/FSST.
- Decode: `parseSymbolTable` + batch decode into the raw dict layout;
  byte-identity of the assembled raw form is preserved (FSST here is a
  persisted-form transform exactly like FOR bit-packing).
- Follow-up (not in scope): pre-encoded literal compares against compressed
  dictionaries à la `ColumnarStringFilter` — only worth it if dictionary
  decode ever shows up in cold-open profiles.

---

## 3. Write path (redesigned)

```
put(leafIndex, leaf):                       // build + full-leaf rewrite
  prior = readDescriptor(leafIndex)         // null → fresh leaf
  desc  = newDescriptor(leaf)               // header + kinds + fences
  for each segment s in encodeSegments(leaf):     // KEYS, BODY(c), DICT(c)
    if prior != null && prior.byteLen(s.id) == s.byteLen
                     && prior.hash(s.id) == s.hash:
      carryForwardRef(s.id)                 // CoW share, no page write
    else:
      ref = newSegmentPage(s.bytes)         // written at commit, key=offset
      sideMap.put(compositeKey(leafIndex, s.id), ref)
    desc.addEntry(s.id, s.byteLen, s.hash, s.stats)
  dropSideMapEntriesNotWritten(prior, desc) // shrink = real delete
  writeDescriptorSlot(leafIndex, desc)      // tiny value; split-safe
```

- The no-op test is **hash+length against the prior descriptor** — no prior
  segment bytes are read *(corrected: the earlier draft's "no-op when bytes
  identical" had no mechanism; nothing compares bytes today)*.
- `putSegment(leafIndex, segmentId, bytes)` updates one segment + the
  descriptor slot in the same commit (stats/hash/refs stay consistent
  atomically).
- Maintenance containment per §2.5: updates → touched columns' segments;
  deletes/appends → all segments of the leaf (rowCount changed); the
  re-encode-then-compare loop makes this automatic. Per-column dirty
  tracking in the listener (`pathNodeKey → column` retained instead of
  discarded after the relevance filter) is a **CPU-only optimization** to
  skip re-encoding untouched columns — measured phase, not a correctness
  requirement (P4).
- Descriptor slot writes go through the loud `updateOrSplitInsert` machinery.
- The builder streams: one leaf encoded and written at a time, fences
  accumulated, slot-0 metadata written last.

## 4. Read path (redesigned)

- **Descriptor read**: one trie descent; parse positionally
  (allocation-free) from the slot slice. Available without segment loads:
  rowCount/columnCount/kinds, fences, per-column min/max (transform-domain
  for doubles), ALL_PRESENT/ALL_MISSING/UNREPRESENTABLE/NON_INTEGRAL/
  NOT_VALUE_EXACT bits, per-segment lengths+hashes.
- **Segment loads validate at fill**: magic/version/length/hash checked when
  a segment is fetched into the cache; a mismatch marks the definition
  `NOT_USABLE` for that buildRevision (negative-cached, as today) so
  corruption is caught at fill time, never mid-kernel, and queries do not
  re-attempt corrupt loads *(corrected: the earlier draft had no negative-
  caching story for lazy loads)*.
- **Kernels stay heap-based** *(corrected)*: segment bytes are copied into
  heap arrays at cache fill (the +4.5 % MemorySegment kernel regression is
  documented history; zero-copy applies to storage-internal paths, not
  kernels). The Handle contract (P5) becomes column-scoped: per-column
  payload access, canonical dicts built from DICT segments, provenance
  probes reading per-segment flag bytes (segment truth, not the descriptor
  mirror — the mirror may only *short-circuit toward* declining, never
  toward serving).
- **Kernel segment needs** (from review; drives the Handle API): predicates
  load BODY of every predicated column (+DICT for string-EQ literal
  resolution); aggregates load BODY(agg) + BODY of predicate columns — no
  KEYS; group-by loads BODY(g)+DICT(g) per group column + predicate columns;
  distinct loads DICT(g) + presence (+ ids only in the empty-string-phantom
  case); `countRows` loads nothing.
- **Cache plumbing** *(corrected — concrete sites)*: the descriptor cache is
  a sibling on the catalog's `DataKey` (resource, defId, buildRevision),
  reached through the existing `PROBES` hop (probes are keyed by *query*
  revision); it needs its own weigher; `invalidateUnder`/`clearCache` must
  enumerate it; the wtx path bypasses shared caches entirely (as today) and
  keys its uncommitted state on (listener identity, maintenanceEpoch) — the
  existing `UncommittedHandle` pattern is the template, and the
  Handle-internal lazy caches (`canonicalDicts`, `integralityEvidence`,
  `sparseStatus`) must live *inside* the epoch-discarded object, which is
  exactly what makes "old dict + new body" unrepresentable (§5.2-l).
- **Full hydrate** (generic pipeline / wtx serving): load all segments,
  assemble the raw scan form byte-identically.
- `readAll`: range-scan descriptors, then fetch segment pages with batched
  parallelism over leaves (no fragment merging). `readOne` (slot-0 stale
  probe): one descent + one segment load.

---

## 5. Corner-case catalog

### 5.1 Invariants to preserve

1. **Shrink-on-rewrite must not resurrect stale bytes.** Removing descriptor
   entries must be transactional with the segment rewrite — CoW covers it,
   but the commit hooks (§2.4-2/4/5) are where it can silently break. Port
   `ProjectionIndexHOTStorageGrowingPayloadTest` (reshaped: fewer/more
   columns across rebuilds) + a shrink-grow-shrink cycle test.
2. **Grow-overwrite must be loud or lossless.** Segment bytes never compete
   for HOT page space; the failure mode shifts to the descriptor growing
   (more columns) on a full HOT page → must split, never drop
   (`updateOrSplitInsert` contract retained).
3. **Stale-swizzle use-after-close** — extend the closed-page-as-cache-miss
   guard to `ProjectionSegmentPage` (§2.4-6); regression test through deep
   split cascades touching descriptors.
4. **Empty payload ≡ absent leaf; live empty leaf is distinct.** *(revised)*
   Zero-length slot value = tombstone; descriptor with `rowCount = 0` = live
   empty leaf (deletes can legitimately empty a mid-store leaf); `get`
   returns `null` only for the former. The truncated-store check counts
   descriptors.
5. **Lengths and hashes are explicit and must agree.** Descriptor `byteLen`/
   `contentHash` vs segment page's own length prefix/bytes: disagreement is
   corruption → fail soft + negative-cache (§4).
6. **Metadata is bounds authority.** `leafCount` bounds reads; rebuilds
   should now *actually delete* orphaned descriptors above `leafCount`
   (hygiene, not load-bearing).
7. **Provenance never fabricated.** Fail-closed probes transfer to
   per-segment validation; descriptor flag mirrors may only short-circuit
   toward declining. Byte-identity round-trip tests run end-to-end, including
   multi-KB dictionaries, ALP exception paths, FSST-compressed dictionaries.
8. **Rollback/isolation.** *(corrected)* The invariant is "**never write a
   segment page before commit**" (OverflowPage discipline) — TIL membership
   itself is not required; descriptors ride the TIL like any HOT slot.
9. **Dropped + recreated definitions.** Stale tombstone over slot 0 +
   `invalidateUnder` cache clearing (must enumerate the new caches); test:
   drop → recreate with fewer leaves/columns must not resolve old segment
   pages via stale descriptors.
10. **Misconfiguration fails fast.** *(revised)* New guards: `3·columnCount +
    2 ≤ 255` (segmentId byte) at creation; worst-case descriptor footprint
    must fit an empty HOT leaf page.
11. **Leaf-index contiguity + monotone record keys.** Gap-probe paths are
    deleted with the redesign; the append-partition classification in
    maintenance depends on **monotone node-key allocation** — document as an
    explicit invariant.
12. **Same-commit create+update / create+delete of a record** dedupe in the
    dirty set and classify as appends; create+delete is skipped when
    extraction finds nothing. Preserve; one regression test.
13. **The degradation ladder end-to-end**, including: nested-root shapes
    created mid-commit make the rebuild rung throw → tombstone valve;
    `emptyRecordSetAllowed` asymmetry (creation throws on unresolved root;
    maintenance rebuild persists a truthful `leafCount = 0` store — which
    must remain distinct from the stale tombstone, both being slot-0-only
    states).

### 5.2 Hazards introduced by the redesign

a. **Commit-order dependency.** *(corrected — precisely located)* Segment
   pages must be written (keys assigned) during the recursive commit descent
   before the HOT leaf's bytes are produced. The work lives in
   `HOTLeafPage.commit` + the writer's segment branch + the HOT serializer —
   **not** in `combineRecordPages`. Test: cold re-open after a commit that
   grew, shrank, and patched leaves in one transaction.
b. **Sparse dirty-entry emit.** *(new — found in review)* `HOT_LEAF_PAGE`
   serializes only dirty entries under non-FULL versioning. A slot whose
   side-map ref was newly resolved must be part of the emitted fragment; a
   ref-only change must not force spurious fragment growth; fragment merge
   and `copy()` must carry refs (today they drop them). This is the subtlest
   part of P1.
c. **Descriptor parsing on the read hot path.** Parse positionally from the
   slot slice, allocation-free; cache decoded descriptors per DataKey.
d. **Append-only disk growth, not reclamation.** *(reframed — the earlier
   draft's GC hazard was wrong)* Sirix has **no reclamation**: the store is
   append-only; nothing frees old pages; `truncateTo` exists only for
   rollback/crash recovery. Segment pages inherit the same cost model as all
   pages. Consequences: (i) "real deletes" free logical slots, never disk
   bytes; (ii) migration's old sub-trees stay on disk until resource
   copy/re-import; (iii) if revision-pruning/compaction ever lands, its
   reachability walk must descend into side-map refs — one forward-looking
   sentence, not current work.
e. **Slot-0 tombstone through the new funnel.** The valve must fully replace
   a multi-segment metadata descriptor (never merge). Note the legacy
   invalidation-only listener mode (`maintenanceTrx == null`) still exists in
   code though unreachable in production — cover or delete it.
f. **Mixed-format stores.** Old composite keys vs new plain-leafIndex keys
   are indistinguishable at the HOT layer; no value sniffing — migration
   gate only (§6).
g. **Parallel hydrate fan-out shifts.** Key space compresses 256×; re-measure
   `DEPTH2_MIN_FANOUT`; at scale, parallelism moves from trie sub-trees to
   batched segment fetches.
h. **Integrity decision.** *(revised)* Segment refs are bare 8-byte offsets
   (overflow precedent carries no hash); integrity is provided by the
   descriptor `contentHash` instead — this is deliberate and must not be
   weakened when someone "optimizes" the hash away. (The earlier
   `maxHotPageKeys` crash-replay hazard is void under offset identity.)
i. **Concurrent readers during maintenance.** wtx tier: descriptor + segment
   + Handle-internal lazy caches all live inside the epoch-discarded
   `UncommittedHandle`-style object; committed tier: buildRevision keying
   suffices (no epoch). Torn reads are unrepresentable only if nothing is
   separately evictable — enforce structurally, don't document around it.
j. **Small-segment fragmentation and descriptor overhead.** A typical
   3-column compact leaf (~2.4 KB) becomes 4–5 segments plus a ~100 B
   descriptor. Log-structured variable-size pages mean no block padding, but
   per-page headers are real — measure against the 5.6 % disk-tax baseline;
   coalesce sub-threshold segments into a shared bundle page only if
   measurement demands (trades back update containment).
k. **Stats mirror vs segment truth.** Mirror may prune/decline; serving
   gates read segment truth (5.1-7). Divergence = corruption → fail soft.
l. **Dictionary/id co-consistency.** DICT and BODY of a column are one
   logical unit; cache them under one key/epoch (see i). The concrete
   historical site: `Handle.canonicalDicts` — coherent today only because it
   lives inside the discarded handle; keep that property.
m. **Double-column corner cases.** *(new)* Transform-domain min/max must
   never be compared against untransformed literals (transform literals at
   plan time, one place); NaN/±Inf unreachable from JSON but defensively
   unrepresentable; `NOT_VALUE_EXACT` gates value-exact serving of
   decimal-fed double columns; double aggregation order fixed (ascending
   leafIndex merge) for run-to-run determinism.
n. **FSST corner cases.** *(new)* Encoding is deterministic per symbol table,
   but table *training* must be deterministic for the writeSegment no-op
   hash to be stable across identical re-encodes (FSSTCompressor's builder
   is deterministic given identical input order — dictionary order is
   append-only interning order, which is deterministic; pin with a test).
   Escape-heavy adversarial inputs must round-trip; the RAW-header fallback
   path must be taken when compression doesn't pay (existing gates).

### 5.3 Corner cases that disappear

- The byte-shift cascade (contained inside one segment by construction).
- Chunk dispersal across HOT sub-trees; `LeafChunkAccumulator`; fill
  bitmaps; absolute-offset assembly.
- Zero-length chunk tombstone semantics and tail tombstone probing loops.
- The 256-chunk / 1 MB leaf cap; the `CHUNK_SIZE` knob and its class-load
  guard.
- Exact-multiple-of-`CHUNK_SIZE` end-of-payload ambiguity.
- The gap-probe discovery paths and their implicit ≤16-gap invariant.
- `maxHotPageKeys` extension for segment pages (offset identity).

---

## 6. Migration

Precedent: the `PIXC`/`PIX1` consolidation shipped as a deliberate break;
unknown metadata versions already degrade to rebuild-on-first-use.

1. Bump `ProjectionIndexMetadata.VERSION` to 2 — asserts segment-directory
   storage and the extended flag set (`NOT_VALUE_EXACT`), and records column
   kinds including `NUMERIC_DOUBLE`.
2. On open, version-1 (or missing) metadata → automatic rebuild through the
   always-maintained contract (or `-Dsirix.projection.forceRebuild=true`).
3. No dual-format read path, no value sniffing. *(corrected)* Old sub-tree
   pages become unreferenced in the new revision but **stay on disk forever**
   (append-only store, no reclamation); only a resource copy/re-import sheds
   them. Say so in release notes rather than pretending they age out.

---

## 7. Alternatives considered

- **Per-row HOT slots**: destroys columnar layout, 1024× key overhead;
  rejected.
- **Per-column HOT slot values (no indirection)**: fixes update containment
  but keeps multi-KB values in the trie — fan-out/deep-split problems remain.
  (An earlier draft rejected "per-column chunks" wholesale on this argument —
  that conflated per-column *slot values* with per-column *segment pages*;
  the indirection is what removes the objection.)
- **Byte-offset chunk pages behind a directory** (the original javadoc
  sketch): fixes fan-out but keeps reads non-columnar and the byte-shift
  cascade. Subsumed by semantic segments at the same machinery cost.
- **Refs inside slot values** (earlier draft): requires the commit walker to
  parse and re-serialize slot values after key resolution; the side-map
  design (OverflowPage template) avoids that entirely. *(new)*
- **Overflow-record spill of whole values**: fixes fan-out, not share
  granularity; subsumed — but its **commit machinery is the template we
  reuse**.
- **Reusing `ChunkDirectorySerializer` v1**: unversioned wire format,
  truncated ids, unsafe half-wired commit path — replaced, not grown. *(new)*

---

## 8. Competitive positioning — intent, measured standing, and the road past the ballpark

The stated goal is to compete with the fastest OLAP engines (DuckDB,
ClickHouse) on analytical queries over document data. This section records
where we measurably stand, decomposes the remaining gap, states exactly which
gap components this redesign closes, and sequences the work that lies beyond
it — so the plan in §9 has explicit success criteria instead of vibes.

### 8.1 Measured standing (the ballpark test)

Head-to-head at **100 M records, same 20-core box**, harness
`SirixVsDuckBenchMain` + `bench/duck_bench.py` (full protocol and caveats in
`docs/COMPARISON_DUCKDB.md`; min of 3 timed runs, fresh executor per run,
results verified byte-identical to the interpreted pipeline):

| query shape | SirixDB PGO native | SirixDB JVM | DuckDB | verdict |
|---|---:|---:|---:|---|
| filtered count (`age > 40 and active`) | **33 ms** | 53 ms | 40 ms | ahead |
| filtered range count (two preds + bool) | **42 ms** | 52 ms | 44 ms | ahead |
| filtered group-by (`where active group by dept`) | **43 ms** | **41 ms** | 59 ms | ahead |
| `sum(age)` / `avg(age)` / `min+max(age)` | 16–19 ms | 21–22 ms | 10–18 ms | 1.1–1.6× |
| group-by one key | 71 ms | 80 ms | 28 ms | 2.5× |
| group-by two keys | 240 ms | 251 ms | 115 ms | 2.1× |
| count-distinct | 81 ms | 76 ms | 18 ms | 4.2× |

Against **ClickHouse** (measured at 10 M / 4 threads via `bench/ch_bench.py`,
PR #1116): two-key group-by 34 ms vs 103.6 ms, count-distinct 4 ms vs
70.3 ms — SirixDB ahead on both measured shapes. ClickHouse has not yet been
run at 100 M; P8 records it.

Conclusion: **on covered shapes, per-kernel physics is already competitive**
— ahead of DuckDB on the filtered shapes, within 1.1–2.5× on aggregates and
group-bys, ahead of ClickHouse where measured. The competition problem is not
kernel speed; it is the three gaps below.

### 8.2 The scale inversion — per-leaf amortization

The dictionary-heavy shapes **win at 10 M and trail at 100 M**:
count-distinct 4 ms vs DuckDB's 6.3 ms at 10 M, but 76–81 ms vs 18 ms at
100 M; two-key group-by 34 ms vs 42.6 ms at 10 M, 240–251 ms vs 115 ms at
100 M. The cause is structural, not incidental: at 1024 rows per leaf, 100 M
rows = ~97,000 leaves, and the dict-id kernels pay a **per-leaf** cost —
dictionary decode, canonical-id remap (`dictSize × canonLen` byte-compares),
per-leaf hash merges — that DuckDB amortizes once in a global hash table.
Per-leaf work that is noise across 9,700 leaves dominates across 97,000.

The 1024-row group is load-bearing (maintenance re-extraction granularity,
SIMD mask width — §2.1) and stays. The fix is therefore **cross-leaf
amortization of dictionaries**, not bigger leaves — R1 in §8.6. This redesign
is the prerequisite, not the fix: separate `DICT(c)` segments (§2.3) make
dictionaries independently addressable, which is what makes a store-level
canonical dictionary buildable and maintainable at all.

### 8.3 Gap taxonomy

| # | Gap | Quantified | Character |
|---|---|---|---|
| G1 | **Shape coverage.** DuckDB/ClickHouse run arbitrary SQL fast: joins, sorts, windows, high-cardinality aggregation, string functions. We have ~a dozen fail-closed kernels; everything else falls to the generic pipeline. | Fallback is scan-class: ~44–59 s (JVM) to ~300–350 s (native) at 100 M — a 1000× cliff, not a slope. | Query-engine work; out of scope of this redesign (see §8.6 R2). |
| G2 | **Per-leaf amortization** on dictionary shapes at scale (§8.2). | count-distinct 4.2×, two-key group-by 2.1× at 100 M — after *winning* both at 10 M. | Kernel + auxiliary-structure work; this redesign builds the prerequisite (DICT segments), R1 closes it. |
| G3 | **Column-type coverage.** No doubles → whole benchmark categories (price/discount-style aggregates) cannot run on the fast path at all. | Category-blocking, not a ratio. | **Closed by this redesign** (P6, ALP). |
| G4 | **Cold build / ingest.** 6-column projection build at 100 M ≈ 319 s (one full-store walk); DuckDB generates its table in 27.3 s. Store shred itself is 548 s (182 k records/s) — a different job (fully versioned tree vs a column table). | One-time per store+shape; re-encode of a persisted projection is ~1.2 s; disk-cold reopen hydrate ~8 s (improves further with column-pruned hydrate, P3/P5). | Mitigated (persistence + always-maintained semantics mean it is paid once, then amortized forever); streaming build (P4) removes the memory spike. Not a query-latency gap. |
| G5 | **Deployment-shape caveats.** Native Image: without `-H:+VectorAPISupport` kernels run 10–600× slower; projection *build* is ~7.6× slower under native than JVM. | Documented footguns. | Documentation/tooling; P8 keeps the flags pinned in the bench harness and docs. |

### 8.4 What this redesign fixes — and what it deliberately does not

| Gap | Status after this redesign |
|---|---|
| Storage scale ceiling, deep-split failure families | Fixed (P1–P3) — removes the disqualifiers for 100 M+ stores |
| Cold-open I/O, disk tax, hydrate time | Improved (column-pruned hydrate, descriptor-only decisions, FSST) — P3/P5/P7, measured at P8 |
| G3 doubles | Fixed (P6) |
| G4 build memory + dead HFT path | Fixed (P4 streaming build) |
| G2 scale inversion | **Prerequisite built** (DICT segments); fix is R1, after P5 |
| G1 coverage | **Not addressed** — separate query-engine roadmap (R2); this doc only guarantees the fallback stays correct |
| Update/versioning story | Already structurally ahead (§8.5); redesign strengthens it (true per-segment CoW sharing) |

### 8.5 The structural advantage — versioned analytics

Where the fastest OLAP engines cannot follow, because their speed is built on
immutability assumptions we do not make:

| | SirixDB (this redesign) | DuckDB | ClickHouse |
|---|---|---|---|
| Point/row updates | per-commit incremental maintenance; one BODY segment + descriptor per touched column (in-place updates) | MVCC update/delete vectors layered over immutable 256 KB segments; checkpoint rewrites | mutations rewrite whole parts (documented as heavyweight) |
| History | **every revision queryable**, CoW-shared storage, no copy | none (WAL for recovery only) | none (TTL/merges discard) |
| Time-travel analytics | same kernels over any revision's snapshot — the catalog is revision-scoped by construction (§1) | export/snapshot workflows | backup/restore |
| Audit/tamper story | hash-chained pages (see `TAMPER_EVIDENCE_PLAN.md`) | n/a | n/a |

"Analytics over versioned documents" is the position where we are not chasing
the ballpark but defining a different field: a query like *"group-by over the
state as of any past revision, or deltas between revisions"* runs on the same
projection kernels, while for DuckDB/ClickHouse it is an ETL project. R2
deliberately includes temporal-analytical kernels to press this advantage
rather than only chasing parity.

### 8.6 Post-redesign performance roadmap (sequenced, out of scope here)

**R1 — Store-level canonical dictionaries** (after P5; the G2 fix).
Design sketch: per string column, maintain a store-level canonical dictionary
as its own segment chain under the definition's sub-tree (a reserved
leafIndex range or a descriptor-addressed auxiliary segment); per-leaf DICT
segments then store canonical ids directly (or a compact leaf→canonical remap
built at encode time, when the writer already holds both). Effects:
count-distinct becomes a canonical-dictionary cardinality read (O(1) per
query); dense group-by loses the per-leaf remap entirely (ids are already
canonical); string-EQ literal resolution becomes one lookup instead of
per-leaf resolution. Maintenance: the canonical dict is append-only within a
buildRevision (interning new values at apply time is a descriptor+segment
append); rebuilds may compact it. Risks to design against: dict growth on
high-cardinality columns (cap + per-leaf fallback, mirroring today's
`CANON_DICT_INELIGIBLE` sentinel), and revision semantics (the canonical dict
is per-buildRevision, CoW-shared like any segment — time travel keeps
working). Prerequisites all land in this redesign: DICT segments, descriptor
stats, hash-based no-op writes.

**R2 — Kernel breadth** (independent track, prioritized by G1 impact):
1. High-cardinality group-by (hash aggregation over canonical ids — pairs
   with R1).
2. Top-N / order-by over projection columns (heap over zone-map-pruned
   leaves).
3. Presence/EXISTS kernel (descriptor flags + presence heads — §11-3).
4. **Temporal-analytical kernels** (revision-delta aggregates, as-of
   group-by) — the §8.5 differentiator, not parity work.
5. Joins are explicitly deprioritized: the target workload is denormalized
   document analytics; the fail-closed fallback remains correct for the rest.

**R3 — Parallel-execution quality** (opportunistic): the current parallel
driver chunks leaves statically across the common pool; morsel-style
work-stealing with per-worker hash states would smooth straggler variance on
the group-by shapes. Only worth doing after R1 changes the profile.

### 8.7 Measurable intent (success criteria per milestone)

Recorded here so P8 and the R-phases have pass/fail bars, all at 100 M on the
`COMPARISON_DUCKDB.md` protocol (and its ClickHouse sibling once run at
100 M):

| Milestone | Bar |
|---|---|
| After P5 (storage redesign complete) | No regression on any of the 9 covered shapes vs the table in §8.1; cold-reopen hydrate ≤ today's ~8 s; disk tax within a few points of 5.6 % |
| After P6 | Double-column sum/avg/min/max and filtered aggregates run on the fast path, within 2× of DuckDB's double aggregates on the same data |
| After P7 | String-heavy dataset disk tax measurably below the P5 baseline; no kernel-latency regression |
| After R1 | count-distinct ≤ 1.5× DuckDB; two-key group-by ≤ 1.5×; single-key group-by ≤ 1.5× (from 4.2× / 2.1× / 2.5×) |
| Standing invariant | Differential suite byte-identical vectorized-vs-interpreted on every shape, every milestone — speed never outruns the fail-closed correctness gates |

---

## 9. Implementation plan

Phases are ordered by dependency; P2 can proceed in parallel with P1; P6 and
P7 depend on P2 (+P3 for end-to-end tests) but not on P5. Each phase lands
green on the full existing projection suite plus its own new tests.

### P0 — Decisions + de-risking spike (small)

- **Confirm the two load-bearing decisions**: (i) side-map refs +
  descriptor-in-slot (vs refs-in-slot); (ii) segment offset identity, no
  logical page keys, integrity via descriptor `contentHash` (XXH3-64).
- **Spike**: a `HOTLeafPage` side-map ref to a prototype segment page
  survives commit → cold reopen → read-back, mirroring the OverflowPage
  chain (`HOTLeafPage.commit` override, writer branch, serializer refs
  section, fragment-merge carry). This exercises hazard 5.2-a/b before any
  projection code changes.
- Freeze wire formats: `PIXD` descriptor v1; per-segment header (magic,
  version, encoding byte); refs section layout.
- Exit: spike test green incl. a deep-split cascade; formats documented in
  this file.

### P1 — Page layer (sirix-core: `io.sirix.page`, `io.sirix.settings`, `io.sirix.access.trx.page`)

- `ProjectionSegmentPage` (new `PageKind`; OverflowPage-shaped contract —
  structural accessors throw).
- `HOTLeafPage.commit` override; `NodeStorageEngineWriter.commit` segment
  branch; `PageKind.HOT_LEAF_PAGE` refs section (sparse-emit correct);
  `mergeHOTFragmentsByKey` + `HOTLeafPage.copy` ref carry;
  `PageReference.getPage` guard.
- Tests: commit-order cold reopen (grow+shrink+patch in one commit);
  ref survival across fragment merges under SLIDING_SNAPSHOT; stale-swizzle
  regression for segment pages; ref-only-dirty slot emit.
- Exit: page layer supports reference-bearing HOT leaves with zero
  projection-specific code above it.

### P2 — Descriptor + codec segmentation (sirix-core: `io.sirix.index.projection`)

- `LeafDescriptor` ("PIXD") serializer, allocation-free positional reader.
- Per-segment `encodeSegment`/`decodeSegment` for KEYS / BODY(c) (presence
  head + flags + values) / DICT(c); assembler → raw scan form
  **byte-identical**; column-scoped provenance probes replacing the
  tail-boundary probes.
- Tests: per-segment and assembled round-trips (adversarial width sweeps
  ported); provenance survival; empty leaf (`rowCount 0`) descriptor;
  84-column cap validation.
- Exit: old storage still in place; new codec fully tested stand-alone.

### P3 — Storage rewrite (sirix-core: `ProjectionIndexHOTStorage`)

- Descriptor+segment API (§2.4-7); hash/length no-op carry-forward; live-
  empty-leaf vs tombstone; deletion of orphaned descriptors above
  `leafCount`; removal of chunk machinery (`CHUNK_SIZE`, accumulator, gap
  probes, composite chunk keys); hydrate copies segment bytes to heap;
  parallel hydrate re-tuned (5.2-g).
- Tests: reshaped `ProjectionIndexHOTStorageGrowingPayloadTest`;
  shrink-grow-shrink; empty≡absent + live-empty distinct; hydrate parity vs
  P2 assembler; `ProjectionPersistForceRebuildTest`.
- Exit: storage class green; sirix-core projection suite green end-to-end on
  the new layout.

### P4 — Builder + maintenance (sirix-core: builder, listener; sirix-query: create/drop)

- Streaming builder (one leaf in memory; fences accumulated; slot 0 last);
  wire the off-heap encode path or delete it (no more dead code).
- Maintenance: per-segment writes via re-encode + hash compare; rebuild
  share-friendliness; tombstone/ladder preserved (incl. nested-root-throws
  path, `emptyRecordSetAllowed` distinction, legacy invalidation-mode
  decision per 5.2-e).
- **Optional, measurement-gated**: per-column dirty tracking
  (`pathNodeKey → column` bitmask instead of the discarded field identity) to
  skip re-encoding untouched columns — CPU-only; land only with a benchmark
  showing recode cost matters.
- Tests: `ProjectionIndexStressTest` soak on new layout; single-row
  single-column update writes exactly one BODY (+descriptor) — measure bytes;
  delete-heavy and append-heavy soaks asserting the scoped containment of
  §2.5; multi-definition same-commit; same-commit create+update/delete;
  `MAX_INCREMENTAL_RECORDS` cliff re-measured.
- Exit: full sirix-core + sirix-query projection suites green.

### P5 — Query integration (sirix-core catalog/registry/bytescan; sirix-query executor)

- Catalog: descriptor cache sibling on `DataKey`; weigher; `PROBES` hop;
  `invalidateUnder`/`clearCache` extension; wtx bypass; segment validation +
  `NOT_USABLE` negative caching at fill.
- Handle v2: column-scoped payload access (per-column heap arrays), canonical
  dicts from DICT segments, provenance probes column-scoped, parallel-driver
  compatibility (sublist shape), `NOT_USABLE` sentinel.
- Kernel segment-scoped entry points per the needs table in §4; executor
  gates read segment truth; epoch structure per 5.2-i.
- Tests: columnar-I/O assertions **on a cold cache** (aggregate loads no
  BODY/DICT of unreferenced columns — page-load counts); catalog/wtx serving
  suites; `ProjectionIndexDescendantRootServingTest`;
  `TypedGroupByDifferentialTest`; `VectorizedServingGateTest`; the 89-case
  differential suite old-vs-new layout.
- Exit: REST + embedded serving green; steady-state benchmarks not regressed
  (heap-kernel identity preserved).

### P6 — NUMERIC_DOUBLE + ALP (independent of P5; needs P2/P3)

- Kind 3 through every switch site (§2.6); `mapType` acceptance; extractor
  exactness + `NOT_VALUE_EXACT`; transform-domain values/zone maps; ALP /
  ALP-RD / raw-64 codec branches; double aggregate kernel + `Dbl` surfacing;
  FP predicate gates bypassed for double columns.
- Tests: transform order-isomorphism property test (doubles ↔ signed longs,
  incl. ±0, denormals, extremes); ALP round-trip with exception paths;
  differential double-column queries vs generic pipeline; aggregate
  determinism across thread counts; decimal-fed `NOT_VALUE_EXACT` gating;
  NaN/Inf defensive path.
- Exit: `jn:create-projection-index` accepts `double`/`float`; benchmark vs
  DuckDB double aggregates recorded.

### P7 — FSST dictionaries (needs P2/P3; independent of P5/P6)

- `DICT(c)` FSST encoding behind existing `FSSTCompressor` gates; RAW
  fallback; deterministic-training pin for hash stability (5.2-n).
- Tests: round-trip incl. escape-heavy inputs; no-op hash stability across
  identical re-encodes; compression-ratio sanity on high-cardinality dicts;
  distinct/group-by correctness unchanged.
- Exit: measured disk-tax delta on a string-heavy dataset.

### P8 — Migration + hardening (last)

- `ProjectionIndexMetadata.VERSION = 2`; rebuild-on-open; release-note the
  no-reclamation reality (§6).
- Scale gates: 100 M-row build + disk-cold reopen (vs ~8 s / 97 k-leaf
  baseline); disk tax vs 5.6 % baseline (decide 5.2-j coalescing); trie-depth
  assertion; update-bytes measurement.
- Benchmark exit artifact: re-run the full `COMPARISON_DUCKDB.md` protocol on
  the new layout (bench flags pinned: `-H:+VectorAPISupport`, JVM build for
  projection construction), run the ClickHouse harness at 100 M for the first
  time, and check the results against the §8.7 bars; update
  `docs/COMPARISON_DUCKDB.md` with the new columns.
- Docs: add the projection on-disk format to `docs/DISK_FORMAT.md` (today it
  has **no projection section**); update `KNOWN_LIMITATIONS.md` (float
  support changes several entries), README projection section, and the class
  javadocs this doc supersedes.

### Risk register

| Risk | Phase | Mitigation |
|---|---|---|
| Sparse-emit / fragment-merge ref loss (5.2-b) | P0/P1 | spike first; dedicated emit/merge tests before projection code moves |
| Hidden dependence on whole-leaf payload shape in executor | P5 | keep `get()`-assembled raw form as the compatibility path; migrate kernels incrementally |
| Descriptor overhead exceeds disk-tax budget (5.2-j) | P3/P8 | measure at P3 with synthetic 100 M store; coalescing fallback designed but unbuilt |
| ALP encode cost on the build path | P6 | encoding-byte fallback to raw-64; ALP is per-column opt-in by data shape |
| No-op hash false sharing (hash collision carries stale segment) | P3 | 64-bit XXH3 + byteLen must both match; collision odds ~2⁻⁶⁴ per compare — document as accepted |
| Rebuild-share changes `MAX_INCREMENTAL_RECORDS` calibration | P4 | re-measure threshold after share-friendly rebuild lands |

---

## 10. Cross-cutting test matrix

Beyond per-phase tests: the full existing projection suite must stay green at
every phase boundary — `ProjectionIndexLeafCodecTest`,
`ProjectionIndexIntegralityPersistenceTest`,
`ProjectionIndexDenseCompositeTest`, `ProjectionIndexCatalogServingTest`,
`ProjectionIndexWtxServingTest`, `ProjectionIndexStressTest`,
`ProjectionIndexDescendantRootServingTest`, `ProjectionPersistForceRebuildTest`,
`ProjectionIndexHOTStorageGrowingPayloadTest`, `TypedGroupByDifferentialTest`,
`VectorizedServingGateTest` (the last three were missing from earlier drafts),
plus the path-summary suites touched by #1122.

## 11. Open questions (deliberately few)

1. **Row-group size decoupling** (scan-batch vs maintenance granularity) —
   only with evidence; 1024 stays.
2. **Segment coalescing threshold** (5.2-j) — measurement-gated at P8.
3. **Presence-only (EXISTS) predicate kernel** — new functionality enabled by
   descriptor flags + presence heads; not part of this redesign.
4. **Un-strand the CAS `ChunkDirectory`/`BitmapChunkPage` path** using the
   P1 hooks — separate effort.
5. **Per-column dirty tracking** in the listener — measurement-gated at P4.
