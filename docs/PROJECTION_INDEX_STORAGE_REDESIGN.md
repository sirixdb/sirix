# Projection Index Storage Redesign

Status: **design draft** (task #57's preferred resolution) — reviewed against the
as-built code on `main` after PR #1116 (compact codec), #1117 (IndexController
lifecycle + revision-scoped catalog), #1120 (incremental maintenance, per-leaf
fences), and #1122/#1128 (production hardening, REST serving).

This document describes the planned redesign of the projection index's
persistent storage: lifting payload bytes **out of HOT slot values** into
dedicated copy-on-write **column-segment pages**, keyed from a tiny per-leaf
segment directory. It first pins down the current (interim) layout precisely,
then derives the target layout from the design rules mature columnar systems
(Parquet, DuckDB, ClickHouse) converged on, and closes with the corner-case
catalog the redesign must preserve or newly handle.

Authoritative source for the current layout (the class javadoc is the current
documentation of record):

- `bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionIndexHOTStorage.java`
  (chunked-values contract; "Architectural path forward" section)
- `bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionIndexLeafPage.java`
  ("Known architectural debt" section, raw scan layout, presence tail)
- `bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionIndexLeafCodec.java`
  (compact persisted form)
- `bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionIndexMetadata.java`
  (slot-0 metadata payload)

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
  `NodeKind.PROJECTION_INDEX_LEAF` (byte 44) where a `DataRecord` shape is
  required.
- `ProjectionIndexPage` mirrors `CASPage`/`PathPage`/`NamePage`: a
  `ReferencesPage4 → BitmapReferencesPage → FullReferencesPage` delegate keyed
  by `IndexDef#getID()`, plus per-index bookkeeping (`maxNodeKeys`,
  `maxHotPageKeys`, `currentMaxLevelsOfIndirectPages`).
- There is deliberately **no dedicated leaf `PageKind`** today — serialized
  leaves live as opaque values in ordinary `HOTLeafPage` slots.

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
- `MAX_CHUNKS_PER_LEAF = 256` (chunkIdx occupies 8 bits) caps one leaf at 1 MB
  serialized; leafIndex effectively has 55 usable bits.
- A class-load guard rejects any `CHUNK_SIZE` whose single-entry footprint
  (2 B suffix-len + ≤ 8 B suffix + 2 B value-len + chunk) cannot fit an empty
  `HOTLeafPage` (`DEFAULT_SIZE` = 64 KB) — otherwise the trie-writer split path
  would fail deterministically.
- **Tombstones:** the HOT trie has no per-entry delete, so a shrinking re-put
  writes zero-length chunk values over the stale tail. Readers treat a missing,
  empty, or partial (`< CHUNK_SIZE`) chunk as end-of-payload.

Because each chunk is a separate HOT slot, `VersioningType#combineRecordPages`
shares unchanged chunk slots across revisions: a one-row change rewrites only
the chunk(s) whose bytes changed, at 4 KB granularity.

### 1.4 Payload encodings

Three self-describing formats, all carrying magic + version:

- **Raw scan form** (`ProjectionIndexLeafPage.serialize()`, `PIX1` presence
  tail, tail version 1): flat little-endian primitive arrays (header, kinds,
  record keys, per-column zone maps + bodies), then the mandatory presence tail
  (per-column flags with `UNREPRESENTABLE`/`NON_INTEGRAL` bits, per-column
  presence bitmaps, tailLength, version, magic). `deserialize()` rejects
  tail-less payloads as corrupt — integrality/presence provenance is never
  fabricated.
- **Compact persisted form** (`ProjectionIndexLeafCodec`, `PIXC` magic,
  version 1): delta/FOR record keys, frame-of-reference bit-packed numerics
  (widths > 56 fall back to aligned raw-64), packed dict-ids, marker-byte
  presence. Decodes **byte-identically** back to the raw form; payloads without
  the magic pass through unchanged (a raw leaf's first int is `rowCount ≤ 1024`
  and can never collide with the magic).
- **Metadata form** (`ProjectionIndexMetadata`, `PIXM` magic, version 1) as in
  §1.2.

The redesign keeps the raw scan form as the in-memory target and keeps the
compact codec's per-column encodings, but restructures the *persisted grouping*
of those encodings into per-column segments (§2.3).

### 1.5 Write and read paths (current)

- **Build:** `ProjectionIndexBuilder.buildAndPersist` walks the record set,
  emits leaves through the codec into an off-heap scratch segment, and calls
  `ProjectionIndexHOTStorage.putFromSegment` (zero heap copies), then writes
  slot 0 last with fences computed via `ProjectionIndexLeafCodec.recordKeyRange`.
- **Incremental maintenance:** `ProjectionIndexChangeListener.applyIncremental`
  reads slot-0 fences (one read, no per-leaf probes), two-pointer-merges dirty
  record keys against ascending zone maps, re-extracts each touched leaf
  entirely, appends new records at the tail (node keys are monotone), rewrites
  slot 0 with new `leafCount`/`buildRevision`/fences. Unattributable changes
  degrade to a same-commit full rebuild; a double failure writes the stale
  tombstone (corruption valve).
- **Hydrate:** `readAll` → depth-2-parallel HOT scan (per-worker
  `HOTTrieReader`, per-worker accumulator maps, merge). Because a logical
  leaf's chunks can be **dispersed across HOT sub-trees after splits** (chunk
  indexes live in the low key byte), accumulation places each chunk at its
  absolute offset (`chunkIdx * CHUNK_SIZE`) and per-task fragments are merged
  chunk-wise via fill bitmaps (`LeafChunkAccumulator`).
- **Point reads:** `readOne` (slot-0 stale probe before hydrate),
  `get`/`getChunkSlice` (zero-copy slices out of HOT leaf memory).

### 1.6 Why this is interim: the SLIDING_SNAPSHOT contract violation

`docs/ARCHITECTURE.md` promises O(1) writes per **record** under
`SLIDING_SNAPSHOT`. The projection's natural record is a ~32-byte row, but the
smallest CoW-shareable unit is a 4 KB chunk (originally the whole ~20 KB leaf).
CAS/NAME/PATH values are naturally KB-sized per record, so slot granularity
matches there; projection leaves pack ~1024 records per slot and need
**sub-slot sharing** before GA. Additionally, carrying 4 KB values in HOT slots
means one 64 KB `HOTLeafPage` holds only ~15 chunk entries, forcing deep trie
splits at scale — the source of two historic failure families (both fixed and
regression-guarded, see §5.1): grow-overwrite chunk drops and stale-swizzle
use-after-close during deep split cascades.

There is a second, quieter defect: **byte-offset chunks compose badly with
bit-packed encodings**. The compact codec's streams (delta/FOR record keys,
FOR numerics, packed dict-ids) have data-dependent lengths, so a value change
that alters a column's FOR width or grows its dictionary shifts every
downstream byte — and "rewrite only chunks whose bytes changed" degenerates
toward "rewrite every chunk from the edited column onward". Only same-width
in-place changes actually stay local today.

---

## 2. Redesign: segment directories + dedicated column-segment pages

The starting point is `ProjectionIndexHOTStorage`'s "Architectural path
forward" javadoc: lift payload bytes out of HOT slot values into their own
CoW-versioned pages, keeping HOT slots tiny. This document refines that sketch
in one important way, derived in §2.1: the unit behind each directory entry is
**not a byte-offset chunk of the serialized leaf but a semantic segment** —
the record-key column, one segment per projected column, and (for string
columns) the local dictionary:

```
HOTLeafPage                        one slot per logical projection leaf
  slot key   = PathKeySerializer.serialize(leafIndex)       // no low chunk byte
  slot value = SegmentDirectory {
                 segCount,
                 (segmentId → PageReference, byteLen, stats/flags)[]
               }
                   │  segmentId 0      = record-key segment (delta/FOR keys)
                   │  segmentId 3c+1   = column c body: encoded values
                   │                     + presence bitmap + column flags
                   │  segmentId 3c+2   = column c local dictionary
                   │                     (STRING_DICT columns only)
                   ▼
        ProjectionSegmentPage (new PageKind)
        one page per segment, CoW-versioned at standard Sirix page
        granularity — the SLIDING_SNAPSHOT unit
```

### 2.1 Why segments, not byte chunks — lessons from Parquet, DuckDB, ClickHouse

The interim layout splits the serialized leaf at arbitrary 4 KB byte
boundaries. Every mature columnar system splits on **semantic** boundaries
instead:

| | Batch unit | Column unit | Stats / pruning | Dictionaries | Validity | Updates |
|---|---|---|---|---|---|---|
| **Parquet** | row group (~128 MB) | column chunk → data pages (~1 MB) | min/max/null-count per page and per column chunk; page skipping via column/offset index | separate dictionary page per column chunk | definition levels per page | immutable; rewrite files |
| **DuckDB** | row group (122,880 rows) | per-column segments (~256 KB blocks) | per-segment zone maps; encoding picked per segment (FOR, bit-pack, dictionary, FSST for strings, ALP for floats) | per-segment | validity mask stored separately from values | MVCC update/delete vectors layered over immutable segments |
| **ClickHouse** | part → granules (8,192 rows) | one file per column (wide parts) + mark files | sparse primary index; per-granule-range skip indexes (minmax/set/bloom) | LowCardinality encoding | separate null maps | background merges; mutations rewrite whole parts |

The transferable rules, in decreasing order of impact here:

1. **Chunk on column boundaries, never byte offsets.** A byte-offset slice of
   a bit-packed stream has no independent meaning: no query can read column
   `c` without fetching every chunk of the leaf, and no writer can touch a
   value without risking the downstream byte-shift cascade (§1.6). Semantic
   segments make both directions columnar: an aggregate over column `c`
   fetches one segment per leaf; a single-field update recodes one segment
   (plus its dictionary iff it grew).
2. **Stats live above the data.** All three systems prune I/O from metadata
   (footer stats, zone maps, sparse/skip indexes) without touching data
   pages. Today the per-column min/max zone maps sit *inside* the payload —
   you pay the fetch to learn you didn't need it; only the per-leaf
   record-key fences live above (slot 0). The directory entry is the natural
   home for per-segment `byteLen`, per-column min/max, and the
   presence/unrepresentable/non-integral flag bits — predicate and aggregate
   planning can then skip segment loads outright.
3. **Dictionaries are separable.** Parquet stores the dictionary in its own
   page, distinct from the id stream. Splitting a string column's local dict
   (segment `3c+2`) from its id stream (in `3c+1`) means
   `distinctPresentStrings` — already defined as the union of per-leaf
   dictionaries — reads *only dictionary segments*, instead of hydrating
   whole leaves as it must today.
4. **Presence/validity is a first-class, tiny stream.** DuckDB's validity
   masks and Parquet's definition levels are separate structures for a
   reason: presence-only predicates should read 128 bytes per 1024 rows, not
   a column body. We bundle each column's presence bitmap at the *head* of
   its column segment (self-contained column reads) and mirror the three
   flag bits into the directory entry, so all-present/all-missing/tainted
   columns are decided without any segment load.
5. **Encodings are chosen and described per segment.** Already true of the
   compact codec's per-column branches; the segment split formalizes it —
   every segment is self-describing (magic + version, like `PIXC` today).
6. **Do NOT copy their update stories.** Parquet is immutable, ClickHouse
   mutations rewrite whole parts, DuckDB layers MVCC delta vectors over
   immutable segments because its segments are fat (256 KB). Sirix already
   has page-granular CoW with cross-revision structural sharing — at 1024-row
   leaves the segment recode is cheap and the versioning story (time travel,
   rollback-for-free) is strictly stronger. Revisit delta vectors only if
   update-heavy soaks show segment-recode churn dominating (§9).

One deliberate divergence: our "row group" stays at `MAX_ROWS = 1024` — two
orders of magnitude below DuckDB's 122,880 rows. The small group is
load-bearing for incremental maintenance (a touched leaf is re-extracted
wholesale; at 122 k rows that would be ruinous) and matches the SIMD kernels'
1024-bit mask width. The costs are finer stats granularity (harmless — it
prunes better) and per-leaf directory overhead (bounded in §5.2-j). Decoupling
the scan-batch size from the maintenance granularity is an open question (§9).

### 2.2 Building blocks that already exist

- **`io.sirix.index.hot.ChunkDirectory`** — sorted `int[] chunkIndices` +
  `PageReference[] chunkRefs`, binary-searched, growable, with `copy()` for
  CoW. Built for the CAS bitmap path but shape-agnostic: `segmentId` plays
  the `chunkIdx` role. The wire form (`ChunkDirectorySerializer`) grows a v2
  with per-entry `byteLen` + stats/flags; its explicit **tombstone
  representation (`chunkCount == 0`)** replaces today's zero-length-value
  tombstone hack.
- **`BitmapChunkPage` + `VersioningType.combineBitmapChunks` +
  `PageKind` wiring** — the template for a dedicated chunk-page kind with its
  own fragment-combine function. `ProjectionSegmentPage` follows this pattern
  but holds one opaque encoded segment (codec output) instead of bitmaps; its
  combine is last-writer-wins per whole page (a segment page is rewritten
  atomically).

### 2.3 What must be built

1. **`ProjectionSegmentPage`** — new `PageKind` entry; payload = one segment's
   bytes (length-prefixed), plus the standard page header. No internal
   structure at the page layer.
2. **Reference-bearing HOT values for `IndexType.PROJECTION`** — the critical,
   cross-cutting piece. Today HOT slot values are opaque to the commit
   machinery. With directories in slots, the commit walker, the post-commit
   reference resolver (assigning durable page keys), and the page
   reclamation/GC walk must all **descend into projection slot values** to
   reach segment-page references. Concretely: a `combineRecordPages` variant
   (or an `IndexType`-gated hook in the HOT leaf commit path) that
   deserializes each modified slot's directory, recursively commits referenced
   segment pages, then re-serializes the directory with resolved keys.
   NodeKind routing (`PROJECTION_INDEX_LEAF`) already exists and stays.
3. **Codec segmentation refactor** — `ProjectionIndexLeafCodec` already
   encodes column-by-column; the refactor moves the per-column branches
   behind per-segment `encodeSegment`/`decodeSegment` entry points (record
   keys / column body incl. presence + flags / dictionary) and adds an
   assembler that reconstructs a `ProjectionIndexLeafPage` from a leaf's
   segments. The **byte-identical raw-form round-trip guarantee is preserved
   at the leaf level**: segments → raw scan form must equal what
   `serialize()` of the original leaf produced, presence tail included.
4. **`ProjectionIndexHOTStorage` rewrite** — same role, columnar API:
   - `put(leafIndex, leaf)` encodes and writes all segments (build path);
   - `putSegment(leafIndex, segmentId, bytes)` for incremental maintenance;
   - `getSegment`/`getSegmentSlice(leafIndex, segmentId)` for kernels;
   - `get(leafIndex)` assembles the full raw form (generic pipeline);
   - keys become plain `leafIndex` (no low chunk byte); shrink = directory
     entry removal (real deletes).
5. **Segment-page key allocation** — segment pages draw from the per-index
   `maxHotPageKeys` counter in `ProjectionIndexPage` (they live in the
   sub-tree's key space) or a sibling counter; must round-trip through the
   `PROJECTIONPAGE` serializer either way.
6. **Kernel plumbing** — `ProjectionIndexByteScan` kernels currently take a
   whole hydrated leaf; the fast paths gain segment-scoped entry points
   (aggregate: keys-fence check via directory + one column segment;
   count-distinct: dictionary segments only; presence predicates: directory
   flags, falling back to the presence bitmap at the segment head).

### 2.4 What this buys

- **HOT slots shrink from ~4 KB to tens of bytes** (directory with 4–10
  entries incl. stats). One 64 KB `HOTLeafPage` then holds hundreds-to-
  thousands of logical leaves instead of a handful (~15 four-KB chunk entries
  fit per page today) — the trie gets shallower by orders of magnitude, deep
  split cascades (the breeding ground of both historic failure families)
  become rare, and the scale ceiling moves well past 100 M records.
- **Columnar I/O, finally.** An aggregate over one column of a 3-column
  projection loads ⅓ (or less) of the bytes it loads today; count-distinct
  loads only dictionary segments; presence checks and fence pruning load
  nothing but directories. The compact codec made the *bytes* columnar; this
  makes the *I/O* columnar.
- **True SLIDING_SNAPSHOT alignment.** A one-row, one-column change rewrites
  exactly one column segment page (plus its dictionary segment iff the dict
  grew) and the small directory slot. Untouched segments are shared by
  reference across revisions. The byte-shift cascade (§1.6) is contained
  inside a single column by construction.
- **Real deletes** replace zero-length tombstones (directory entry removal;
  empty directory = leaf tombstone, already representable in the serializer).
- **Chunk dispersal disappears**: one slot per leaf means a leaf can never
  straddle HOT sub-trees. `LeafChunkAccumulator`'s cross-task fragment merge
  and absolute-offset assembly become dead code; parallel hydrate partitions
  cleanly by directory.
- **The `CHUNK_SIZE` knob disappears.** Segment size is organic (the encoded
  column body — typically a few hundred bytes to ~8 KB per 1024 rows, since
  the whole compact leaf averages ~2.4 KB at the 100 M benchmark). The 1 MB /
  256-chunk leaf cap lifts; a per-segment sanity bound (e.g. 1 MB) replaces
  it as policy, not encoding.

---

## 3. Write path (redesigned)

```
put(leafIndex, leaf):
  dir = readDirectory(leafIndex)                 // null → new SegmentDirectory
  writeSegment(dir, KEYS,      encodeKeys(leaf))
  for c in 0..columnCount-1:
    writeSegment(dir, BODY(c), encodeColumnBody(leaf, c))   // presence + values
    if kind(c) == STRING_DICT:
      writeSegment(dir, DICT(c), encodeDictionary(leaf, c))
  dropSegmentsNotWritten(dir)                    // shrink = real delete
  refreshDirectoryStats(dir)                     // byteLen, min/max, flag bits
  writeDirectorySlot(leafIndex, dir)             // tiny value; split-safe

writeSegment: no-op (share prior page by reference) when bytes are identical
              to the previous revision's segment — the common case for
              untouched columns under incremental maintenance.
```

- `putSegment(leafIndex, segmentId, bytes)` (incremental maintenance) touches
  one segment page; the directory slot is rewritten in the same commit (it is
  tiny, so this is cheap), keeping stats and refs consistent atomically.
- Directory slot writes go through the existing loud `updateOrSplitInsert`
  machinery; values are tiny so the split path is exercised rarely but must
  remain correct (regression suite retained).
- The metadata payload at slot 0 (leafIndex 0) uses the identical mechanism
  as a single-segment directory; the stale-tombstone write becomes a
  one-segment directory replacement.

## 4. Read path (redesigned)

- **Directory read**: one trie descent; parse positionally (allocation-free)
  from the slot slice. Decisions available without any segment load: leaf
  fences, per-column min/max, all-present/all-missing, tainted
  (unrepresentable / non-integral) columns, per-segment lengths.
- **Kernel reads**: load exactly the segments the kernel needs —
  `BODY(c)` for a predicate/aggregate on column `c`, `DICT(c)` for
  count-distinct, `KEYS` only when record attribution or fence-splitting is
  required. Zero-copy slices come from segment-page memory; **lifetime is
  tied to the segment page's residency** — consumers keep the "don't retain
  past the call" rule.
- **Full hydrate** (generic pipeline / wtx serving): load all segments,
  assemble the raw scan form byte-identically (§2.3-3).
- `readAll`: range-scan directories (tiny values → few HOT pages → fast),
  then fetch segment pages, parallelizing over leaves. Per-leaf work is
  independent; no fragment merging. See corner case 5.2-g for the fan-out
  shift.
- `readOne` (slot-0 stale probe): one descent + one segment load.

---

## 5. Corner-case catalog

This section is the review checklist for the implementation PR. §5.1 lists
behaviors the current code guarantees that must survive the redesign
verbatim; §5.2 lists hazards the redesign newly introduces.

### 5.1 Invariants to preserve (each has an existing guard or test)

1. **Shrink-on-rewrite must not resurrect stale bytes.** Today: tail-chunk
   tombstoning + `LeafChunkAccumulator` truncation at the lowest tombstone.
   Redesign: removing directory entries must be transactional with the
   segment rewrite — a directory that still lists a removed segment after
   crash recovery would serve garbage. Covered by CoW (directory + segments
   commit atomically via the revision root), but the commit-walker work in
   §2.3(2) is exactly where this can silently break. Port
   `ProjectionIndexHOTStorageGrowingPayloadTest` (reshaped: fewer/more
   columns across rebuilds) and add a shrink-grow-shrink cycle test.
2. **Grow-overwrite must be loud or lossless.** The historic bug:
   re-persisting a larger payload silently dropped chunks that no longer fit.
   In the redesign, segment bytes never compete for HOT page space, so the
   failure mode shifts to the *directory* growing (more entries, e.g. a
   6-column force-rebuild over a 3-column store) on a full HOT page — which
   must take the split path, never drop. Keep the "fail loudly, never drop a
   write" contract of `updateOrSplitInsert`.
3. **Stale-swizzle use-after-close.** Fixed by treating a closed `HOTLeafPage`
   as a cache miss in `PageReference.getPage()`. The redesign adds a second
   page type resolved through `PageReference`s **stored inside slot values** —
   the same guard must apply to `ProjectionSegmentPage` (a closed segment page
   re-resolves via its logKey/key), and the regression test must cover deep
   split cascades touching directories.
4. **Empty payload ≡ absent leaf.** Today `put(leafIndex, byte[0])` writes one
   zero-length chunk (a tombstone), and `get` returns `null` — empty and
   absent are indistinguishable. The redesign's empty directory encodes the
   same thing explicitly. Do not accidentally make `get` return `byte[0]`;
   callers (catalog truncation check, stale probe) distinguish `null` only.
5. **Payload lengths are explicit and must agree.** Today the reader infers
   end-of-payload from a partial or missing chunk (with a subtle corner at
   exact multiples of `CHUNK_SIZE`). In the redesign every segment's length
   exists twice: the directory entry's `byteLen` and the segment page's own
   length prefix. Treat disagreement as corruption (fail soft to the generic
   pipeline, like the truncated-store check) — never trust one silently over
   the other.
6. **Metadata is bounds authority, not slot presence.** Reads are bounded by
   slot-0 `leafCount`; leftover higher slots from a shrinking rebuild are
   ignored, and the truncated-store check (`persisted.size() < leafCount + 1`)
   fails soft to the generic pipeline. Unchanged semantics — but the redesign
   should now *actually delete* orphaned leaf directories above `leafCount`
   during rebuild, since real deletes exist (space hygiene; must not be
   load-bearing for correctness).
7. **Provenance never fabricated.** The presence-tail validation and the
   fail-closed integrality/sparse probes stay: a leaf whose segments cannot
   be assembled into a tail-valid raw form is corrupt, and the flag bits
   mirrored into the directory (§2.1 rule 2) are a *cache* of the segment
   truth, never a substitute — the probes must keep reading the persisted
   segment bytes, not the mirror (see 5.2-k). Byte-identity round-trip tests
   run end-to-end against segmented storage, including a leaf whose string
   column carries a multi-KB dictionary.
8. **Rollback/isolation for free.** Uncommitted builds, incremental patches,
   and stale tombstones ride the transaction log and vanish on rollback;
   time-travel readers see only their revision's sub-tree. The redesign must
   keep segment pages inside the same log/CoW discipline — any path that
   writes a segment page outside the transaction log breaks both isolation
   and rollback.
9. **Dropped + recreated definitions.** Drop writes a stale tombstone over
   slot 0 precisely so an id-reusing re-creation can't adopt leftover columns;
   `invalidateUnder` clears caches on database/resource removal. Unchanged,
   but add a test: drop → recreate with *fewer* leaves/columns must not read
   the old definition's segment pages via a stale directory (guard 5.1-6
   covers it if rebuild deletes orphans; the test pins it).
10. **Misconfiguration fails fast.** The class-load `CHUNK_SIZE` capacity
    guard is obsolete (the knob is gone), but its replacement is needed: the
    worst-case *directory entry* footprint — `3 × columnCount + 1` entries at
    the configured stats width — must fit an empty HOT leaf page. Validate at
    index-creation time against the definition's column count and fail with a
    clear message.
11. **Leaf-index contiguity.** The gap-probe discovery paths
    (`readAllViaProbe`, `readAllParallel`) exit after 16 consecutive absent
    leaf indexes. Builder and maintenance allocate contiguously, and metadata
    `leafCount` bounds catalog reads, so this holds — but it is an implicit
    invariant. The redesign's range-scan over directories removes the
    dependence; delete the gap-exit paths or document the invariant where it
    remains.

### 5.2 New hazards introduced by the redesign

a. **Commit-order dependency (the central risk).** Directories persist
   `PageReference`s whose durable page keys are assigned when the referenced
   page is flushed. The commit walk must flush segment pages *before*
   serializing the directory that names them (same discipline as indirect
   pages → leaf pages). A directory serialized with an unresolved reference
   (logKey only) is corruption on re-open. This is why §2.3(2) is a
   `combineRecordPages`-level change, not a storage-class-local one. Test:
   cold re-open after a commit that grew, shrank, and patched leaves in one
   transaction.
b. **GC / page reclamation must see segment refs.** Any reachability walk
   that frees unreferenced pages must descend into projection slot values, or
   old segment pages get reclaimed while historical revisions still reference
   them (time-travel reads garbage) — or conversely never get reclaimed
   (leak). Enumerate every reachability walk in the page layer and extend
   each.
c. **Directory parsing on the read hot path.** Today `getChunkSlice` returns
   HOT slot memory directly. With directories, every point read first parses
   a directory (now carrying stats too). Keep the parse allocation-free (read
   ints/longs positionally from the slot slice; do not materialize
   `ChunkDirectory` objects on the scan path) and cache decoded directories
   per (resource, defId, buildRevision) alongside the existing decoded-leaf
   cache.
d. **Segment-page cache pressure and pinning.** Zero-copy slices now pin
   segment pages. A scan holding slices across many leaves pins many small
   pages; ensure the buffer manager's pin accounting covers the new kind and
   that segment-slice consumers can't extend lifetimes accidentally (same
   rule as today, new enforcement point).
e. **Slot-0 metadata races through a smaller funnel.** The stale-tombstone
   corruption valve and the incremental rewrite both target slot 0. Today
   they overwrite chunk values; with directories they swap references. Verify
   the valve still produces a payload that `ProjectionIndexMetadata.parse`
   reads as stale even if the *previous* metadata spanned multiple segments
   (tombstone directory must fully replace, not merge with, the old one).
f. **Mixed-format stores.** A store written by the current layout has
   composite keys (`leafIndex << 8 | chunkIdx`); the redesign writes plain
   `leafIndex` keys with directory values. The two are not distinguishable by
   key shape alone at the HOT layer (both are sign-flipped 8-byte keys).
   Detection must be value-level: directory values start with the
   `ChunkDirectorySerializer` header; today's chunk values start with
   arbitrary codec bytes. Do **not** rely on sniffing — see §6 for the
   explicit migration gate.
g. **Parallel hydrate fan-out shifts.** Removing the low chunk byte
   compresses the key space by 256×, lengthening shared prefixes; the depth-2
   fan-out heuristic (`DEPTH2_MIN_FANOUT`) was tuned on the old shape and the
   trie is now far shallower. Re-measure; the serial fallback may become the
   common case at small scale (fine), and at large scale the parallelism
   should move from trie sub-trees to batched segment-page fetches.
h. **`maxHotPageKeys` bookkeeping.** Segment pages consume page keys from the
   per-index counter; the counter round-trips through the `PROJECTIONPAGE`
   serializer. A missed counter update after crash-recovery replay would
   double-allocate a page key. Add a re-open test asserting monotone key
   allocation across a crash-simulated (log-replayed) commit.
i. **Concurrent readers during maintenance.** Unchanged in principle (readers
   bind a committed revision), but the wtx-visible serving path
   (`maintenanceEpoch` revalidation) now must invalidate cached *directories*
   as well as decoded segments — a stale directory pointing at pre-patch
   segment pages would serve a torn leaf if only the leaf cache is
   epoch-gated. Key both caches on the same epoch.
j. **Small-segment fragmentation and directory overhead.** A typical
   3-column, 1024-row compact leaf (~2.4 KB total) becomes 4–5 segments of a
   few hundred bytes each: per-page headers plus a ~30–50 B directory entry
   per segment. Sirix's log-structured store writes variable-size pages, so
   there is no block-padding penalty, but the fixed per-page cost is real —
   measure the on-disk tax against the 5.6 % baseline from PR #1116 and, if
   it exceeds a few percent, coalesce sub-threshold segments (e.g. < 256 B)
   into a shared "cold bundle" page per leaf. Coalescing trades back some
   update containment (a bundle rewrite touches all bundled columns) — make
   the threshold a measured decision, not a guess.
k. **Stats duplication (directory mirror vs segment truth).** Per-column
   min/max and flag bits now exist in the directory entry *and* inside the
   segment bytes. Single-writer discipline (both written by the same
   `put`/`putSegment` commit) keeps them consistent, but every consumer must
   pick one authority per decision: pruning may use the mirror; provenance
   gates (integrality, unrepresentable) must read segment truth (5.1-7). A
   divergence found at read time is corruption → fail soft to the generic
   pipeline.
l. **Dictionary/id co-consistency.** Splitting `DICT(c)` from `BODY(c)` means
   an id stream is meaningful only against exactly the dictionary generation
   it was encoded with. Within one revision CoW guarantees the pair matches;
   the hazard is *caches* — a decoded-dictionary cache entry surviving a
   commit that re-encoded the body (or vice versa) yields wrong strings, not
   an error. Cache dict and body under one key (leaf + buildRevision/epoch),
   never separately evictable halves.

### 5.3 Corner cases that disappear

- The **byte-shift cascade** (§1.6): a width/dictionary change in one column
  can no longer dirty the persisted bytes of other columns — the cascade is
  contained inside one segment by construction.
- Chunk dispersal across HOT sub-trees (and `LeafChunkAccumulator`'s
  fragment merge, fill bitmaps, absolute-offset assembly).
- Zero-length-value tombstone semantics and tail tombstone probing loops.
- The 256-chunk / 1 MB leaf cap as an encoding limit, and the `CHUNK_SIZE`
  knob with its class-load capacity guard.
- The exact-multiple-of-`CHUNK_SIZE` end-of-payload ambiguity (lengths are
  explicit; see 5.1-5).
- Cursor-order divergence handling for long-common-prefix composite keys
  (`readAllViaCursor`'s "leaves 0-59, then 128+, then 64-127" caveat) — the
  cursor's known entry-dropping issue on large trees still argues for keeping
  the probe fallback until fixed, but the trees get ~256× smaller.

---

## 6. Migration

Precedent: the `PIXC`/`PIX1` consolidation shipped as a deliberate break
("no deployed databases", rebuild via `-Dsirix.projection.forceRebuild=true`),
and unknown metadata versions already degrade to rebuild-on-first-use.

Plan:

1. Bump `ProjectionIndexMetadata.VERSION` to 2. Version-2 metadata asserts
   "this sub-tree uses segment-directory storage."
2. On open, version-1 (or missing) metadata → the catalog treats the
   definition as absent-with-definition, which already triggers an automatic
   rebuild through the always-maintained contract (or the user forces it).
   The rebuild recreates the sub-tree from scratch in the new layout; the old
   sub-tree's pages become unreferenced in the new revision and age out with
   their revisions.
3. No dual-format read path, no value sniffing (see 5.2-f). The projection
   feature is still labeled experimental; the rebuild cost is bounded by index
   size and matches the existing degradation ladder.

---

## 7. Alternatives considered

- **Per-row HOT slots** (1024 slots/leaf): exact SLIDING_SNAPSHOT match, but
  destroys the columnar scan layout and multiplies key overhead by 1024;
  rejected.
- **Per-column HOT slots** (column bodies directly as HOT slot values, no
  directory): fixes update containment but keeps multi-KB values in the trie,
  so the fan-out/deep-split problem and both historic failure families
  remain. *Note: an earlier draft of this document rejected "per-column
  chunks" wholesale on this argument — that conflated per-column **slot
  values** (still wrong) with per-column **segment pages behind a directory**
  (adopted, §2). The directory indirection is precisely what removes the
  fan-out objection to columnar chunking.*
- **Byte-offset chunk pages behind a directory** (the original
  `ProjectionIndexHOTStorage` javadoc sketch): fixes fan-out and scale but
  keeps reads non-columnar and leaves the byte-shift cascade (§1.6) intact.
  Subsumed by semantic segments, which cost the same machinery.
- **Overflow-record spill** (`OverflowPage` referenced from the slot): closest
  existing mechanism, but it spills *whole values* — a one-row change still
  rewrites the whole overflow payload, so it fixes fan-out without fixing
  share granularity. Subsumed likewise.

## 8. Test plan

- Port and re-run the entire existing projection storage suite against the new
  layout: codec round-trips (incl. adversarial width sweeps),
  `ProjectionIndexIntegralityPersistenceTest`,
  `ProjectionIndexHOTStorageGrowingPayloadTest` (reshaped per 5.1-1/2),
  `ProjectionPersistForceRebuildTest`, catalog serving/wtx serving tests, and
  the `ProjectionIndexStressTest` soak (375 ops / 15 commits oracle, time
  travel over every revision, cold re-open, concurrent readers).
- New targeted tests, one per §5.2 hazard: commit-order cold re-open (a),
  historical-revision read after many rebuilds + a reclamation pass (b),
  epoch-gated directory cache under wtx serving (i), crash-replay key
  allocation (h), multi-segment metadata tombstone replacement (e),
  dict/body cache coherence across an incremental commit (l), directory
  stats vs segment truth divergence handling (k).
- Differential test: build the same data set under old and new layout in two
  stores; assert identical *assembled raw-form* payload bytes and identical
  query results across the 89-case differential suite.
- Columnar-I/O assertions: an aggregate over one column of a 3-column
  projection must load no `BODY`/`DICT` segments of other columns (count
  page loads); count-distinct must load only `DICT` segments.
- Scale gate: 100 M-record build + disk-cold reopen; assert trie depth and
  hydrate time improve (the ~8 s / 97 k-leaf baseline from PR #1116), that
  the on-disk tax stays within a few percent of the 5.6 % baseline
  (5.2-j), and that a single-row single-column update commit writes O(1)
  segment pages (measure bytes written).

## 9. Open questions

1. **Row-group size.** 1024 rows is right for maintenance granularity and the
   SIMD mask width, but scan-side batch efficiency and stats overhead might
   favor grouping several leaves per directory read at very large scales.
   Decouple only with evidence — DuckDB-scale row groups are off the table
   while "re-extract the touched leaf" is the maintenance primitive.
2. **Segment coalescing threshold** (5.2-j): measure first; if needed, bundle
   sub-256 B segments per leaf and accept the coarser update containment for
   those columns.
3. **Float columns.** `xs:float`/`xs:double` are currently rejected because
   numeric columns store 64-bit longs with integrality provenance. DuckDB's
   ALP shows lossless float compression is practical — a `NUMERIC_DOUBLE`
   column kind with an ALP-encoded segment would close the gap without
   touching the integrality machinery for longs.
4. **String compression.** Local dictionaries cover repetitive strings; FSST
   (DuckDB) would additionally compress *within* dictionary entries for
   high-cardinality columns. Segment-local, so it slots into `DICT(c)`
   encoding without format ripple.
5. **Does the commit-walk hook generalize?** CAS's `ChunkDirectory`/
   `BitmapChunkPage` path wants the same reference-bearing-slot-value
   treatment; building the hook `IndexType`-agnostically would un-strand that
   machinery too.
6. **Reclamation policy** for orphaned leaf directories above `leafCount`
   (5.1-6): delete eagerly at rebuild, or leave to revision aging? Eager is
   proposed; confirm it doesn't complicate the rollback story (deletes must
   ride the log like writes).
