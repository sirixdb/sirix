# Projection Index Storage Redesign

Status: **design draft** (task #57's preferred resolution) — reviewed against the
as-built code on `main` after PR #1116 (compact codec), #1117 (IndexController
lifecycle + revision-scoped catalog), #1120 (incremental maintenance, per-leaf
fences), and #1122/#1128 (production hardening, REST serving).

This document describes the planned redesign of the projection index's
persistent storage: lifting serialized leaf bytes **out of HOT slot values**
into dedicated copy-on-write chunk pages, keyed from a tiny per-leaf
`ChunkDirectory` value. It first pins down the current (interim) layout
precisely, then the target layout, and closes with the corner-case catalog the
redesign must preserve or newly handle.

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

### 1.4 Payload encodings (unchanged by this redesign)

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

The redesign changes **where chunk bytes live**, not these encodings.

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

---

## 2. Redesign: chunk directories + dedicated chunk pages

The correct design — sketched in `ProjectionIndexHOTStorage`'s "Architectural
path forward" javadoc — lifts chunk bytes out of HOT slot values and into their
own pages, keeping HOT slots tiny:

```
HOTLeafPage                       one slot per logical projection leaf
  slot key   = PathKeySerializer.serialize(leafIndex)      // no chunkIdx byte
  slot value = ChunkDirectory { chunkCount, (chunkIdx → PageReference)[] }
                                        │
                                        ▼
                          ProjectionChunkPage (new PageKind)
                          one per chunk, CoW-versioned at standard
                          Sirix page granularity — exactly matches
                          the SLIDING_SNAPSHOT unit
```

### 2.1 Building blocks that already exist

- **`io.sirix.index.hot.ChunkDirectory`** — sorted `int[] chunkIndices` +
  `PageReference[] chunkRefs`, binary-searched, growable, with `copy()` for
  CoW. Built for the CAS bitmap path but shape-agnostic.
- **`ChunkDirectorySerializer`** — length-prefixed wire form incl. an explicit
  **tombstone representation (`chunkCount == 0`)**, replacing today's
  zero-length-value tombstone hack.
- **`BitmapChunkPage` + `VersioningType.combineBitmapChunks` +
  `PageKind` wiring** — the template for a dedicated chunk page kind with its
  own fragment-combine function. `ProjectionChunkPage` follows this pattern but
  holds opaque chunk bytes (the codec output) instead of bitmaps; its combine
  is last-writer-wins per whole page (a chunk page is rewritten atomically).

### 2.2 What must be built

1. **`ProjectionChunkPage`** — new `PageKind` entry; payload = one chunk's
   bytes (length-prefixed), plus the standard page header. Serialization is
   trivial; no internal structure.
2. **Reference-bearing HOT values for `IndexType.PROJECTION`** — this is the
   critical, cross-cutting piece. Today HOT slot values are opaque to the
   commit machinery. With directories in slots, the commit walker, the
   post-commit reference resolver (assigning durable page keys), and the page
   reclamation/GC walk must all **descend into projection slot values** to
   reach chunk-page references. Concretely: a `combineRecordPages` variant (or
   an `IndexType`-gated hook in the HOT leaf commit path) that
   deserializes each modified slot's directory, recursively commits referenced
   chunk pages, then re-serializes the directory with resolved keys. NodeKind
   routing (`PROJECTION_INDEX_LEAF`) already exists and stays.
3. **`ProjectionIndexHOTStorage` rewrite** — same public API (`put`,
   `putFromSegment`, `putChunk`, `get`, `getChunk*`, `forEachChunk`,
   `readAll*`, `readOne`), new internals:
   - `put`: split payload, allocate/CoW chunk pages, write/update the
     directory slot. Shrink = remove directory entries (real deletes).
   - `get`: one trie descent to the directory, then `chunkCount` direct page
     loads (no per-chunk trie descents).
   - keys become plain `leafIndex` (no low chunk byte).
4. **Chunk-page key allocation** — chunk pages draw from the same per-index
   `maxHotPageKeys` counter in `ProjectionIndexPage` (they live in the
   sub-tree's key space) or a sibling counter; must round-trip through the
   `PROJECTIONPAGE` serializer either way.

### 2.3 What this buys

- **HOT slots shrink from ~4 KB to ~16–40 bytes** (directory with 1–5
  entries). One 64 KB `HOTLeafPage` then holds thousands of logical leaves
  instead of a handful (~15 four-KB chunk entries fit per page today) — the
  trie gets shallower by orders of magnitude, deep split
  cascades (the breeding ground of both historic failure families) become
  rare, and the scale ceiling moves well past 100 M records.
- **True SLIDING_SNAPSHOT alignment**: a one-row change rewrites exactly one
  chunk page + one small directory slot. Untouched chunk pages are shared by
  reference across revisions — no byte copying at all (today unchanged chunks
  are shared but still live inline in re-merged HOT leaf fragments).
- **Real deletes** replace zero-length tombstones (directory entry removal;
  empty directory = leaf tombstone, already representable in the serializer).
- **Chunk dispersal disappears**: one slot per leaf means a leaf can never
  straddle HOT sub-trees. `LeafChunkAccumulator`'s cross-task fragment merge
  and the absolute-offset assembly become dead code; parallel hydrate
  partitions cleanly by directory.
- **The 1 MB / 256-chunk leaf cap lifts** (directory chunk indices are ints);
  keep a sanity bound, but it is a policy choice, not an encoding limit.
- **`CHUNK_SIZE` becomes tunable upward** without hurting trie fan-out
  (chunk bytes no longer sit in slots); the CoW share granularity is the only
  remaining trade-off. Note the compact codec makes the *typical* 3-column
  1024-row leaf ~2–3 KB total (100 M rows → ~230 MB / ~97 k leaves), i.e.
  single-chunk; multi-chunk leaves are the wide/raw-passthrough case.

---

## 3. Write path (redesigned)

```
put(leafIndex, payload):
  dir = readDirectory(leafIndex)            // null → new ChunkDirectory()
  for chunkIdx in 0..ceil(len/CHUNK_SIZE):
    ref = dir.getOrCreateChunkRef(chunkIdx)
    page = cowChunkPage(ref)                // fresh page or CoW of prior
    page.setBytes(payload[chunkIdx*CS ...]) // full-chunk rewrite, atomic
  removeEntriesAbove(dir, lastChunkIdx)     // shrink = real delete
  writeDirectorySlot(leafIndex, dir)        // small value; split-safe
```

- `putChunk(leafIndex, chunkIdx, bytes)` (incremental maintenance) touches one
  chunk page + the directory slot only if the ref changed (it always does under
  CoW — the directory slot is rewritten each commit that touches the leaf; it
  is tiny, so this is cheap).
- Directory slot writes go through the existing loud
  `updateOrSplitInsert` machinery; because values are tiny, the split path is
  exercised rarely but must remain correct (regression suite retained).
- The metadata payload at slot 0 uses the identical mechanism (it is just
  leafIndex 0); the stale-tombstone write becomes a one-chunk directory update.

## 4. Read path (redesigned)

- `get`: descend once to the directory, load chunk pages in index order,
  concatenate. Absent slot or empty directory → `null` (same contract).
- `getChunkSlice`/`forEachChunk`: zero-copy slices now come from chunk-page
  memory; **lifetime is tied to the chunk page's residency**, not the HOT
  leaf's — consumers keep the "don't retain past the call" rule.
- `readAll`: range-scan directories (tiny values → few HOT pages → fast), then
  fetch chunk pages, parallelizing over leaves. Per-leaf work is independent;
  no fragment merging. Depth-2 parallel partitioning survives unchanged, but
  note the key-space shape shifts (see corner case 5.2-g).
- `readOne` (slot-0 stale probe): one descent + typically one chunk page load.

---

## 5. Corner-case catalog

This section is the review checklist for the implementation PR. §5.1 lists
behaviors the current code guarantees that must survive the redesign
verbatim; §5.2 lists hazards the redesign newly introduces.

### 5.1 Invariants to preserve (each has an existing guard or test)

1. **Shrink-on-rewrite must not resurrect stale bytes.** Today: tail-chunk
   tombstoning + `LeafChunkAccumulator` truncation at the lowest tombstone.
   Redesign: removing directory entries must be transactional with the chunk
   rewrite — a directory that still lists a removed chunk after crash recovery
   would concatenate garbage. Covered by CoW (directory + chunks commit
   atomically via the revision root), but the commit-walker work in §2.2(2) is
   exactly where this can silently break. Port
   `ProjectionIndexHOTStorageGrowingPayloadTest` and add a shrink-grow-shrink
   cycle test.
2. **Grow-overwrite must be loud or lossless.** The historic bug: re-persisting
   a larger payload silently dropped chunks that no longer fit. In the
   redesign, chunk bytes never compete for HOT page space, so the failure mode
   shifts to the *directory* growing (more entries) on a full HOT page — which
   must take the split path, never drop. Keep the "fail loudly, never drop a
   write" contract of `updateOrSplitInsert`.
3. **Stale-swizzle use-after-close.** Fixed by treating a closed `HOTLeafPage`
   as a cache miss in `PageReference.getPage()`. The redesign adds a second
   page type resolved through `PageReference`s **stored inside slot values** —
   the same guard must apply to `ProjectionChunkPage` (a closed chunk page
   re-resolves via its logKey/key), and the regression test must cover deep
   split cascades touching directories.
4. **Empty payload ≡ absent leaf.** Today `put(leafIndex, byte[0])` writes one
   zero-length chunk (a tombstone), and `get` returns `null` — empty and
   absent are indistinguishable. The redesign's empty directory encodes the
   same thing explicitly. Do not accidentally make `get` return `byte[0]`;
   callers (catalog truncation check, stale probe) distinguish `null` only.
5. **Exact-multiple payloads.** A payload of exactly `k * CHUNK_SIZE` bytes has
   no partial final chunk; today the reader detects the end by the missing
   chunk `k` probe. With a directory, `chunkCount` is explicit — but the final
   chunk's *byte length* must still be stored per chunk page (the directory
   alone doesn't know total payload length). Length-prefix each chunk page.
6. **Metadata is bounds authority, not slot presence.** Reads are bounded by
   slot-0 `leafCount`; leftover higher slots from a shrinking rebuild are
   ignored, and the truncated-store check (`persisted.size() < leafCount + 1`)
   fails soft to the generic pipeline. Unchanged semantics — but the redesign
   should now *actually delete* orphaned leaf directories above `leafCount`
   during rebuild, since real deletes exist (space hygiene; must not be
   load-bearing for correctness).
7. **Provenance never fabricated.** The presence tail (`PIX1`) validation and
   the fail-closed integrality/sparse probes are codec-level and untouched —
   but hydrate must keep delivering *byte-identical* payloads. The
   byte-identity round-trip tests (`ProjectionIndexLeafCodecTest`) must run
   against the new storage end-to-end, including a leaf whose payload spans
   multiple chunk pages.
8. **Rollback/isolation for free.** Uncommitted builds, incremental patches,
   and stale tombstones ride the transaction log and vanish on rollback;
   time-travel readers see only their revision's sub-tree. The redesign must
   keep chunk pages inside the same log/CoW discipline — any path that writes
   a chunk page outside the transaction log breaks both isolation and
   rollback.
9. **Dropped + recreated definitions.** Drop writes a stale tombstone over
   slot 0 precisely so an id-reusing re-creation can't adopt leftover columns;
   `invalidateUnder` clears caches on database/resource removal. Unchanged,
   but add a test: drop → recreate with *fewer* leaves must not read the old
   definition's chunk pages via a stale directory (guard 5.1-6 covers it if
   rebuild deletes orphans; the test pins it).
10. **Misconfiguration fails fast.** The class-load `CHUNK_SIZE` capacity guard
    becomes vacuous for chunk bytes (they no longer live in HOT slots) but a
    new guard is needed: a *directory entry* footprint bound (worst-case
    `chunkCount` at the configured `CHUNK_SIZE` and max leaf size) must fit an
    empty HOT leaf page.
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
   page is flushed. The commit walk must flush chunk pages *before*
   serializing the directory that names them (same discipline as indirect
   pages → leaf pages). A directory serialized with an unresolved reference
   (logKey only) is corruption on re-open. This is why §2.2(2) is a
   `combineRecordPages`-level change, not a storage-class-local one. Test:
   cold re-open after a commit that grew, shrank, and patched leaves in one
   transaction.
b. **GC / page reclamation must see chunk refs.** Any reachability walk that
   frees unreferenced pages must descend into projection slot values, or old
   chunk pages get reclaimed while historical revisions still reference them
   (time-travel reads garbage) — or conversely never get reclaimed (leak).
   Enumerate every reachability walk in the page layer and extend each.
c. **Directory deserialization on the read hot path.** Today `getChunkSlice`
   returns HOT slot memory directly. With directories, every point read first
   parses a directory. Keep the parse allocation-free (read ints/longs
   positionally from the slot slice; do not materialize `ChunkDirectory`
   objects on the scan path) and cache decoded directories per
   (resource, defId, buildRevision) alongside the existing decoded-leaf cache.
d. **Chunk-page cache pressure and pinning.** Zero-copy slices now pin chunk
   pages. A scan holding slices across many leaves pins many small pages;
   ensure the buffer manager's pin accounting covers the new kind and that
   `forEachChunk` consumers can't extend lifetimes accidentally (same rule,
   new enforcement point).
e. **Slot-0 metadata races through a smaller funnel.** The stale-tombstone
   corruption valve and the incremental rewrite both target slot 0. Today they
   overwrite chunk values; with directories they swap references. Verify the
   valve still produces a payload that `ProjectionIndexMetadata.parse` reads
   as stale even if the *previous* metadata spanned multiple chunks (tombstone
   directory must fully replace, not merge with, the old one).
f. **Mixed-format stores.** A store written by the current layout has
   composite keys (`leafIndex << 8 | chunkIdx`); the redesign writes plain
   `leafIndex` keys with directory values. The two are not distinguishable by
   key shape alone at the HOT layer (both are sign-flipped 8-byte keys).
   Detection must be value-level: directory values start with the
   `ChunkDirectorySerializer` header; today's chunk values start with
   arbitrary codec bytes. Do **not** rely on sniffing — see §6 for the
   explicit migration gate.
g. **Parallel hydrate fan-out shifts.** Removing the low chunk byte compresses
   the key space by 256×, lengthening shared prefixes; the depth-2 fan-out
   heuristic (`DEPTH2_MIN_FANOUT`) was tuned on the old shape and the trie is
   now far shallower. Re-measure; the serial fallback may become the common
   case at small scale (fine), and partitioning may need to move to
   chunk-page fetches instead of trie sub-trees at large scale.
h. **`maxHotPageKeys` bookkeeping.** Chunk pages consume page keys from the
   per-index counter; the counter round-trips through the `PROJECTIONPAGE`
   serializer. A missed counter update after crash-recovery replay would
   double-allocate a page key. Add a re-open test asserting monotone key
   allocation across a crash-simulated (log-replayed) commit.
i. **Concurrent readers during maintenance.** Unchanged in principle
   (readers bind a committed revision), but the wtx-visible serving path
   (`maintenanceEpoch` revalidation) now must invalidate cached *directories*
   as well as decoded leaves — a stale directory pointing at pre-patch chunk
   pages would serve a torn leaf (old chunk 0, new chunk 1) if only the leaf
   cache is epoch-gated. Key both caches on the same epoch.

### 5.3 Corner cases that disappear

- Chunk dispersal across HOT sub-trees (and `LeafChunkAccumulator`'s
  fragment merge, fill bitmaps, absolute-offset assembly).
- Zero-length-value tombstone semantics and tail tombstone probing loops.
- The 256-chunk / 1 MB leaf cap as an encoding limit.
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
   "this sub-tree uses directory storage."
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

## 7. Alternatives considered (from the debt note)

- **Per-row HOT slots** (1024 slots/leaf): exact SLIDING_SNAPSHOT match, but
  destroys the columnar scan layout and multiplies key overhead by 1024;
  rejected.
- **Per-column HOT slots** (one slot per column per leaf): row updates re-emit
  ~one column body (~8 KB raw); better than whole-leaf but still couples slot
  size to leaf width, keeps large values in the trie, and misaligns with the
  compact codec (whose column bodies are variable and tiny). Rejected in favor
  of directories, which decouple trie fan-out from payload size entirely.
- **Overflow-record spill** (`OverflowPage` referenced from the slot): closest
  existing mechanism, but it spills *whole values* — a one-row change still
  rewrites the whole overflow payload, so it fixes fan-out without fixing
  share granularity. The directory design subsumes it.

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
  allocation (h), multi-chunk metadata tombstone replacement (e).
- Differential test: build the same data set under old and new layout in two
  stores; assert identical hydrated payload bytes and identical query results
  across the 89-case differential suite.
- Scale gate: 100 M-record build + disk-cold reopen; assert trie depth and
  hydrate time improve (the ~8 s / 97 k-leaf baseline from PR #1116), and that
  a single-row update commit writes O(1) chunk pages (measure bytes written).

## 9. Open questions

1. Should `CHUNK_SIZE` grow (e.g. 16 KB) now that chunks are off-trie? Larger
   chunks halve directory sizes and page-load counts but coarsen CoW sharing.
   Default answer: keep 4 KB until the single-row-update byte-write metric
   (§8) says otherwise.
2. Does the commit-walk hook generalize? CAS's `ChunkDirectory`/
   `BitmapChunkPage` path wants the same reference-bearing-slot-value
   treatment; building the hook `IndexType`-agnostically would un-strand that
   machinery too.
3. Reclamation policy for orphaned leaf directories above `leafCount`
   (5.1-6): delete eagerly at rebuild, or leave to revision aging? Eager is
   proposed; confirm it doesn't complicate the rollback story (deletes must
   ride the log like writes).
