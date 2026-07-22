# Projection Index — Hybrid Inline / Referenced Segment Slots

Status: **design proposal**. Builds directly on the storage layout shipped in
#1131 (`docs/PROJECTION_INDEX_STORAGE_REDESIGN.md`) and its follow-ups
(#1129–#1132). Verified against as-built code: `LeafDescriptor`,
`ProjectionIndexHOTStorage`, `ProjectionSegmentPage`, `HOTLeafPage`,
`PageKind.HOT_LEAF_PAGE`, `AbstractHOTIndexWriter`,
`NodeStorageEngineReader.loadHOTLeafPageWithVersioning`, `VersioningType`.

Scope: give each per-leaf column **segment** two storage classes — **inlined**
into the owning HOT descriptor slot, or **referenced** as a standalone
`ProjectionSegmentPage` (today's only option) — and choose per segment by size.
This is the same inline-until-it-doesn't-fit rule `KeyValueLeafPage` already
uses for node records (inline in the slotted heap; spill to `OverflowPage` past
the largest size class, #1076). The projection path is the outlier that always
spills; this closes that gap.

---

## 1. Motivation, and the versioning observation that prompted it

The redesign split each logical projection leaf into a tiny **descriptor**
(`LeafDescriptor`, "PIXD", the HOT slot value) plus one **segment** per
KEYS / BODY(c) / DICT(c), each segment a dedicated CoW page
(`ProjectionSegmentPage`) hung off the leaf's side map by a bare 8-byte offset
key. That fixed the §1.6 deep-split problem (multi-KB values no longer sit in
HOT slots) and made unchanged segments shareable across revisions by reference.

But it left a real edge, which the prompting question named precisely:

> *the different versioning strategies have no effect, basically all act like a
> full copy of the page.*

Made exact against the code, this is **true for the tier that holds ~all the
bytes, and false for the tiny tier**:

- **Segment tier (`ProjectionSegmentPage`) — the claim is true.** The page is
  deliberately `OverflowPage`-shaped: offset identity, **no fragment chain**,
  `commit()`/`getReferences()` throw. `VersioningType` is **never consulted** on
  the segment path (`NodeStorageEngineWriter.commit(PageReference)` has one
  branch — `instanceof OverflowPage || instanceof ProjectionSegmentPage` → write
  the whole page immediately, key = file offset). A changed segment is always a
  whole new page; an unchanged one is carried forward by reference. FULL,
  INCREMENTAL, DIFFERENTIAL, and SLIDING_SNAPSHOT are **indistinguishable** here.
  Since a one-row change re-encodes and rewrites the entire ≤1024-row column
  segment, that segment is copied in full on every touch — regardless of the
  resource's configured strategy.

- **Descriptor tier (`HOTLeafPage`) — the claim is false, but it's the ~150-byte
  tier.** `VersioningType` *is* honoured: under FULL the writer calls
  `markAllEntriesDirty()` (full dump of every slot); under the default
  SLIDING_SNAPSHOT the copy starts with a clean dirty bitmap and each mutated
  slot is marked dirty, so `PageKind.HOT_LEAF_PAGE.serializePage` emits a
  **sparse dirty-only fragment** (`getDirtyEntryCount` / `packDirtyEntries`),
  reassembled on read via `mergeHOTFragmentsByKey`. Caveat: the side-map **refs
  section is re-emitted in full on every fragment**, sparse or not
  (`serializeSegmentRefs`), and the three non-FULL strategies differ from each
  other only in fragment-chain retention (INCREMENTAL ≡ SLIDING_SNAPSHOT in the
  HOT path).

Net: the bytes that dominate the projection index live in an **un-versioned,
whole-page-LWW tier**. The configured strategy governs only the descriptors,
whose bytes are marginal. The observation is right where it matters.

The important corollary: you **cannot** fix this by "turning versioning on" for
segment pages. They are offset-identity pages with no logical page key precisely
so they need no fragment chains and dodge the `maxHotPageKeys` crash-replay
hazard (redesign §5.2-a/h). Giving them fragment chains would reintroduce all of
that. **The only way to bring segment bytes under the versioning machinery is to
move them into a tier that already has it — the descriptor slot.** That is what
inlining does, and it is the design's own contemplated-but-unbuilt escape hatch
(§5.2-j: "coalesce sub-threshold segments … only if measurement demands").
Inlining is strictly better than that bundle-page idea because it does **not**
trade back update containment (below).

---

## 2. The size facts that make this worthwhile

Measured from the codecs (`ProjectionIndexSegmentCodec`,
`ProjectionIndexLeafCodec`; `MAX_ROWS = 1024`):

**Fixed cost of a referenced segment — what inlining removes, per segment:**

| Component | Bytes |
|---|---|
| Side-map ref entry: `(compositeKey u64, offsetKey u64)`, re-emitted on **every** HOT fragment | 16 |
| Segment-page framing: outer length (4) + kind envelope (3) + 8-byte fragment align (~3.5) | ~14–15 |
| PIXS segment header (magic+version+kind), redundant once the descriptor entry already holds id/byteLen/hash | 6 |
| **On-disk fixed total** | **~36** |
| plus: a whole page, a `PageReference`, a page-cache entry, and **one random page read** per segment at hydrate/scan | — |

**Segment size distribution — how many segments are smaller than that overhead:**

- **Tiny (dominant for boolean / enum / sparse / low-cardinality / constant
  columns):** empty-leaf BODY **7 B**; constant or all-null numeric BODY **~33 B**;
  single-value string BODY **~25 B**; low-cardinality DICT **16–60 B**; boolean
  BODY **~152 B**; dense-ascending KEYS **~160 B**.
- **Large (KBs — stay referenced):** wide-range numeric bodies (1–8 KB),
  high-cardinality dict-id bodies (~1 KB), high-cardinality / FSST DICT segments,
  wide-delta KEYS.

So for a broad class of real columns we **spend ~36 B of fixed overhead plus a
random read to store 7–60 B of payload** in its own page. That is pure loss, and
exactly the segments a hybrid inlines.

**Descriptor / slot budget** (`HOTLeafPage`: 64 KB slot memory, 512-entry cap;
`LeafDescriptor` fixed 30-byte entries):

| Leaf shape | Descriptor bytes | Descriptors per 64 KB HOT leaf (today) |
|---|---:|---:|
| 3 cols numeric/bool | 152 | ~417 |
| 3 cols, 1 string | 182 | ~360 |
| + one ~60 B inline segment | ~242 | ~302 |
| + a full 256 B inline | ~410 | ~158 |
| 84-col max, all string | 5 183 | (still far under the 65 535 u16 value cap) |

Inlining trades descriptor density for eliminated pages. Even inlining a full
256 B into *every* leaf keeps ~158 leaves per HOT page — an order of magnitude
above the ~3–15 chunk entries of the pre-redesign layout, so the §1.6 shallow-trie
win is preserved. The trade is bounded because we inline **only small segments**
under a **per-leaf budget**.

---

## 3. Design

### 3.1 Two storage classes per segment

Each descriptor entry gains a storage class:

- **REF** (today's behaviour): bytes live in a `ProjectionSegmentPage`; the entry
  carries `byteLen` + `contentHash` (integrity + maintenance no-op comparator);
  the side map carries the offset ref.
- **INLINE**: bytes live in a trailing region of the descriptor slot value
  itself; **no** side-map ref, **no** separate page, **no** PIXS header.
  Integrity rides the enclosing HOT leaf page (same XXH3 page hash that already
  protects the descriptor bytes next to them); the no-op comparator is a direct
  byte compare (cheaper than hashing, and the bytes are already in hand).

### 3.2 PIXD v2 wire format

Keep the 30-byte fixed entry (positional, allocation-free readers are
load-bearing on the scan hot path) and append a variable **inline region**:

```
  int   MAGIC "PIXD"                              [0]
  byte  VERSION = 2                               [4]
  int   rowCount                                  [5]
  short columnCount                               [9]
  long  firstRecordKey                            [11]
  long  lastRecordKey                             [19]
  byte[columnCount] kinds                         [27]
  short segCount                                  [27 + columnCount]
  segCount × 30-byte entry:
     byte  segmentId
     int   byteLen           // REF: durable segment length; INLINE: inline length
     long  contentHash       // REF: XXH3-64; INLINE: XXH3-64 of the inline bytes (kept: uniform no-op compare, integrity-in-depth)
     byte  colFlags          // + SEG_INLINE (0x80): high bit, additive to the existing column flags
     long  min
     long  max
  inline region: for each INLINE entry in ascending segmentId order, its raw
                 encoded segment bytes, concatenated. Offset of entry i =
                 (end of entry table) + Σ byteLen of prior INLINE entries.
```

- **No new per-entry offset field** — the inline region is ordered by segmentId,
  so a reader recovers each inline slice by summing prior inline `byteLen`s in a
  single positional pass. Entry table stays fixed-width; all existing positional
  readers are unchanged.
- **`validate` length rule** becomes
  `entriesEnd + Σ(byteLen where SEG_INLINE)`; a REF-only descriptor is exactly
  today's length, so the check degenerates cleanly.
- **VERSION → 2.** No deployed databases (project convention, redesign §6);
  legacy PIXD v1 stores are already structurally detected and rebuilt on open, so
  this needs no dual-read path.

New positional readers (mirroring the existing ones):
`entryIsInline(d,i)`, `inlineSlice(d,i) → (offset,len) into d`.

### 3.3 Decision rule (encode time)

Two knobs, both `-D`-overridable, defaults chosen from §2:

- `sirix.projection.inlineMaxSegmentBytes` (default **192**, band 128–256): a
  single segment is inline-eligible iff its encoded length ≤ this.
- `sirix.projection.inlineMaxTotalBytes` (default **512**): cap on total inlined
  bytes per descriptor; once exceeded, remaining eligible segments spill to REF
  (largest-first, so the smallest stay inline).

Both are pure size predicates on already-encoded bytes — deterministic, so the
maintenance no-op hash stays stable across identical re-encodes. KEYS, BODY(c),
and DICT(c) are all eligible; in practice the winners are boolean/constant/
all-null bodies, single-value and low-cardinality dicts, and dense KEYS.

### 3.4 Write path

`ProjectionIndexHOTStorage.putEncodedLeaf` (the current REF-only loop at
lines ~251–266) generalises. `EncodedLeaf` already yields per-segment bytes; the
codec now also classifies each as INLINE/REF and folds INLINE bytes into the
descriptor it emits, so the storage layer sees a descriptor that already contains
its inline region plus a (possibly shorter) REF segment list:

```
put(leafIndex, encoded):
  writeSlotValue(leafIndex, encoded.descriptor)          // now carries the inline region
  for each REF segment s:                                // INLINE segments need no page
    if prior REF entry s has equal (byteLen, contentHash):
      carry the reference forward                         // §3 no-op, unchanged
    else:
      putSegmentPage(leafIndex, s.id, s.bytes)
  reconcileVanished(prior, encoded):                      // generalises dropVanishedSegments
    - segment present-as-REF before, now INLINE or gone  → removeSegmentPage
    - segment present before, absent now                 → removeSegmentPage
```

INLINE segments ride the descriptor slot's own dirty tracking — no extra
bookkeeping, and no side-map entry to route. A segment that crosses the threshold
between revisions simply migrates class; `reconcileVanished` drops any stale REF,
and the migration is never a false no-op because the crossing implies a size (and
thus hash) change.

### 3.5 Read path

`getSegmentBytes(leafIndex, segmentId)` branches on the descriptor entry:

- **INLINE** → return `inlineSlice(d, i)` straight from the descriptor bytes,
  which are already resident in the HOT leaf slot the caller just read. Zero page
  loads, near-zero-copy. Cold-open hydrate and first-touch scans of small columns
  lose an entire random read apiece.
- **REF** → resolve the side-map ref and load the `ProjectionSegmentPage` (exactly
  today's path).

The byte-identical raw-scan-form assembler (`get(leafIndex)`) is unaffected: it
already asks for segment bytes by id; it neither knows nor cares which class
produced them. `verifySegment`'s length/hash check applies uniformly (INLINE
keeps a real `contentHash`).

### 3.6 Split, maintenance, carry-forward

- **Split:** INLINE segments are part of the slot *value*, so they move with their
  descriptor automatically when `splitTo` relocates the slot — no analogue of
  `moveSegmentRefsAfterSplit` needed for them. That routing stays only for REF
  segments (unchanged), and its surface **shrinks**.
- **Sparse-emit hazard (redesign §5.2-b) shrinks too.** The subtlety today is that
  a slot whose *ref* was newly resolved must still be included in the emitted
  fragment even if its value bytes didn't change. INLINE has no such split between
  "value" and "resolved ref": if the bytes change, the slot value changes and is
  naturally dirty; if they don't, it isn't. The tricky case only remains for REF
  segments.
- **Carry-forward / no-op:** REF↔REF unchanged uses the existing (byteLen,
  contentHash) compare. INLINE↔INLINE unchanged falls out of the descriptor being
  byte-identical. Maintenance only re-writes genuinely touched leaves, so an
  inline segment is re-emitted only when its leaf is touched — and then it's a few
  tens of bytes folded into a descriptor delta that was going to be written
  anyway.

---

## 4. What it buys — and what it deliberately does not

**Buys:**

1. **Eliminates the ~36 B + page + ref + random-read overhead per small segment.**
   Directly resolves redesign hazard §5.2-j (small-segment fragmentation), and
   does so *without* the bundle-page's loss of update containment: each inline
   segment is still independently re-encoded, and a changed inline segment
   rewrites only its own leaf's descriptor slot — which is already the maintenance
   containment unit.
2. **Brings small/hot segment bytes under the descriptor tier's real versioning.**
   Under SLIDING_SNAPSHOT they now ride the sparse dirty-entry fragments; the
   configured strategy finally governs them instead of the un-versioned
   whole-page-LWW tier. This is the concrete answer to the prompting observation.
3. **Fewer pages, shallower everything, smaller cache footprint** — a broad class
   of columns stops minting a page + `PageReference` + cache entry per leaf.
4. **Better cold-read locality** — narrow columns are served from the HOT slice
   already in hand; hydrate and first-touch scans drop one random read per inlined
   segment.
5. **Consistency with `KeyValueLeafPage`** — the projection path stops being the
   lone "always spill to a referenced page" outlier.

**Does not (by design):**

- **No sub-segment / row-level deltas.** A changed inline segment is still
  rewritten whole (as part of its descriptor slot). Columnar FOR/bit-pack/ALP/FSST
  streams have data-dependent lengths — the byte-shift cascade (§1.6) is why
  1024-row leaves were chosen so a full re-encode is cheap. Inlining does not and
  should not attempt intra-segment versioning.
- **No help for large segments.** Wide bodies and high-cardinality dicts stay
  referenced — that is the point; keeping them out of HOT slots is the §1.6 fix.
- **No change to the segment-page contract.** `ProjectionSegmentPage` stays
  offset-identity and un-versioned; we simply route fewer, larger segments to it.

---

## 5. Corner cases

- **Descriptor density regression.** Inlining lowers leaves-per-HOT-page (§2). The
  per-leaf budget bounds it (worst case ~158 leaves/page at a full 256 B inline;
  typically ~300+). Measure trie depth at 100 M against the redesign's baseline
  (§8.7) and lower `inlineMaxTotalBytes` if depth regresses.
- **u16 value cap.** Descriptor + inline region must stay ≤ 65 535 (the HOT slot
  value length field). The budget makes this unreachable; keep an assertion.
- **Integrity for inline bytes.** Covered by the HOT leaf page's XXH3 page hash
  (same protection the adjacent descriptor bytes already rely on) plus the
  retained per-entry `contentHash`. No weakening versus REF.
- **Class migration across revisions** (a dict grows past the threshold, or a
  column becomes constant and shrinks under it). Each revision's descriptor records
  class independently; `reconcileVanished` drops the stale REF page; the size change
  guarantees it is never a false no-op. Time-travel reads resolve each revision's
  descriptor on its own terms.
- **Determinism.** The threshold predicates run on already-encoded bytes, so the
  no-op hash and FSST-training determinism guarantees (redesign §5.2-n) are
  preserved; identical inputs still re-encode to identical descriptors.
- **BLOB slots** (whole-leaf oversized payloads, `BLOB_SEGMENT_ID`) are unaffected —
  they are already a distinct slot shape and always exceed any inline threshold.

---

## 6. Implementation phases

Each phase lands green on the full projection suite (redesign §10) plus its own
tests; no phase changes query results.

- **H1 — PIXD v2 + codec classification.** `SEG_INLINE` flag, trailing inline
  region, `validate` length rule, `entryIsInline`/`inlineSlice`; codec classifies
  segments and folds inline bytes into the descriptor. Tests: round-trip with mixed
  inline/ref; byte-identical assembled raw form; threshold boundary (a segment at
  exactly the cap, budget spill order); 84-column max with inline mix; empty-leaf
  (rowCount 0) with an inline 7 B body.
- **H2 — storage read/write.** `putEncodedLeaf` inline/ref split;
  `getSegmentBytes` inline branch; `reconcileVanished` generalisation. Tests:
  inline↔ref migration across commits; shrink-grow-shrink; a leaf whose every
  segment inlines writes **zero** `ProjectionSegmentPage`s (assert page count);
  cold reopen after a commit mixing inline, ref, and carried-forward segments.
- **H3 — versioning + split.** Confirm inline segments ride sparse SLIDING_SNAPSHOT
  fragments and survive `mergeHOTFragmentsByKey`; split relocates inline bytes with
  their slot. Tests: single-slot inline change emits a single-entry delta (assert
  fragment slot count); deep-split cascade preserving inline payloads; the
  ref-only-dirty emit case still holds for REF segments.
- **H4 — measurement.** Disk tax vs the 5.6 % baseline and vs the §5.2-j status quo;
  trie depth at 100 M; cold-reopen hydrate page-read count on a boolean/enum-heavy
  dataset (expect a measurable drop); tune the two defaults from the curve.

## 7. Config

| Property | Default | Meaning |
|---|---|---|
| `sirix.projection.inlineMaxSegmentBytes` | 192 | per-segment inline eligibility ceiling |
| `sirix.projection.inlineMaxTotalBytes` | 512 | per-descriptor total inline budget (spill largest-first past it) |
| `inlineMaxSegmentBytes = 0` | — | disables inlining → exactly today's REF-only layout (escape hatch / A-B baseline) |
