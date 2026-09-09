# Segment dictionaries — one lane for document pages and projection leaves

Status: APPROVED DESIGN, 2026-09-05. Supersedes the arrival-order position and the two-lane split in
`SEGMENT_SCOPED_DICTIONARIES.md`, and the pre-pass route (`P2_GLOBAL_DICTIONARY_DESIGN.md`,
`ClickBenchLoadPrepassHook`, `PrebuiltGlobalDictionary`, `ProjectionRankPass`).

One principle: **the segment is the unit of everything; the name dictionary is the model.**

## 0. What is being replaced, and by what measurement

At 1M rows (same session, FULL versioning, `gate1mT.sh`):

| route | total | mechanism |
|---|---|---|
| no dictionary | 918.9 MB | strings inline in document pages and projection leaves |
| segment lane, document pages only | 874.4 MB | ids minted at page encode, per 1024-leaf segment |
| pre-pass dictionary + trie lane | **472.6 MB** | a closed corpus, ranked before the load, one global dictionary |

The pre-pass wins by 363.5 MB in the PROJECTION INDEX's leaves and 82.9 MB in document pages. It is
not incremental (a second load cannot extend it), it is a benchmark harness (a closed corpus read
twice), and a global dictionary is rewritten as it changes — all three rulings against it stand.
This design reaches the same bytes incrementally: **gate = same-session 1M pair against 472.6 MB,
43/43 byte-identical query dumps, then 100M.**

## 1. Unit — the segment

A **segment** is a page-aligned node-key range `[startKey_s, startKey_{s+1})` of the DOCUMENT trie.
Node keys are monotone and never reused, so membership of a node in a segment is permanent.

- Boundaries are a persisted, sorted `long[]` of start PAGE keys in the **segment directory** (§2).
  `segmentOf(recordPageKey)` is a binary search over it; the last entry is the open segment.
- A segment **closes** when the bytes of distinct values minted into it (sum over tags) reach
  `SEGMENT_DICTIONARY_BUDGET_BYTES = 64 MiB`, or when it spans `SEGMENT_MAX_LEAVES = 2^17` document
  leaves, or at commit. Two constants, no properties. The close happens at the next page boundary
  the coordinator adopts; the projection builder cuts its row group at the same record.
- Projection row groups never straddle a boundary: a row group holds rows whose record keys lie in
  one segment. A record straddling a boundary (its tail nodes in segment s+1) belongs to the segment
  of its ROOT node key; its tail nodes' document pages mint into s+1. One row per boundary appears
  in two dictionaries — harmless, each side resolves through its own page's segment.
- A commit closes the open segment (a revision's pages must be readable at that revision). A new
  write transaction that touches a sealed segment does NOT reopen it: it appends to the segment's
  TAIL (§5). A new transaction inserting keys beyond the last boundary opens a new segment.

## 2. Storage — one keyed sub-trie, resource scope

The dictionary lives in the existing projection-value-dictionary sub-trie (`NamePage.
createProjectionValueDictionaryTree`, keys reserved as runs by `reserveProjectionValueDictionaryKeys`).
It is a RESOURCE structure: document pages resolve through it without any projection index. (Today's
trie lane resolves through a projection index's metadata blob — dropping the index orphans the pages.)

**Segment directory** — ONE record at the FIXED key `SEGMENT_DICTIONARY_DIRECTORY_KEY = 1` of the
sub-trie (the lane reserves keys 1..1023 at bind, so the directory is alone in page 0 and the NamePage
format does not change; a resource without the lane has a rank-pass HEADER or nothing at key 1, and
readers dispatch with `instanceof SegmentDictionaryDirectoryNode` — "no segment directory"):
- `long[] segmentStarts` (sorted start PAGE keys; entry 0 is 0)
- per SEGMENT its own slot table, one entry per slot: `int[] tags` (the path classes the slot
  covers — a tag belongs to at most one slot per segment, verified at seal), `long headerKey`
  (0 = never minted), `int sealedEntryCount`. Slot = projection column index of the load that wrote
  the segment, so the tag sets are repeated per segment on purpose: a later load may bind columns
  differently.
- rewritten only at seal and at compaction (CoW of one record).

**Per (segment, tag) dictionary generation** — one key run:
- `ValueDictionaryHeaderNode` at the run's first key, alone in its leaf (the run is padded to a
  1024 multiple so buckets start in the next leaf). Fields: `entryCount`, `orderedPrefixCount`,
  `blockIndexKey` (separator array), `generation`, **new: `rankTableKey`** (0 = identity;
  invariant `rankTableKey != 0 → orderedPrefixCount > 0`). `forwardRootKey` is always 0 — no
  FORWARD radix, ever; `reverseRootKey` keeps rooting the value blocks exactly as the ordered
  appender writes them today (the reverse radix is the value blocks' address space, not a cost the
  lane adds). Written as a trailer after the `(orderedPrefixCount, blockIndexKey)` pair; read only
  when the bytes are there, so `db100m-ovf`'s rank-pass headers stay readable.
- Value buckets (`ValueDictionaryValueBlockNode`, `MAX_BLOCK_VALUES = 256`) in RANK order for
  `1..orderedPrefixCount`, front-coded (`ValueDictionaryValueBlockCodec`), then tail buckets in
  arrival order (§5). Bucket i holds ranks `256·i+1 .. 256·(i+1)`.
- Separator array: the first value of every ordered bucket; probe = binary search over separators,
  then one bucket.
- **Rank table**: the permutation between mints and ranks over `1..orderedPrefixCount`, bit-packed
  to `bitsPerEntry = 32 - numberOfLeadingZeros(orderedPrefixCount)` bits, in records of 16384
  entries (`ValueDictionaryRankTableNode`, ≤ 64 KiB even at 32 bits). BOTH directions are persisted
  as two runs of `records = ceil(P / 16384)` records each at arithmetic keys: the forward run
  `mint → rank` at `rankTableKey + i` (decode) and the inverse run `rank → mint` at
  `rankTableKey + records + i` (probe), so a point probe costs one separator search, one bucket and
  ONE inverse record — never an O(P) in-memory inverse per view (query-side probes run once per
  segment per query). Each record packs `(count × bits + 63) / 64 + 1` words: the last word is a
  PADDING word the bit-extraction reads past the end of the payload into, so records are NOT
  concatenable by array copy — a reader addresses entries through the record that covers the key
  (`firstKey + index`). Identity above `orderedPrefixCount` (tail mints are their own rank), identity
  everywhere when `rankTableKey == 0` (a compacted generation, §7). Priced at 100M with 1M-row
  segments: ~196 MB per direction (100 segments × 3 slots × 275k × 19 bits), ~0.4 GB for both.

CoW per revision: touching segment s rewrites only s's touched records; the header is alone in its
leaf; the one open tail bucket is the only in-place modification (≤ ~1.5 KB).

## 3. Ids — one space per (segment, tag)

An **id** ("mint") is dense from 1 per `(segment, tag)`, assigned at ENCODE time in arrival order,
by the writer's in-memory `SegmentScopedDictionaries` — the same mechanism for a document page's
string region and for a projection leaf's id lane. **Document pages and projection leaves carry the
same id for the same value.** Ids are permanent within a generation and never reused; a value that
is deleted and re-inserted resurrects its id (§7).

A **rank** is the id's position in the sealed generation's collation order (UTF-16, the header
contract). Ranks are storage positions, not identities: the lane never carries a rank. Decode:
`mint → rank (forward run) → bucket(rank / 256) → value`. Probe: `value → rank (separators + bucket
binary search) → mint (inverse run, or the writer's in-memory hash)`.

**Why ids are not ranks (the one deviation from the first spec, decided 2026-09-05).** Writing
ranks into pages needs the value set closed before the pages are encoded, i.e. holding every
document leaf of the open segment: 32 KiB frames × ~10 rows/leaf = ~525 MB at 159k rows, ~3.3 GB at
the 1M-row segment the 100M capture curve wants (88.8 % vs 78.7 % at 100k rows = 1.08 GB of
dictionary bytes at 100M), inside the async-flush pinned-region machinery. The rank table costs
~2.25 B/entry per direction ≈ 0.4 GB at 100M for both, replaces the 65 B/entry radix, is a per-generation artefact
that compaction removes, and lets the segment be sized by its dictionary alone.

Empty string: exactly kind 5's contract, both lanes. A clean (present, representable) `""` is
interned like any other value and gets a real id; id 0 in a long lane means absent/unrepresentable
(the row-level presence bits remain the truth). The document side already mints the empty value
(`resolveGlobalIds` interns every entry of a tag or none). No lane-specific special case.

## 4. Liveness — none on the write path

The dictionary write path only APPENDS immutable records. No reference counts, no per-value
tombstones (priced under FULL: 2 count buckets ≈ 300 KB against a 50 KB row write). Liveness is
computed at compaction by scanning the segment's pages (a reference bitmap over the id lane, exact,
`projectionNumericDistinct` is the kernel). Tombstones = `removeRecord` (`DeletedNode`) of a whole
old generation's records at compaction, never per value.

## 5. Tail and probe

After the seal, a writer touching segment s (any later revision):
- probes value → mint: binary search over the ordered prefix, then a lazily built in-memory hash
  for the tail (`Names` pattern); dropped at commit; readers never probe for encode.
- a miss appends to the open tail bucket (32-wide, in place) → mint `entryCount + 1`, rank = mint.
  The header's `entryCount` grows; `orderedPrefixCount` does not.
- pages record the `entryCount` they saw (`dictionaryEntryCount`, already in the page format); a
  reader refuses an id above the header's count at its revision — the existing `accepts` contract.

## 6. Seal — after every encode of the revision, before its tree is written

**When (corrected 2026-09-05, e2e witness `1707 vs 2000`).** NOT in `beforeCommit`. That hook runs
before `commitWritePages`, and the only fence it can await (`awaitPendingAsyncFlush`) fences the
flush POOL, not the pages the COMMIT itself encodes: the tail leaf (adopted, never flushed) and
every CoW'd leaf (page 0 under a growing record-set array). Those leaves mint AFTER the dictionary
was written, record an `entryCount` above the sealed count, and `accepts` refuses them on read — an
unreadable page. The seal therefore runs INSIDE `commitWritePages`, between the parallel
serialization pass (P1, `parallelSerializationOfKeyValuePages`) and the recursive write (P2,
`uberPage.commit(this)`): a writer hook (`RevisionEncodeCompleteHook`, installed by the lane next to
the resolver factory) that fires once per commit — intermediate auto-commits included, because
revision k must be readable AT k, not only at the load's last revision. The hook's records go
through the transaction's intent log (`namePage.putProjectionValueDictionaryRecord`) and ride the
same commit: the NAME sub-tree (offset 4) is walked after DOCUMENT (offset 0).

**What P1 guarantees.** `prepareFinalCommitKeyValuePage` fully encodes — and caches the bytes of —
every live DOCUMENT leaf that carries a resolver, whatever the byte-handler pipeline (an empty
pipeline's identity result is an owned copy, `ByteHandlerPipeline.compress`); every other leaf keeps
today's preparation-only pass under an empty pipeline. After P1 every value of the revision's lane
pages is minted. P2 serves the cached bytes (`FileChannelWriter.bufferSerializedPage` honours the
cache regardless of the pipeline) and re-encodes only a leaf declined for unresolved overflow
carriers (#1076) — which, against the frozen table, finds every value present and produces the same
ids.

**Freeze, not release.** The seal FREEZES s: `idOf` turns lookup-only — a present value → its id, an
absent one → `ID_ABSENT` (the region encoder then keeps that tag's bytes for that page), never a
mint, never a throw. Tables are released after P2 (`afterRevisionWritten`), not at the seal. A leaf
of a sealed segment encoded in a LATER revision (a half-filled tail continued by the next epoch, a
post-load update) resolves against nothing and keeps bytes — correct, storage-only; slice 4's tail
generation plus a probe against the persisted dictionary gives that storage back.

**CoW.** `deepCopyFrozenContainer` hands the copy `documentStringResolverFor(pageKey)` while a
factory or resolver is installed — the fresh-page rule, applied to the third way a leaf becomes
modifiable (`deepCopy()` never carried the resolver). A leaf dereferenced from DISK for modification
gets none until slice 4 (its segment is sealed and released; bytes either way).

**Prerequisite.** A lane page is readable only through `deserializePageLazily` with a chunk-framed
body (`ChunkedBodyConfig.enabled()`, decided at WRITE time, off by default); the eager expansion
refuses a global tag loudly. `SegmentDictionaryLane.bind` refuses to bind while chunk framing is
off (a positive witness, not a fallback); 1f decides whether lane loads force framing on.

**Per slot with values in s, on the committing thread:**
1. `byId` from the mint map; sort the mints by value (UTF-16 collation, `compareUtf16Range`, the
   header contract) → `rankByMint`;
2. feed the values in rank order to chained rank-ordered generations (`markRankOrdered()` +
   `flush`/`flushAppend`, 16384 distinct values per generation, `ordered = rankOrdered &&
   base.isFullyOrdered()` keeps the chain ordered) → `headerKey`;
3. `GlobalValueDictionary.buildBlockIndex(headerKey, …)` — it reads values by STORAGE position, so
   it MUST run before the rank table exists (returns 0 when `entryCount <= 256`);
4. `GlobalValueDictionary.attachRankTable(headerKey, rankByMint, …)`: refuses (before reserving a
   single key) anything but an ordered prefix with no forward index and no table yet, a
   `rankByMint` that is not a permutation of `1..P`, or a wrong length; then reserves
   `ceil(P / 16384)` consecutive keys, writes the rank-table records, and rewrites the header with
   its own fields carried over plus `rankTableKey` (the header invariant `table → no forward root`
   is the second line of defence, so the forward root is carried, never written as a literal 0).
   The seal SKIPS this step when the permutation is the identity — ids then ARE positions, which is
   strictly cheaper to serve; `attachRankTable` itself accepts an identity table;
5. record `(tags, headerKey, entryCount)` for `(s, slot)` in the directory (key 1) — THE persisted
   seal record, the one thing the reader consults (§10 1d); the projection-metadata anchors are not
   written; then freeze s.
Cost: one sort of ~200k values per slot per segment (~250 ms; on the committing thread first,
off-thread only if a profile says so); nothing is held; the flush pipeline is untouched. The seal is
the rank pass, per segment.

Read side under a table (`GlobalValueDictionary.ReadView`): `positionOf(mint)` translates before
every storage access (`sliceSlot`, `blockKeyCovering`, `valueOffset`, `spillKeyCovering`);
`compareIds` compares positions; `probe`/`searchOrderedPrefix` return a position and translate back
to a mint. `fullyOrdered` on the view means "id order IS collation order", so it is
`header.isFullyOrdered() && !header.hasRankTable()`; the header's `isFullyOrdered()` keeps meaning
STORAGE ordered (binary-search probe legal, `forwardRootKey == 0` legal). `fillStringOpVerdict`
REFUSES a view with a rank table in slice 1: its bucket → verdict-word aliasing and lane-split
argument do not survive an id permutation; slice 2 evaluates in rank space and translates at
consumption.

## 7. Compaction — per segment, whole-or-nothing

Triggered at commit when a segment's tail or dead fraction exceeds a default. In ONE revision:
live values (reference bitmap over every page and leaf of the segment) → sorted → new ids (mint =
rank, no table) → every document page and projection leaf of the segment rewritten → old
generation's records `removeRecord`. **Invariant: a segment's id assignment changes only in a
revision that rewrites every record of the segment.** This is what keeps DIFFERENTIAL /
INCREMENTAL / SLIDING_SNAPSHOT `combineRecordPages` sound: a fragment from revision r decodes
against the generation that was current at r, and no revision mixes generations within a segment.
A witness per versioning type asserts that a remap without the full rewrite is refused.

## 8. Cardinality — a constraint on kinds, not a kernel rewrite

The projection column kind for segment dictionaries is a LONG-LANE kind (`COLUMN_KIND_STRING_SEGMENT`),
sibling of kind 5 (`STRING_GLOBAL`), never of kind 2 (`STRING_DICT`): `COUNT(DISTINCT)` is a bitset
over the id lane per segment plus one string hash per (segment, distinct id) for the cross-segment
merge; the entry-union kernels (`distinctPresentStrings`, `distinctDictUnionParallel`) stay gated
on kind 2. Group keys are `(segment, id)`; groups merge across segments by value bytes, once per
distinct (segment, id) — the cost every block-scoped column store pays.

## 9. Delete

The pre-pass (`ClickBenchLoadPrepassHook`, `PrebuiltGlobalDictionary`, `PrePassDictionaryBuilder`,
`-Dsirix.projection.globalDict.prebuilt`), the rank pass as a mode (`ProjectionRankPass`,
`RankPassDictionaryAppender`), the trie-lane / segment-lane split (`TrieLaneWriteDictionaries`,
`TrieLaneDictionaries`, `-Dsirix.projection.trieLane`, `-Dsirix.projection.segmentDict*`), the forward
radix write side (`GlobalValueDictionaryRadix`, `ValueDictionaryRadixNode`), the projection-metadata
anchors (`SegmentAnchor`, `valueDictionaryHeaderKeys`), and every property in this area. One lane,
on by default for string columns of a projection index.

## 10. Order of work, each slice with witnesses + mutants + an independent review

1. Storage + seal + read translation, in sub-slices: (a) `ValueDictionaryRankTableNode` (kind 61),
   `SegmentDictionaryDirectoryNode` (kind 62), `rankTableKey` on the header — no NamePage change —
   PLUS, pulled forward from (c) because the translation is untestable without a table writer: the
   `ReadView` mint ↔ position translation, `attachRankTable`, and `RankTableReadViewTest` (every
   mint decoded through a random permutation with both extremes pinned, every value probed to its
   MINT, `compareIds` against the collation oracle, verdict refusal, second-index/append/double-table
   refusals, and "a refused attach reserves no keys"; 27 mutants, 25 killed, 2 equivalent);
   (b) `SegmentBoundaries` (byte-budget close decided once, on the adopting thread) and
   `SegmentScopedDictionaries` keyed by `(segment, column)`; (c) the seal writer (§6), the writer's
   `RevisionEncodeCompleteHook` between P1 and P2 with P1's full encode of resolver-carrying leaves,
   freeze semantics, the CoW hand-off and the chunk-framing refusal, with witnesses that read every
   mint back through the table and probe every value to its mint, each mutated to prove it bites;
   (d) the document-side read resolver through the directory (per-(segment, slot) cached views; the
   projection-metadata anchors are neither written nor consulted) — lands WITH (c), because (c)
   stops producing the anchors; (e) `COLUMN_KIND_STRING_SEGMENT` — a long-lane kind — encoded at append with the
   row group cut at the segment boundary, decoded in the column store, refused LOUDLY by every other
   kind-5 kernel until slice 2; (f) on by default in `ProjectionBulkLoad`, the two properties gone.
2. Query side: every kind-5 site made segment-aware or refused for the new kind (grouping, distinct,
   EQ/IN/LIKE/range, order, zone maps, materialisation).
3. Gate at 1M vs 472.6 MB (same session, 43/43 byte-identical dumps); then delete (§9).
4. Compaction (§7) with the four versioning-type witnesses; the incremental-maintenance lazy route
   in write transactions (`ProjectionIndexChangeListener` refuses converted pages today).
5. 100M.
