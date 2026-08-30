# P2 — Resource-wide dictionaries for fat string columns

Status: **DESIGN, for review. No code written.** Brief B7 of `docs/STORAGE_AND_SPEED_PLAN.md` (§3 "P2", §6 "B7").
Tree: branch `codex/clickbench-port-rebased-20260827`, HEAD `03e3ed583` (wave 3 committed as `c0d2e8ee5`).
Every number below is one of: **[M]** measured by this brief or quoted from `CAMPAIGN_PROGRESS.md` with its
timestamp, **[D]** derived arithmetically from an [M] with the derivation shown, or **[A]** an assumption, each
carrying the experiment that settles it.

---

## 0. The one-paragraph result

Promotion to `COLUMN_KIND_STRING_GLOBAL` declines at 100M not because the memory is genuinely needed but because the
planner has **no cardinality estimate** and defends against `distinct == rows` (`ProjectionIndexBuilder:2128-2131`,
`:356-364`): the reservation is `4 × rows × (avg + 52)` = **57 GB for one column at 100M**, against a default
aggregate of `min(maxMemory/8, 2 GiB)`. The fix is therefore two independent things: a **measured** cardinality, and
a build whose heap is bounded by a spill rather than by `D`. Both are only reachable in a **post-pass**, because the
property P2 actually needs — *ids that sort like their values* — is global and cannot be produced by a streaming
mint. Rank-ordered ids turn every ordering operator on these columns into integer work: `ORDER BY URL` becomes a
numeric top-k with exact zone-map pruning, `MIN(URL)` becomes `min(id)`, `URL < 'x'` becomes an id range, and the
`O(entryCount)` single-threaded verdict sweep survives only for `contains`/regex. The storage arithmetic, on
inputs that are now all measured — the corpus's true cardinality (**53.8 M distinct across the four columns**) and
rebuild #2's own 69.63 GB — says two things: the projection half reaches its ≤ 30 B/row acceptance **only with
front-coded dictionary blocks**, which sorting is what makes possible; and the projection half alone lands at
62.2 GB, so **M1 needs the trie's string region to name those same ids** (19.31 GB of the 69.63 GB file is that
region, and 87 % of it is these four columns). The trie lane is therefore in scope, staged last, behind a go/no-go
gate.

---

## 1. Evidence

### 1.1 Corpus measurements taken for this brief [M]

Probe: `$S/agents/b7/card2.py`, `card3.py` — one pass over
`bundles/sirix-query/build/diagnostics/clickbench-official-100m/hits.json.gz`, p=14 HyperLogLog (±0.81 % standard
error) over the raw JSON-escaped value bytes (raw ↔ value is a bijection for a fixed encoder), plus exact non-empty
counts and summed byte lengths. Validated against an exact blake2b run on the 1M file (agreement within 1.7 %).

| rows | URL D | Referer D | Title D | SearchPhrase D |
|---|---|---|---|---|
| 1 M | 275,439 | 227,543 | 73,853 | 18,572 |
| 2 M | 691,954 | 622,304 | 163,555 | 100,988 |
| 5 M | 1,737,860 | 1,868,289 | 988,075 | 611,423 |
| 10 M | 2,618,783 | 2,735,361 | 1,609,150 | 827,942 |
| 20 M | 4,138,833 | 4,423,861 | 2,447,373 | 1,344,176 |
| 30 M | 5,200,106 | 5,422,429 | 2,920,572 | 1,581,559 |
| 50 M | 9,585,944 | 10,542,309 | 5,656,895 | 3,302,917 |
| 80 M | 15,060,784 | 16,371,483 | 7,948,027 | 4,990,120 |
| **99,997,497 (all)** | **18,364,684** | **19,966,360** | **9,411,056** | **6,031,488** |

The last row is the **whole corpus**, measured — the pass completed at exactly 99,997,497 rows, the same row count
the load reports (`load.log:52` `rows=99997497`), which cross-checks that the probe saw every record.

Presence and length over the whole corpus [M]: non-empty 99,929,734 / 81,032,736 / 85,087,080 / 13,172,392
(99.93 % / 81.03 % / 85.09 % / 13.17 %); raw average bytes 95.22 / 86.54 / 111.61 / 56.17. The raw→decoded
correction, measured on the 1 M file by decoding every escape [M]: URL ×0.950, Referer ×0.943, Title ×0.999,
SearchPhrase ×1.000 (URL and Referer are ~99 % escaped, Title 10 %, SearchPhrase 1.8 %) — this is the one place a
1 M-scale measurement is carried to 100 M, and it moves the decoded averages by at most 5 %.

**The design's input table, all [M]** (decoded average = raw average × the 1 M-measured escape correction):

| column | D at 100 M | non-empty | decoded avg B | payload B/row | `ceil(log2 D)` |
|---|---|---|---|---|---|
| URL | **18,364,684** | 99.93 % | 90.46 | 90.4 | **25 bits** |
| Referer | **19,966,360** | 81.03 % | 81.61 | 66.1 | **25 bits** |
| Title | **9,411,056** | 85.09 % | 111.50 | 94.8 | **24 bits** |
| SearchPhrase | **6,031,488** | 13.17 % | 56.17 | 7.4 | **23 bits** |
| **total** | **53.77 M distinct** | | | **258.7 B/row** | **97 bits = 12.1 B/row** |

Two facts fall straight out and drive everything below:

- **The corpus carries 258.7 B/row of fat-column string payload but only 4.68 GB of distinct values** (§4.1) — a
  **5.5× dedup** that per-leaf dictionaries over ~9.7 rows/page cannot see and a resource-wide one takes in full.
- The cardinality is **1.7× higher than a Heaps' extrapolation from the 10–30 M prefix predicted** (11 M vs 18.4 M
  for URL). The prefix is clustered; extrapolating it would have under-priced the dictionary by 40 %. This is why
  the full pass was worth running, and it is why §4.1's conclusion changed.

> The equivalent experiment inside the product is `-DbuildPathStatistics=true` on a rebuild — `PathNode` maintains
> a `HyperLogLogSketch` per path (`index/path/summary/PathNode.java:669-691`). Rebuild #2 does **not** have it on
> (`load.log:43` `pathStatistics=false`), which is why the corpus was measured directly. Turning it on for a future
> rebuild would make the election of §10 free rather than pass-derived.

### 1.2 Storage measurements quoted from the ledger

| what | value | source |
|---|---|---|
| 100 M projection, four fat columns | URL 36.4 + Referer 27.3 + Title 26.6 + SearchPhrase 5.4 = **95.7 B/row = 9.57 GB** | `CAMPAIGN_PROGRESS.md:2004` (`ProjDiskDump`, 97,654 leaves) |
| 100 M URL DICT segments alone | 3.44 GB | same |
| DICT_HASHES at 1 M | 4.3 MB over five per-leaf string columns ≈ 4.3 B/row | 17:35 |
| string blooms at 1 M | 1.2 MB | 17:35 |
| rebuild #1 at 100 M (`eb5a307b7`) | 90.4 GB = leaf 70.76 + overflow/projection 17.45 + HOT 1.93 | 20:55 |
| **rebuild #2 at 100 M (`c0d2e8ee5`, wave 3) — measured while this brief was written** | **`sirix.data` = 69,625,908,164 B = 69.63 GB**; leaf **49.94 GB**, OverflowPage **17.45 GB**, HOT 1.93 GB; load **2,741 s = 45m41s**; acceptance OK, rows 99,997,497, 97,654 row groups | `…-2129/load.log:52-55`, `:121-125` |
| **rebuild #2 string region at 100 M** | **written 19,313,068,264 B = 19.31 GB = 193.1 B/row** (raw 29,634,985,906 = 296.3 B/row, LZ77 0.652) — **38.7 % of the leaf class, 27.7 % of the whole file** | `…-2129/load.log:106` |
| rebuild #2 other regions at 100 M | objKeyNameKey 5.97 GB, numberZoneMap 4.36 GB, number 4.88 GB, stringDictSketch 1.17 GB, recordOrdinal 0.78 GB; region table 36.48 GB total (3.44 B/record); body 1.04 B/record | `…-2129/load.log:104-110` |
| rebuild #2 string slots | `OBJECT_NAMED_STRING` 3,199,920,207 slots = **32 string fields per row** | `…-2129/load.log:95` |
| wave-3 final at 1 M (`c0d2e8ee5`, the same build) | file 1,174.8 MB, **leaf 479.0 MB**; string region **written 173,022,156 B = 173.0 B/row**, raw 268.5 B/row, LZ77 0.644; regions 2.95 B/record; 113,589,990 records / 1 M rows = **113.6 records/row** | 21:22 + `$S/storage1m-wave3f/load.log:114` |
| Σ cold at 100 M / 8 GB, 43/43 on route | 807 s | plan §2 |

**Rebuild #2 finished at 22:15 while this brief was being written and is the baseline for every figure in §4.**
It came in at 69.63 GB against the pre-registered 66–70 GB, and its leaf class (49.94 GB) is within 0.3 % of the
29.6 % reduction wave 3 measured at 1 M. Nothing in §4 is extrapolated from rebuild #1 any more.

### 1.3 Why promotion declines today — the exact arithmetic [M]

`ProjectionIndexBuilder.java`:

- `:305` `PER_ENTRY_OVERHEAD_BYTES = 52` (offsets 8 + lengths 4 + primary hash 8 + secondary hash 8 + a half-full
  open-addressed table's 2 slots × (8 + 4) = 24).
- `:366-370` `bytesPerEntry = averageValueBytes + 52`; `:356-364` `projectedBytes = rows × bytesPerEntry` — **`rows`,
  not an estimate of `D`** (the comment at `:2128-2131` states distinct == rows is the deliberate upper bound).
- `:515-524` `conservativeAutoDictionaryBudget = max(MINIMUM_BUDGET_BYTES × k, projectedBytes × 2k)` with
  `k = 2` when streaming → **`4 × rows × (avg + 52)`**.
- `:214-224` aggregate default `min(maxMemory()/8, 2 GiB)`, split greedily across candidates (`:391-455`).

At 100 M with URL's **measured** avg of 90.46 B: `4 × 99,997,497 × 142.46` = **57.0 GB required against ≤ 2 GiB
available**, and the four candidates split that 2 GiB between them. Solving for the crossover,
`rows_max = 2^31 / (4 × (avg + 52))` → **3.8 M rows at avg 90, 10.3 M at avg 0** for a single candidate holding the
whole aggregate — the plan's observed 6–18 M band is the same curve at the shorter columns' averages. A second,
independent wall: `GlobalValueDictionaryProbeFront:61` `MAX_TABLE_CAPACITY = 2^26` slots →
`maxEntries = 2^25 = 33.5 M`, and at a bounded budget `highestOneBit(component / 22) / 2`.

The genuinely resident structure is the **probe front** — `D × (22 + avgLen)` for the whole load
(`GlobalValueDictionaryProbeFront:66-69` `BYTES_PER_LOGICAL_SLOT = 22` plus a chunked value arena). The
per-generation `GlobalValueDictionaryWriter` is *not* the problem: `MAX_DISTINCT_ENTRIES_PER_APPEND = 16,384`
(`:72`) caps a generation at ~2.4 MB, and `ProjectionBulkLoad:648-657` records that rotating bounded writers is the
only way a column exceeds that ceiling at all. So the "held twice" duplication the plan names is bounded by the
generation, and the 4× reservation is defending against a cost that is really `D × (22 + avgLen)` once:
**[D from M] URL 18.4 M × 112 = 2.06 GB, Referer 19.97 M × 104 = 2.07 GB, Title 9.4 M × 134 = 1.26 GB,
SearchPhrase 6.0 M × 78 = 0.47 GB — 5.86 GB for the four**: still far over the envelope, still worth spilling, but
**ten times smaller than the 57.0 GB the planner reserves for URL alone.**

---

## 2. What P2 changes, in one list

1. **Rank ids.** Ids `1..orderedPrefixCount` are assigned in UTF-16 collation order of their values. Nothing about
   the id *representation* changes; only their meaning, and that meaning is carried by one new header field.
2. **A post-pass build** with a spilled distinct set: bounded heap, sequential I/O, no persistent probe per value.
3. **A boundary field** in `ValueDictionaryHeaderNode` separating the ordered prefix from an append-order tail.
4. **Executor arms** that today decline kind 5, or compare dictionary *entries* per heap comparison, become integer
   arms — plus three arms that must be *built* (ungrouped `COUNT(DISTINCT)`, ungrouped `MIN`/`MAX`, row
   materialisation).
5. **A verdict strategy**: ordering ops stop needing one; `contains`/regex get a parallel, per-query-cached sweep,
   and later a per-block filter.
6. **Optionally, per tag, by measured size: the trie's string region names those ids** instead of carrying value
   bytes. This is the M1 lever and is staged last behind a go/no-go gate.

**Wire changes:** one appended field in one node record (§3.1) and the value block's entry framing (§3.3.1), both
in segment 1; two additions confined to segment 5 (§3.4, §3.5). **The projection's column segments do not change at
all** — `COLUMN_KIND_STRING_GLOBAL` already encodes as a FOR
bit-packed long lane (`ProjectionIndexColumnSegmentCodec:780-784` → `ProjectionIndexRowGroupCodec:109-128`) with no
DICT, no BLOOM and no DICT_HASHES (`:829`, `:853`, `:861`).

---

## 3. Wire layouts

### 3.1 `ValueDictionaryHeaderNode` — the boundary field

Today (`node/ValueDictionaryHeaderNode.java:37-82`): `nodeKey, version=0, entryCount, forwardRootKey,
reverseRootKey, generation`. One `int` is added, in place, no version bump (plan §6: *layouts change in place;
affected golden pins are re-recorded by name*):

The record is fixed-width (`NodeKind:1672-1703`, `VALUE_DICTIONARY_HEADER`): 28 bytes today, 32 after.

```
  ValueDictionaryHeaderNode (record at the dictionary namespace's local key 0)
    int32   version              // 0, unchanged; a mismatch still yields unknownLayout() and a decline
    int32   entryCount           // ids 1..entryCount are live; entryCount+1 is the next to mint
    int64   forwardRootKey       // hash-prefix radix root
    int64   reverseRootKey       // id-prefix radix root
    int32   generation           // successful append generations
    int32   orderedPrefixCount   // NEW, appended. ids 1..orderedPrefixCount are in UTF-16 collation
                                 // order of their values; ids above it are in append (first-intern)
                                 // order. 0 for every dictionary built by today's streaming mint.
```

**No version bump, and no version machinery.** The field is appended and read defensively:

```java
final int orderedPrefixCount = source.readRemaining() >= Integer.BYTES ? source.readInt() : 0;
```

`NodeStorageEngineReader:697` does `reusableBytesIn.reset(data, 0)` with `data` the record's own slot segment, so
"absent" is decidable — and the sibling arm `VALUE_DICTIONARY_VALUE_BLOCK` already relies on exactly this, bounding
its claimed sizes against `source.remaining()` before allocating (`NodeKind`, the value-block `deserialize`). So
this is an established idiom in the same enum, not a new dependency, and the default is *semantically correct*: a header written before P2 describes a dictionary
the streaming mint built, whose ids are in intern order, whose ordered prefix is genuinely empty. Bumping `VERSION`
was rejected — it would make every existing dictionary decline (`unknownLayout`), a compatibility break bought for
nothing.

> **Verification obligation, not an assumption.** That the record source is exactly slot-scoped (so
> `readRemaining()` is 0 after 28 bytes rather than some padding) is asserted from `NodeStorageEngineReader:697`
> and **must be pinned by W14 before segment 1 ships**: a header written by a `git show HEAD:`-compiled writer,
> read by a P2 build, yields `orderedPrefixCount == 0`. If the slot turns out not to be exactly scoped, the
> fallback is to bump `VERSION` to 1 and let the existing `unknownLayout` path decline pre-P2 dictionaries — which
> for this campaign costs nothing, because the rank pass rewrites the header anyway.

Invariants, all checked in the constructor beside the existing ones:
`0 <= orderedPrefixCount <= entryCount`; `orderedPrefixCount > 0 ⇒ entryCount > 0`; it is **never decreased** by an
append (an append raises `entryCount` only), and it is set to `entryCount` exactly once, by the rank pass, in the
same transaction that wrote the ranked run.

Serialization site: the `VALUE_DICTIONARY_HEADER` arm of `NodeKind` (the field goes last, so a pre-P2 payload
read by a P2 build is a truncated stream — **that is not permitted**; the arm must write and read the field
unconditionally and the golden pins that cover a dictionary header are re-recorded by name in the same commit).

**Why a boundary and not "the whole dictionary is ordered".** Ordering is a property the *build* establishes; every
later append — a maintenance transaction interning a value that did not exist — can only put its value at the end.
Refusing those appends would make a globally-dictionaried resource read-only, which is not on the table. The
boundary is the smallest honest statement: *the prefix is sorted, the tail is not, and every arm that depends on
order says which it needs.*

### 3.2 The ordered-prefix contract (what every reader may assume)

Let `B = orderedPrefixCount`.

| claim | holds when |
|---|---|
| `compareIds(a,b) == signum(a-b)` | `a <= B && b <= B` |
| `value(a) < literal ⇔ a < lo` for a `lo` found by one binary search | `B == entryCount` (a tail id could belong on either side) |
| a leaf's `(min,max)` id zone map prunes an ordering predicate exactly | `B == entryCount` |
| the id lane is a valid sort key for `ORDER BY <column>` | `B == entryCount` |
| the id lane is a valid *group identity* | always (equality, not order) |
| `count(distinct)` over ids is exact | always |

`B == entryCount` is the normal state after the rank pass and until the first maintenance append. Arms in the
middle column must test it — never `B > 0` — and the test is one field load off the header the arm already reads.
When it fails they either fall back to `compareIds` (correct, slower) or decline; §5 says which, per arm.

### 3.3 Value storage — the read API is unchanged, and why that matters

`ValueDictionaryValueBucketNode` spans 256 consecutive ids; each bucket holds as many
`ValueDictionaryValueBlockNode`s as its values need, each ≤ `MAX_BLOCK_BYTES = 64 KiB`
(`node/ValueDictionaryValueBlockNode.java:44`), values packed **ascending by id** behind a prefix-offset table, and
a value longer than the block target spills to its own `ValueDictionaryEntryNode`. Because the reverse index is
already id-ordered and densely packed, assigning ids in rank order makes the on-disk value store **sorted by
value**, for free, with no layout change: a block is a run of lexicographically adjacent values.

Two consequences the design leans on:

- **Compression.** The blocks live in `KeyValueLeafPage`s of the NamePage sub-trie and go through the ordinary body
  codec bake-off (zero-run vs LZ77, `PageKind.emitSmallestBody` — at 100 M LZ77 wins 100 % of string regions,
  `load.log:106`). **[A]** a ranked dictionary compresses at ~0.45 where an intern-ordered one compresses at ~0.65
  (the ratio the trie's string region achieves on these same values today).
- **Front coding**, which rank order is what makes possible: inside a block each entry becomes
  `(uvarint sharedPrefixLen, suffix bytes)` measured against the **previous entry in the same block only**, so a
  block stays independently decodable and §3.3's bounded tail rewrite is unaffected. §4.1 shows this is
  **mandatory** at the measured cardinality, so it is part of segment 1.

#### 3.3.1 Front-coded value blocks (segment 1, mandatory — see §4.1)

Today (`NodeKind:59 VALUE_DICTIONARY_VALUE_BLOCK`): `int32 firstId, int32 count, int32 byteLength,
int32[count] offsets, byte[byteLength] bytes`. Front coding is added as a **storage form only**, discriminated by
the sign of `byteLength` — the same sign-discrimination pattern B2 used for the order-label lane and B5-d used for
the suppressed-tag list, so it is house style rather than a new idea:

```
  int32   firstId                      // unchanged
  int32   count                        // unchanged
  int32   byteLength                   // >= 0 : PLAIN form, byte-identical to today
                                       // <  0 : FRONT-CODED form; -byteLength is the suffix-region length

  -- PLAIN (byteLength >= 0), unchanged --
  int32[count]      offsets            // prefix offsets, offsets[0] implicit 0
  byte[byteLength]  bytes              // values concatenated, ascending by id

  -- FRONT-CODED (byteLength < 0) --
  uvarint[count]    sharedPrefixLen    // vs the PREVIOUS entry in this block; entry 0 is always 0
  uvarint[count]    suffixLen
  byte[-byteLength] suffixes           // concatenated
```

**The decoded shape and the whole read API are unchanged.** `deserialize` reconstructs `(offsets, bytes)` in one
forward pass with a running buffer and hands them to `takeOwnership` exactly as today, so `rawBytes()`,
`valueOffset(id)`, `valueLength(id)` — and therefore `sliceSlot`, `compareIds`, `stringOpVerdict` and
`lengthTable` — do not change by a line, and no read kernel learns that the stored form moved. The decode is
~64 KiB of `arraycopy` amortised over the block's ~700 values, paid once per block-cache miss; the `ReadView`
already retains 16 decoded blocks (`GVD:120`).

Two obligations carried from the existing decoder's care about corrupt records:

- `-byteLength <= MAX_BLOCK_BYTES` and `count <= MAX_BLOCK_VALUES` are checked before any allocation, as today;
- the **decoded** size is accumulated as `Σ (sharedPrefixLen[i] + suffixLen[i])` and refused above
  `MAX_BLOCK_BYTES` *before* the output array is allocated, and `sharedPrefixLen[i]` is refused when it exceeds the
  previous entry's decoded length. A corrupt record must not be able to make this reserve arbitrary memory or read
  past the previous value. (The existing arm already guards its plain form this way with `source.remaining()`.)

**Election is per block, by measured size:** the encoder builds both forms and writes the smaller; a tie writes
plain. That makes the kill switch `-Dsirix.projection.globalDict.frontCoding=false` provably byte-identical to a
HEAD-compiled encoder (W10), because "off" simply never offers the front-coded candidate.

Front coding only pays because the values are in **rank order** — in intern order adjacent entries are unrelated
and `sharedPrefixLen` is ~0 everywhere, which is why this lane belongs to P2 and could not have been added before
it.

### 3.4 `NamePage` — the global string-tag registry (segment 5 only)

For the trie lane the page-local reader must answer "which dictionary names this tag's ids". The tag *is* a
`pathNodeKey` (`StringRegion.TAG_KIND_PATH_NODE`), so a small resource-level table suffices and costs the page
nothing:

```
  NamePage.globalStringTagRegistry            // serialized with the NamePage, beside maxNodeKeys
    uvarint  entryCount
    per entry (ascending by pathNodeKey):
      svarint pathNodeKeyDelta                // from the previous entry, first from 0
      uvarint dictionaryHeaderNodeKey         // the ValueDictionaryHeaderNode's key
```

At four promoted tags this is ~40 bytes for the whole resource. It is the *only* place that says a tag's string
values live in a dictionary; a page carries no dictionary identity of its own, so a page can never disagree with
another page about which dictionary it means.

### 3.5 `StringRegion` — the `GLOBAL_ID` tag lane (segment 5 only)

`ENC_VARINT_FRAMED` (kind 2, `page/pax/StringRegion.java:151`) frames each retained tag with
`uvarint tagMeta // bit0 plain lane, bits1-2 length width, bits3.. dictionary size`. One bit is taken, in place:

```
  tagMeta  bit0      plain lane (unchanged)
           bits1-2   length width (unchanged)
           bit3      GLOBAL_ID lane                       // NEW
           bits4..   dictionary size (was bits3..)        // shifted by one
```

A `GLOBAL_ID` tag writes **no length table and no value bytes**. In place of them, in the same position the tag's
length table would occupy:

```
  per GLOBAL_ID tag:
    uvarint  idBase         // FOR base: the minimum id under this tag on this page
    byte     idWidth        // bits per id, 0..32; 0 means every id equals idBase
    byte[]   ids            // (tagCount * idWidth + 7) / 8, LSB-first, in slot order
```

Preconditions, each refused loudly at encode time rather than heuristically avoided:
`tagKind == TAG_KIND_PATH_NODE` (a nameKey tag cannot identify a dictionary); the tag's `pathNodeKey` is present in
the registry of §3.4; the lane is never combined with the plain lane on the same tag (rank *is* the id, so a plain
lane would be a second, contradictory identity); a value the dictionary does not hold is a build error, not a
fallback.

**Slot payload.** Value-elision reinjection (`page/PageKind.java:4795-4832`) rebuilds a record's heap payload as
`[flag:1][length:varint][storedBytes]` and *deliberately copies stored bytes verbatim* because "no symbol table is
reachable at deserialize time" — an FSST-compressed value is already handed to the record in a form it cannot
decode alone (`node/json/StringNode.java:758-763` decodes on first access using a table given to it). The global
lane is the same move one step further:

```
  flag byte  0 = raw UTF-8   (unchanged)
             1 = FSST        (unchanged)
             2 = GLOBAL_ID   (NEW)                        // payload is [varint id], no bytes
```

`StringNode.getRawValue()` gains a third arm that resolves through a **resolver bound above the page** (§10.3),
and throws a typed error when none is bound — never returns the id bytes as a value.

---

## 4. Storage arithmetic at 100 M — all inputs measured

### 4.1 The projection half, and the finding that changed the design

Per row, the id lanes are FOR bit-packed over the leaf's `(min,max)`. `min` is 0 whenever a leaf has an absent cell,
so the width is governed by the resource-wide id magnitude, not by leaf locality — stated as a cost, not hidden:
25 + 25 + 24 + 23 bits = **12.125 B/row = 1.21 GB [D from M]**.

The dictionary, at the measured cardinalities and decoded averages:

```
  18,364,684 × 90.46  +  19,966,360 × 81.61  +  9,411,056 × 111.50  +  6,031,488 × 56.17
=      1.661 GB       +       1.630 GB       +       1.049 GB       +      0.339 GB     =  4.679 GB raw
```

| dictionary codec ratio | dictionary | + id lanes | total for the four columns | vs acceptance ≤ 30 B/row |
|---|---|---|---|---|
| 0.65 (LZ77 on intern-ordered values, ≈ what the trie's string region achieves today) | 3.04 GB | 1.21 | 42.5 B/row | **missed by 42 %** |
| 0.45 (LZ77 on rank-ordered values) | 2.11 GB | 1.21 | **33.2 B/row** | **missed by 11 %** |
| 0.40 | 1.87 GB | 1.21 | 30.8 B/row | missed by 3 % |
| **0.35 — the break-even** | 1.64 GB | 1.21 | **28.5 B/row** | met |
| 0.20 (front-coded, then LZ77) | 0.94 GB | 1.21 | **21.5 B/row** | met, 28 % headroom |

**This is the design's most important measured consequence, and it inverts a decision.** With the cardinality
extrapolated from the corpus prefix (11 M for URL) the acceptance held at plain LZ77 on sorted values and front
coding was a follow-up. With the measured 18.4 M it does **not**: sorting alone gets the dictionary to roughly 0.45
and the four columns land at 33.2 B/row, over the ≤ 30 acceptance. **The dictionary must compress to ≤ 0.37, so
front-coded value blocks move from "measured follow-up" into segment 1 as a mandatory part of it.**

Front coding is exactly the encoding rank order exists to enable: inside a 64 KiB block the values are
lexicographic neighbours, so each entry stores `(sharedPrefixLen, suffix)` against its predecessor. On sorted URLs
and Titles — host + path prefixes repeated across long runs — 3–5× before any generic codec is the ordinary result.
**[A] 0.20–0.25 combined.** *Experiment, and it is segment 1's entry gate:* encode the same 5 M-value sample in
(i) intern order raw, (ii) rank order raw, (iii) rank order front-coded, through `PageKind`'s real body codec, and
compare **written** bytes. One bench-package probe, one JVM, ~2 minutes. If (iii) lands above 0.37 the projection
acceptance is unreachable as designed and the brief must be re-scoped before code is written.

Against today's **95.7 B/row [M]**, the projection half at the design figure is **9.57 GB → 2.15 GB, −7.4 GB**.

### 4.2 The trie half — measured, and required for M1

Rebuild #2 measured the trie's string region at 100 M: **written 19.31 GB = 193.1 B/row**, raw 296.3 B/row, LZ77
0.652 (`load.log:106`). The corpus measurement says the four fat columns carry **258.7 B/row** of decoded payload
(§1.1), so they are **87.3 % of the region's raw bytes**. The rest of the region is the other 28 string fields
(`OBJECT_NAMED_STRING` is 3.2 G slots = 32 per row, `load.log:95`).

Under the `GLOBAL_ID` lane those 258.7 B/row become four ids at ~3.03 B = **12.1 B/row raw**:

| | raw B/row | written B/row | 100 M |
|---|---|---|---|
| string region today | 296.3 | 193.1 | **19.31 GB** |
| after: residue (37.6 raw, LZ77 0.652) | 37.6 | 24.5 | 2.45 GB |
| after: id lanes (12.1 raw, **[A]** LZ77 0.85 — ids compress poorly) | 12.1 | 10.3 | 1.03 GB |
| **string region after** | **49.7** | **34.8** | **3.48 GB** |
| | | | **−15.8 GB** |

Plus `stringDictSketch` (1.17 GB written, `load.log:108`): a `GLOBAL_ID` tag has no per-page string dictionary, so
it has nothing to sketch — **[D] −0.9 GB** at the same 87 % share. **Trie half ≈ −16.7 GB.**

### 4.3 M1

| | 100 M | vs M1 (≤ 50 GB) |
|---|---|---|
| **rebuild #2, measured** | **69.63 GB** | |
| − projection half (§4.1) | −7.4 → **62.2 GB** | **missed** |
| − trie lane (§4.2) | −16.7 → **45.5 GB** | **met, 4.5 GB of margin** |

**The projection half alone does not reach M1; the trie lane is what does.** That is the single sentence this
design exists to establish, and both of its inputs are now measurements rather than derivations: the 69.63 GB
baseline and the 19.31 GB string region are both from rebuild #2's own log, and the 258.7 B/row is from the corpus.

Two honest caveats on the 45.5 GB:

- The id-lane compression ratio (0.85) and the front-coding ratio (0.20–0.25) are the two **[A]**s left. The
  break-evens are wide: M1 still holds at a front-coding ratio of 0.45 (→ 47.0 GB) and at an id compression of 1.0
  (→ 45.9 GB). It fails only if *both* miss badly, which the segment-1 gate would catch first.
- Nothing here counts the `OverflowPage` class's 17.45 GB, which holds the projection's segments **and** genuine
  trie overflows of values above `MAX_RECORD_SIZE`. Long Titles and URLs that overflow today would become 3-byte
  ids and stop overflowing. That is a further, unquantified gain deliberately left out of the arithmetic.

### 4.4 What P2 does not pay for twice

The dictionary is counted **once**. If the trie lane ships, the same ~0.94 GB serves both the projection's id lanes
and the trie's, which is the whole reason the two halves belong in one brief — and it is why the trie lane is
cheap *given* the projection half: it costs one bit in `tagMeta`, a registry, and a resolver, and reuses a
structure that is already paid for.

---

## 5. Build pipeline

### 5.1 Why a post-pass, and why it is not a regression

Rank order is a global property of the value set. A streaming mint assigns id `k+1` to the `k+1`-th *distinct value
seen*, and no amount of bookkeeping makes that a rank without knowing every value that follows. So P2's build is a
pass over an already-built index. That is also the cheapest place to put it: the load path is left **byte-identical
and heap-identical** to today (`globalDict=auto` still elects per-leaf DICT for these columns; nothing about
`ProjectionBulkLoad` or the streaming front changes), so P2 cannot regress the ingest (rebuild #2: 2,741 s), and a build that fails
leaves a perfectly good per-leaf projection behind.

The existing streaming mint stays exactly as it is, for the resources it already serves (a few million distinct):
its dictionaries simply carry `orderedPrefixCount = 0` and get the equality-only half of §3.2.

### 5.2 The four stages

Per elected column, one at a time (so the spill peak is one column's, not four):

```
S1 EXTRACT      for each row group in physical order:
                  read the column's DICT segment (≤ 1024 entries, already deduped per leaf)
                  for each entry e:  emit (value, leafId, localId)
                deduplicate inside a bounded run buffer; when it fills, sort the run
                (UTF-16 order, §5.4) and spill it; carry an HLL for the measured D.

S2 MERGE+RANK   k-way merge the runs in UTF-16 order.
                for each distinct value v, in order:
                  rank := rank + 1
                  dictionary.intern(v)                    // mints exactly `rank` — see §5.3
                  for each (leafId, localId) of v:  emit (leafId, localId, rank) to a triple spill
                every 16,384 values: flushStreamingDictionaryGeneration-style rotation.
                at the end: header.orderedPrefixCount := header.entryCount.

S3 SORT TRIPLES external sort the (leafId, localId, rank) triples by (leafId, localId).
                Fixed-width 12-byte records; ~1.14 GB for URL at 100 M; a 2-pass merge.

S4 REMAP        stream row groups in physical order, consuming the matching triple run:
                  build the new BODY long lane from the leaf's local ids via the triples
                  re-encode the column, drop DICT / BLOOM / DICT_HASHES
                  putRowGroupAsColumnSegmentSlots(slot, encoded, changedColumnWords, keysChanged=false)
                flip the column's kind in the metadata blob and record the header key.
```

### 5.3 `intern` in rank order mints the rank — no new mechanism

`GlobalValueDictionaryWriter.intern` assigns ids densely from 1 in call order
(`GlobalValueDictionaryWriter:246-259`, `nextId()` at `:479`), and `GlobalValueDictionaryRadix.append` lays values
into id-ordered blocks. **Feeding the sorted distinct stream to the existing writer therefore produces `id == rank`
with no change to the minting code at all.** Generation rotation
(`ProjectionIndexBuilder.flushStreamingDictionaryGeneration`, `:1711-1745`) already exists and already keeps each
append under `MAX_DISTINCT_ENTRIES_PER_APPEND = 16,384`; S2 reuses it verbatim. The only new obligation is that S2
must *not* use a probe front — it never probes, because a merged sorted stream emits each distinct value once by
construction.

That is worth stating plainly: **the rank pass removes the very structure whose memory made promotion decline.**

### 5.4 UTF-16 order from unsigned byte order — the transform

The engine's collation is UTF-16 code-unit order (`ValueDictionaryEntryNode.compareUtf16Range`), which differs from
UTF-8 byte order in exactly one place: a supplementary character (U+10000+, 4-byte lead `0xF0..0xF4`) sorts *after*
U+E000..U+FFFF (3-byte lead `0xEE`, `0xEF`) in UTF-8 bytes, but *before* it in UTF-16, because it is written as
surrogates `0xD800..0xDFFF`.

`0xEE` and `0xEF` can only ever be **lead** bytes of valid UTF-8 (continuations are `0x80..0xBF`), and `0xFE`/`0xFF`
never occur in valid UTF-8 at all. So the byte-wise substitution

```
  0xEE -> 0xFE      0xEF -> 0xFF      every other byte unchanged
```

is unambiguous, and **unsigned byte order over the transformed bytes is exactly UTF-16 code-unit order.** The
transform is applied in the comparator (or materialised into the sort key), never stored. Order within
`0xE0..0xED` is untouched; `0xF0..0xF4` now precede `0xFE/0xFF`, matching surrogates preceding U+E000.

This makes S1/S2 a plain byte-order external sort — SIMD-comparable, no code-point decoding in the merge — while
being *provably* the collation the query engine uses. W1 (§13) is the proof obligation, and its mutation is
"remove the substitution".

### 5.5 Memory arithmetic at 100 M

Everything the pass holds, per column, with the design defaults:

| structure | bytes | note |
|---|---|---|
| S1 run buffer (values + `(leafId, localId)`) | **512 MB**, configurable | the only tunable; correctness-neutral |
| S1 run dedup index (open-addressed, 16 B/slot at half load) | ≤ 160 MB | sized from the buffer, not from `D` |
| S1 HLL (p=14) | 16 KB | the measured `D` |
| S2 merge cursors: `k` runs × 1 MB read buffer | ≤ 64 MB | `k ≤ 64`; a second merge level beyond that |
| S2 dictionary generation writer | ≤ 2.4 MB | `16,384 × (52 + avg)` — `MAX_DISTINCT_ENTRIES_PER_APPEND` |
| S2 radix append workspace | ≤ 256 MB | `GlobalValueDictionaryRadix.reservationBytesForAppend`, already preflighted |
| S3 external sort of 12-byte triples | **256 MB**, configurable | |
| S4 one row group + its encode buffers | ≤ 2 MB | 1024 rows |
| **peak** | **≈ 1.0 GB** | **independent of `D` and of `rows`** |

Sized as `min(HeapHeadroom.plannedShareBytes, sirix.projection.globalDict.rankPass.bufferBytes)`, i.e. through the
figure B6 already made authoritative (`min(max/8, headroom/4)`); at the campaign's 10 GB load heap that is 1.25 GB.
The pass never allocates from `D`.

**Disk.** S1's spill is the dominant temporary cost:
`Σ_leaves (per-leaf dictionary entries) × (avgLen + 12)`. At 100 M the per-leaf dedup for URL is close to 1.0
(**[M]**: the 100 M URL DICT segments are 3.44 GB *FSST-compressed*, which at a typical 2–2.5× on URLs is
7–8.6 GB raw ≈ 80–95 M entries at 90 B — essentially one dictionary entry per row), so before in-run dedup the
spill is **≈ 9.5 GB** for URL; the 512 MB run buffer removes the locally recurring values, and ClickBench's
counter/time ordering makes that recurrence high. **[A] 4–7 GB in practice.** Plus S3's 1.14 GB. *Experiment:* a
10 M run reports both the raw and the post-dedup spill volume; extrapolation is linear in rows.

A **preflight refuses the pass** when free space is below `2.5 × estimatedSpillBytes + 1.5 × estimatedDictionaryBytes`,
naming the column and the shortfall, and leaves the per-leaf form untouched. URL's peak is ~12–25 GB; four columns
processed one at a time never sum. **This is a live constraint, not a theoretical one:** the campaign disk held
104 GB free before rebuild #2 and holds ~34 GB after it, so the pass over rebuild #2's DB needs either the spill
directory pointed at another device or rebuild #1-era artefacts removed first. The preflight must say which.

### 5.6 Exactness — no hash-only mapping anywhere

The `(leafId, localId, rank)` triples carry the mapping the merge itself established, so a value's rank is never
recovered by hashing. Two distinct values that collide on `valueHash` are separated in S2 by the byte comparison
the merge performs anyway. This is a deliberate rejection of the cheaper design (a resident `(hash64, secondary64)
→ rank` map, ~300 MB for URL): it would be correct with probability `1 − 3e-25`, and that is still a silent
wrong-answer mode in a database. The triples cost one extra external sort of fixed-width records, which is the
cheapest sort in the pipeline.

### 5.7 The boundary field and values crossing promotion mid-build

Because the pass runs *after* the load, nothing crosses promotion mid-build on the bulk path — the class of defect
the plan's question (4) names does not arise there. It arises in exactly two places:

1. **Maintenance appends after the pass.** A commit interning a new value calls `flushAppend`, which raises
   `entryCount` and leaves `orderedPrefixCount` where it was. From that moment `B < entryCount` and §3.2's middle
   column is false: sorted scans, id-range predicates and ordering zone-map pruning stop using the integer order and
   fall back to `compareIds` / decline. Equality, grouping and count-distinct are unaffected. `MaintenanceGlobalDictionary`
   (`ProjectionIndexChangeListener:3120-3227`) needs no change beyond preserving the field.
2. **A concurrent writer during the pass.** The pass runs in one transaction over one revision. If the resource is
   written concurrently, the remap would publish row groups against a moved revision. **The pass takes the
   resource's write transaction for its duration and refuses to start if one is open** — it is a bulk reorganisation,
   not an online one. Stated as a limitation, not engineered around.

A later **re-rank** (fold the tail back into the ordered prefix) is the same pass with the cheaper input adapter of
§5.8, scheduled by an operator or by a measured tail-fraction threshold. Not in this design's scope; the boundary
field is what makes it possible without another format change.

### 5.8 The cheaper input adapter (already-global columns)

When a column is *already* `STRING_GLOBAL` in intern order, S1 does not need to read leaves at all: read the
dictionary's `D` values in id order (sequential over the reverse blocks), sort them, and produce a permutation
`oldId → newId` — `D × 4` bytes, 44 MB for URL, resident. S4 then rewrites each leaf's id lane through the
permutation with no strings involved and no triple spill. This is the maintenance/re-rank path and a strictly
cheaper specialisation; it is listed here so the two are recognised as one operation with two front ends.

---

## 6. Read and serving path, per executor arm

Site references are `EXEC` = `bundles/sirix-query/src/main/java/io/sirix/query/scan/SirixVectorizedExecutor.java`,
`PCS` = `.../index/projection/ProjectionColumnScan.java`, `PIBS` = `.../ProjectionIndexByteScan.java`,
`PIRGP` = `.../ProjectionIndexRowGroupPage.java`, `GVD` = `.../GlobalValueDictionary.java`.

### 6.1 Arms that must be BUILT (they do not exist)

| # | operator | today | P2 |
|---|---|---|---|
| A1 | ungrouped `COUNT(DISTINCT s)` — **q5** | `EXEC:11521` `projectionNumericDistinct` gates on `== COLUMN_KIND_NUMERIC_LONG`; `EXEC:11692` `parallelDistinctPresentStrings` gates on `== COLUMN_KIND_STRING_DICT`. Kind 5 declines both → the whole projection is abandoned and the corpus is rescanned. | A third arm: OR every leaf's present ids into one `long[(D>>6)+1]` bitset and popcount. `D = 6.03 M` for SearchPhrase → **754 KB**, parallel per leaf, merged by `or`. Exact by construction: the id *is* the identity. Budgeted against `HeapHeadroom.plannedShareBytes`; above it, fall back to `GroupDistinctAccumulator`'s bounded hash arm. |
| A2 | ungrouped `MIN`/`MAX(s)` | `EXEC:3774-3776` `tryProjectionStringMinMax` gates on `== COLUMN_KIND_STRING_DICT`; the caller falls to a full typed document scan (`EXEC:3666`). | With `B == entryCount`: `min(id)` over the present cells, then one `valueAsString`. This is the *existing numeric* min/max kernel with a different emitter. With `B < entryCount`: the ids above `B` are compared with `compareIds`, the prefix with integers — a two-tier fold, still one pass. |
| A3 | row materialisation | `EXEC:16784-16809` — the kind switch has no case 5, so `default: return null`, even though `predicateScanFieldValues` (`EXEC:16229`) serves the same column. **q23 (`SELECT *` … `LIMIT 10`) needs this.** | Add the case: buffer ids, resolve the ≤ `rowMaterializeMaxRows` winners in one batched `GlobalValueDictionary.values` call (the same shape `EXEC:16300-16310` already uses). |

### 6.2 Arms that become integer arms (the rank dividend)

| # | site | today | P2 |
|---|---|---|---|
| B1 | `PIRGP:284-295` `isOrderedLongKind` | excludes kind 5 — *"a dictionary id orders by first intern, not by value"* | The comment becomes conditional. `isOrderedLongKind` stays as it is (it is a *static* kind predicate and must not lie); a **new** call-site test `handle.globalIdsAreOrdered(col)` = `header.orderedPrefixCount == header.entryCount` admits kind 5 wherever order is needed. Keeping the two apart is deliberate: a kind predicate that consults a page would be a trap. |
| B2 | `PCS:540-547` `sortColumnsOrderable` | returns `false` for kind 5 → `PCS:481-483` throws → uncapped `ORDER BY` on a global column leaves the projection (`EXEC:16409-16411`, `EXEC:16441-16443`). | Returns `true` when `globalIdsAreOrdered`. The uncapped tuple collector then carries the id as a long, like any numeric key. |
| B3 | `PCS:671-675` best-first disabled for a global first key; `PCS:1079-1085` `leafBestFirstKeys` reads extrema only for `STRING_DICT` | a global top-k walks **every leaf in document order** with neither best-first nor zone pruning | Ordered ids make the BODY zone map `(min,max)` a *value* range: `leafBestFirstKeys` uses `columnZoneRange` directly, and the numeric zone prune at `PCS:742` applies. **This is the q25/q26 fix.** |
| B4 | `PCS:1194-1222` `compareKeyAt` → `GVD.compareIds` (two `sliceSlot` resolutions per comparison, each a possible block decode) | the heap comparison is a dictionary access | `Long.compare(a,b)`. The `ReadView` is still opened, for emission only. |
| B5 | `EXEC:13148-13160` + `EXEC:17384-17400` `GroupOrderPlan.resolve(… keyIsNumeric = numericSingleKey && !globalSingleKey …)` | a global group key is deliberately not "numeric", so `orderPlan == null`, which transitively disables **`HAVING`** (`EXEC:13172`), transformed keys (`EXEC:13165`) *and the dense arm* (`EXEC:15165` wraps `EXEC:15156`) | `!globalSingleKey` becomes `!(globalSingleKey && !globalIdsAreOrdered)`. **This unlocks `DenseGlobalGroupAggTable` for ordered global keys** — the id is already used directly as a dense index (`DenseGlobalGroupAggTable:209`, `:294`), sized from `entryCount`, so q13/q14/q16/q17/q33/q34 get an allocation-free group table *and* an in-kernel `ORDER BY`. |
| B6 | `PIBS:5706-5717` `zoneSkip` returns `false` for any predicate that still carries `stringLitBytes` | correct today: an id bound says nothing about a value bound | Ordering predicates no longer carry a literal (B7 below), so they prune through the ordinary numeric path. The rule at `PIBS:5706` is **unchanged** — it stays the single authority, and it stays right, because a `contains` verdict still carries its literal. |
| B7 | `EXEC:8331-8348` → `globalStringVerdictPredicate` for `STR_LT/LE/GT/GE` | builds an `O(entryCount)` verdict | Two binary searches over the ordered dictionary (`GlobalValueDictionaryRadix` already exposes id→value; the search is over ids, `O(log D)` block reads) produce `lo`/`hi`, and the predicate becomes a numeric range with **no literal retained** — so it prunes exactly (`EXEC:8385-8386` already documents this for translated `EQ`). |
| B8 | `EXEC:12884-12899` grouped deferred string extremum; `EXEC:13245-13255` any global deferred extremum / regex key / composite component / string-length operand forces the **whole-leaf byte arm** | the query hydrates every column of every leaf | With ordered ids the grouped extremum is a numeric min/max lane, so `anyDeferredGlobal` stops forcing `stringSlicedArm` off. `hasGlobalComposite` likewise: `CompositeGroupIdentity:86-94` already gives a global component **one exact lane**. |

### 6.3 Arms that keep declining, and why

- `EXEC:8148-8153` numeric/FP/decimal comparison against a global column — an id is not a quantity. Unchanged.
- `EXEC:2459-2465` `SUM`/`AVG` over a global column. Unchanged.
- `EXEC:12647-12652` `fn:string()` over a global key — the value is not the key; leave it dict-only until a
  measured need appears.
- Any ordering arm when `B < entryCount` (§3.2). It falls back to `compareIds`, which is what the code does today.
- `EXEC:8187-8194` `OP_ARRAY_CONTAINS` (STRING_SET only). Unchanged.

### 6.4 Cost that P2 *introduces*, and must answer for

| site | cost at the measured `D` | answer |
|---|---|---|
| `GVD:239-278` `lengthTable(mode)` — `new int[entryCount+1]` and one `sliceSlot(id)` per id; called at `EXEC:12952` for `AVG(STRLEN(URL))` (**q28**) | **an `int[]` sized by `D` (73.5 MB at the measured 18.36 M) plus a `D`-iteration loop, per query, per worker** | The per-id work is *not* a record decode — `sliceSlot` resolves through the retained bucket and block caches (`GVD:423-473`), so a sequential sweep costs ~`D/700` block decodes and `D` cached array reads. The cost that matters is the array and the loop. Two changes: build the table by walking **blocks** (`valueOffset`/`valueLength` are O(1) off the block's prefix-offset table) instead of by id, and charge the `int[]` against `HeapHeadroom.plannedShareBytes` so the arm **declines above it** rather than allocating. Code-point mode still scans bytes and is charged the same way. |
| `EXEC:14353-14358` regex group key — `valueAsString` per distinct id (**q29**, `REGEXP_REPLACE(Referer, …)`) | **19.97 M** `String` materialisations + regex matches | Already once per *distinct value* rather than per row (100 M → 20 M, a 5× reduction). Bound it with the same headroom check and decline above it; a persistent per-dictionary derived-key lane is a follow-up, not this design. |
| `EXEC:17262-17274` `globalDistinctBitmaps` sizes the bitmap from the **row count**, not the header | `rows/8` = 12.5 MB **per group** at 100 M | Size it from `entryCount` instead (`D/8` = 2.3 MB for URL, 0.75 MB for SearchPhrase), which is both smaller and correct; `DenseGlobalGroupAggTable` already sizes from the header (`EXEC:15570-15575`). One-line change, listed as a defect P2 must fix while it is here. |
| leaf id-lane width | rank order destroys leaf locality: 25 bits for URL where intern order might pack 14–16 | **[D] ≈ +5 B/row over four columns = 0.5 GB** — priced into §4.1 and bought deliberately. A cascade encoding of the id lane (`SchemeSelector` is not applied to `encodeForBitPacked` today, `ProjectionIndexRowGroupCodec:109-128`) is the measured follow-up. |

---

## 7. The verdict strategy

`GVD:313-336` `stringOpVerdict` is a scalar `for (id = 1..entryCount)` sweep, single-threaded on the coordinator
thread, **uncached**, and rebuilt on every serving *attempt* — `convertPredicateLeaf` is reached from 15 distinct
`extractConjunctivePredicates` call sites in `EXEC`. At the measured `D = 18.36 M` for URL that is the query's
critical path (q20, q22, q23).

Three measures, in the order they should land:

1. **Ordering ops stop needing it** (§6.2 B7). `STR_LT/LE/GT/GE` become id ranges. This removes the sweep from
   every ordering predicate at a stroke.
2. **Cache it, per revision, per `(headerKey, op, literal)`.** The verdict is a pure function of those three and is
   already immutable once built (it rides on an immutable `ColumnPredicate`, `ProjectionIndexScan:84`). A small
   bounded cache (default 16 entries, evict-largest) on the registry handle collapses the 15 attempt sites to one
   build. Witness: a counter that reads 1, whose mutation (cache disabled) reads > 1.
3. **Parallelise it, partitioned by BLOCK.** `sliceSlot`'s caches are per-view and unsynchronised, so each worker
   opens its own `ReadView` — which is already the pattern at `EXEC:13945/14200/14307/14949/16044`. Partition the id
   space into contiguous ranges aligned to reverse *buckets* (256 ids) so no two workers decode the same block, and
   `or` the partial bitsets. Expected ~10× on 20 cores; the correctness witness is bit-for-bit equality with the
   serial sweep on a fixture containing spilled entries.

**Not chosen, and why.** *Candidate-only evaluation* (evaluate `contains` only for ids a visited leaf references)
was considered and rejected for the first cut: collecting the referenced-id set costs a pass over the id lane, and
a lazily-filled verdict needs a second "computed" bitset and a branch in the row loop — it trades a bounded
predictable cost for an unbounded unpredictable one. It becomes attractive only with a `LIMIT` small enough that
best-first stops early, which is q23's shape; it is listed as the measured follow-up in §11.

**The real fix for `contains`, staged after the first cut:** a per-block filter written *at dictionary build time*.
Each value block (≤ 64 KiB, ~700 values) gets a 2 KiB 3-gram Bloom filter stored beside it; a `contains(needle)`
tests the filter first and skips blocks whose 3-grams are absent. Cost ~3 % of the dictionary; effect: the sweep
becomes sublinear in `D` for selective needles; at 18.36 M values a URL dictionary holds ~26 K blocks, so a needle
present in 1 % of them turns a 18.36 M-value sweep into a ~184 K-value one. This is the general form of the plan's "per-leaf 3-gram blooms"
fallback, lifted to the place where each value exists once. It needs one new record kind and is therefore a
segment of its own (§12, segment 6).

---

## 8. Residency, pinning and B6

A global dictionary is *shared* across queries and across columns, and the read side is already disk-resident: a
`ReadView` retains 256 slot triples + 16 buckets + 16 decoded blocks — **fixed footprint whatever `D` is**
(`GVD:105-121`, `:160-200`). Nothing about P2 makes an 8 GB heap dictionary.

- **Who pins it: nobody.** `ProjectionResidencyScope` pins *column lanes* in a `ProjectionColumnStore`
  (`ProjectionResidencyScope:103-145` → `store.acquirePin`), and a `ReadView` is not a store lane. It is a
  per-worker, per-operator object bounded by ~1 MB (16 blocks × 64 KiB), created and dropped with the operator.
  Making it scope-pinned would be a category error: it is a cursor, not a fill.
- **What B6 *does* have to account for** are the two `O(D)` heap objects P2 introduces: the verdict bitset
  (`D/8` = **2.3 MB** for URL — negligible) and the length table (`4D` = **73.5 MB** for URL — not negligible when
  four columns and several workers coincide). Both are charged against `HeapHeadroom.plannedShareBytes`, the single figure B6 made
  authoritative, and both **decline above it** rather than allocate. The verdict cache of §7.2 is charged the same
  way.
- **A global column is already priced correctly in the store**: `ProjectionColumnStore:1002-1004` charges
  `rows*8 + presence` for a long lane and its javadoc says why counting it as weightless would be wrong. P2 changes
  none of that. It does *remove* the STRING_DICT arm's `dictByteLen × DICT_DECODED_EXPANSION(=4)` charge
  (`:1010-1024`) for these columns, which is the residency win that comes free with the storage win.
- **Interaction with the 21:14 q3 regression** (the residency share tightening to `min(max/8, headroom/4)`): P2
  makes the fat columns *smaller* to fill (8 B/row instead of 4 B/row + 4× the dictionary), so more of them fit the
  share. P2 must not be measured until that regression is resolved, or the A/B is confounded.

---

## 9. Versioning and the revision story

The dictionary is **append-only, per resource, forever**. That is not a new claim — `ValueDictionaryValueBlockNode`
is documented as never rewriting a closed sub-block, and an append rewrites only the open tail plus a small
directory — but P2 makes the consequences load-bearing, so they are written down:

1. **An id is never reused and never retired.** A revision `r` that stored id `k` must still resolve `k` in every
   later revision, so a value deleted from every row keeps its entry. `entryCount` only grows. Witness W12.
2. **A `ReadView` is bound to a revision** and re-checks it before every operation (`GVD:143-157`, `ensureRevision`),
   so an id can never be interpreted against another revision's dictionary. Unchanged by P2.
3. **`orderedPrefixCount` only grows, and only by a rank pass.** An append never raises it. A reader that sees
   `B < entryCount` knows the tail is unordered without reading it.
4. **Cost of monotonicity.** On an update-heavy resource the dictionary accumulates dead entries. That is the price
   of ids being stable across revisions, and it is the same price the name dictionary already pays. The election
   (§10) therefore requires an **analytical/bulk shape**: the pass is offered for a resource whose measured update
   rate is nil, and a resource marked transactional is never elected. This is a data/statistics gate, not a name.
5. **Garbage collection is out of scope and must stay out.** Compacting the dictionary would renumber ids and
   invalidate every older revision's stored ids — it is a *resource rewrite*, not a maintenance operation. If it is
   ever wanted, it is the re-rank pass of §5.8 applied to every revision at once, which is a different brief.
6. **No format-version machinery.** Per the plan's standing rule the header layout changes in place; the golden
   pins covering a dictionary header are re-recorded by name in the same commit, with the sticky-codec reset rule.

---

## 10. Election — what is measured, when, and what happens on a tie

Nothing in this design consults a column name, a field list, a query id or a benchmark. Every gate is a number the
build already has or can get for free.

### 10.1 Candidate set

A column is a candidate iff (a) its kind is `COLUMN_KIND_STRING_DICT`, (b) its summed per-leaf dictionary entries
over the whole index exceed `MIN_GLOBAL_DICTIONARY_ENTRIES` (existing, `:160`, default 4096), and (c) the resource
has no open write transaction and its measured revision count is 1 (the analytical shape of §9.4).

### 10.2 The measurement (free — S1 already reads everything)

- `perLeafBytes[c]` = **exact**, summed from the row-group descriptors: the BODY + DICT + BLOOM + DICT_HASHES entry
  lengths for column `c`. No estimate; the descriptor carries `byteLen` per segment
  (`ProjectionIndexColumnSegmentCodec:875-876`).
- `D[c]` = the S1 HLL (p=14, ±0.81 %). At election time, before S1 runs, the same HLL over a **16-leaf sample**
  (the existing `SAMPLE_LEAVES`, `:149`) gives a first cut; S1 refines it and the election is re-tested before S2
  commits anything. A column that fails the refined test is abandoned with its spill deleted and its per-leaf form
  untouched.
- `avgLen[c]` = summed value bytes / entries, from the same pass.
- `dictRatio` = the measured post-codec ratio of the first 64 dictionary blocks actually written. **Post-codec, per
  the campaign's standing rule** — never staged bytes.

### 10.3 The rule

```
globalBytes(c) = rows * ceil(log2(D+1)) / 8                 // the id lane
               + D * (avgLen + 6) * dictRatio               // the dictionary, amortised over the resource

elect(c)  iff  globalBytes(c) <= perLeafBytes(c) * (1 - MARGIN)      // MARGIN default 0.10
```

A **tie, or anything inside the margin, declines** — the per-leaf form is cheaper to maintain and already works.
The margin exists so a rebuild is never spent on a 3 % gain.

### 10.4 The trie lane, per tag (segment 5)

```
elect(tag) iff  the tag's pathNodeKey has an elected dictionary
            and stringRegionBytes(tag) >= 4 * idLaneBytes(tag)       // measured over sampled pages
            and tagKind == TAG_KIND_PATH_NODE
```

`stringRegionBytes(tag)` comes from the existing `PageSectionDiag` per-region accounting, sampled over pages, and
compared **post-codec**. Factor 4 rather than 1 because the lane costs a resolver hop on every value read (§10.5),
and a marginal byte gain does not pay for that.

### 10.5 What the trie lane costs on the read path — and the resolver

`page/PageKind.java`'s reinjection has only the region table in hand; there is no reader and no `NamePage` at page
deserialize time. So a `GLOBAL_ID` value is left in the record heap as `[flag=2][varint id]` and resolved lazily by
whoever materialises the value — exactly as an FSST value is left compressed and decoded on first access with a
table it was handed (`StringNode:758-763`). The design adds:

```java
public interface GlobalStringResolver {
  /** Resolve a global id for a pathNodeKey-tagged value; never returns null. */
  byte[] resolve(int pathNodeKey, int id);
}
```

bound **at the transaction**, not at the page: `AbstractNodeReadOnlyTrx` holds one per revision (it already holds
the `StorageEngineReader` and the `NamePage`), and hands it to a record when the record is handed out.
`StringNode.getRawValue()` with `flag == 2` and no resolver bound throws a typed
`IllegalStateException("global-dictionary string value read without a resolver")` — it must never fall back to
returning the id bytes.

**This is the deepest change in the design and the reason segment 5 is last and gated.** Its risks are R4/R5 in §14.

---

## 11. Measured follow-ups (named, not scheduled)

*(Front-coded dictionary blocks were on this list until §1.1's cardinality was measured. At 53.8 M distinct they
are mandatory, not optional, and have moved into segment 1 — see §4.1.)*

1. **Cascade the id lane.** `SchemeSelector`'s BtrBlocks cascade is not applied to `encodeForBitPacked`
   (`ProjectionIndexRowGroupCodec:109-128`); an RLE or delta lane may recover part of the locality rank order
   costs (§6.4, ~0.5 GB at stake).
2. **Candidate-only `contains`** for small-`LIMIT` best-first plans (§7).
3. **A persistent length lane** if `AVG(STRLEN)` stays hot after §6.4's block-walk fix.
4. **The `OverflowPage` class.** Trie values above `MAX_RECORD_SIZE` that become 3-byte ids stop overflowing;
   §4.3 deliberately leaves that gain uncounted.

---

## 12. Staged plan — a gate plus six segments, each gating independently

Each segment is independently revertible, has its own kill switch whose "off" state is proven byte-identical against
a `git show HEAD:`-compiled build, and lands with its witnesses.

| # | segment | acceptance | kill switch |
|---|---|---|---|
| **0** | **The compression gate** (no product code). A bench-package probe encoding a 5 M-value sample through `PageKind`'s real body codec in three forms: intern order raw, rank order raw, **rank order front-coded**. | the front-coded form's post-codec ratio is **≤ 0.37** (§4.1's break-even). **Above it, segment 1 does not start** and the brief is re-scoped. | n/a |
| **1** | **The rank pass, offline, one column** — `orderedPrefixCount` in the header, front-coded value blocks, S1–S4, the election, the disk preflight. No executor change; the column serves exactly as a global column serves today (equality, grouping), just with ordered ids. | At 1 M on a synthetic fixture: the four fat columns' post-codec bytes match the §4.1 arithmetic ±10 %; W1–W6, W10, W12, W14 green; the load path byte-identical. | `-Dsirix.projection.globalDict.rank=false`, `…globalDict.frontCoding=false` |
| **2** | **The integer arms.** §6.2 B1–B8: `globalIdsAreOrdered`, `sortColumnsOrderable`, best-first + zone prune, `compareKeyAt`, `keyIsNumeric`/dense arm, the id-range translation of ordering predicates. | Differentials under `STRICT_SERVING` for q25/q26/q13/q14/q16/q17/q33/q34; W3, W7 green. | `-Dsirix.projection.globalDict.orderedArms=false` |
| **3** | **The three missing arms.** §6.1 A1–A3 + the `globalDistinctBitmaps` sizing defect. | q5 < 1 s at 100 M; q23 materialises; W7 green. | `-Dsirix.projection.globalDict.newArms=false` |
| **4** | **The verdict**: cache + block-partitioned parallel sweep. | q20 within §4 of the plan; W8, W9 green. | `-Dsirix.projection.verdict.parallel=false`, `…verdict.cache=false` |
| **5** | **The trie lane** — registry, `tagMeta` bit3, flag byte 2, the resolver. **Go/no-go gate first: the resolver cost** (R4) — measure point-lookup and full-reconstruction latency against the lane before writing the encoder, on a 1 M resource whose dictionary is already built by segment 1. | The plan's rule holds — *point/row-path queries never slower*; reconstruction round trip byte-identical with the lane on and off; the 100 M leaf class falls by ≥ 12 GB; W11 green. | `-Dsirix.page.stringRegion.globalIdLane=false` |
| **6** | **Per-block 3-gram filters** for `contains`. | q20/q22 sublinear in `D`; a measured false-positive rate ≤ 2 %. | `-Dsirix.projection.globalDict.blockFilter=false` |

Segment 0 is a gate with no product code. Segments 1–4 are the plan's "P2"; segment 5 is the M1 lever; segment 6 is
speed only. **Segments 1 and 2 must land together in one 100 M leg** — segment 1 alone changes storage without
changing speed, and measuring it alone would mis-attribute.

---

## 13. Witnesses, each with the mutation that must fail it

| # | witness | mutation that must fail |
|---|---|---|
| W1 | `RankOrderIsUtf16Order` — a generated value set spanning ASCII, U+0800..U+D7FF, **U+E000..U+FFFF** and **supplementary** characters; the ranks produced by S2 equal the order of `ValueDictionaryEntryNode.compareUtf16Range`. | remove the `0xEE→0xFE / 0xEF→0xFF` substitution → the PUA-vs-supplementary pair inverts. |
| W2 | `RankPassIsExactUnderHashCollision` — two distinct values constructed to collide on `GlobalValueDictionary.valueHash`; both get distinct ranks and every row resolves to its own value. | map `(leafId, localId) → rank` by hash instead of by the merge's triples → one row reads the other's value. |
| W3 | `OrderedPrefixBoundaryIsHonoured` — a dictionary with `B < entryCount`; a sorted scan, an id-range predicate and a zone prune each either decline or produce the interpreter's answer. | test `B > 0` instead of `B == entryCount` → the sorted scan emits the append-order tail in the wrong place (assert the exact wrong sequence, so the guard's absence is *visible*, not merely "different"). |
| W4 | `RemapPreservesEveryValue` — for every row of a fixture, the value read after S4 equals the value read before it, through the interpreter. | start ranks at 0 → an off-by-one on every row; `intern` after the merge rather than during → ids stop being ranks. |
| W5 | `RemapDropsTheAccelerationSegments` — after S4 the row-group descriptor names exactly one segment for the column, and the vanished DICT/BLOOM/DICT_HASHES slots are tombstoned. | skip `tombstoneVanishedColumnSegmentSlots` → the stranded-page assertion fires. |
| W6 | `RankPassIsHeapBounded` — a fixture whose distinct set is 50× the configured buffer **completes**, and the peak retained bytes stay under the declared budget. (Positive witness: it must *finish*, not merely refuse.) | remove the run-buffer spill → the budget guard fires and the pass declines, proving the guard was load-bearing. |
| W7 | `GlobalArmDifferential` — one case per admitted arm (ungrouped count-distinct, ungrouped min/max, uncapped ORDER BY, in-kernel ORDER BY on a global key, row materialisation, grouped deferred extremum), served result == interpreter result under `sirix.query.strictServing=true`. | per arm: emit the id instead of the value; compare ids without the boundary test; size `globalDistinctBitmaps` from rows again (then assert the *bytes*, not the answer). |
| W8 | `VerdictIsBuiltOnce` — a query reaching two serving attempts increments the verdict-build counter once. | disable the cache → the counter reads > 1. |
| W9 | `ParallelVerdictEqualsSerial` — bit-for-bit, on a fixture containing spilled (oversized) entries and a supplementary-character literal. | partition by id range rather than by bucket → two workers race the same block cache; assert the mismatch. |
| W10 | `RankPassKillSwitchIsByteIdentical` — with `-Dsirix.projection.globalDict.rank=false` the built resource is byte-identical to one built by a `git show HEAD:`-compiled encoder (SHA-256 of `sirix.data`), the header field included. | any unconditional write of `orderedPrefixCount` → the digest moves. |
| W11 | `TrieGlobalLaneRoundTrip` — the resource serialises back to byte-identical JSON with the lane on and off; a record read without a bound resolver throws the typed error. | resolve without the tag registry (use the first dictionary) → wrong values for the second promoted tag. |
| W12 | `DictionaryIsAppendOnlyAcrossRevisions` — revision *r*'s ids still resolve after *r+1* appends and after a value is deleted from every row. | reuse a retired id → revision *r* reads the wrong value. |
| W13 | `DerivedHeapIsBudgeted` — the length table and the verdict decline above `HeapHeadroom.plannedShareBytes` rather than allocating. | remove the check → the fixture OOMs at a 256 MB heap. |
| W14 | `PreP2HeaderReadsAsUnordered` — a 28-byte header written by a `git show HEAD:`-compiled writer is read by a P2 build as `orderedPrefixCount == 0` (§3.1's verification obligation). | read the field unconditionally → the deserializer over-reads or throws, proving the defensive read is load-bearing. |

Every guard above demands a **positive** witness in the sense of `guards-must-demand-a-positive-witness`: the suite
is also run with the guard inverted, and each named case must then fail.

---

## 14. Risks

| # | risk | severity | mitigation / decision rule |
|---|---|---|---|
| R1 | **The front-coded dictionary does not compress to ≤ 0.37.** At 0.45 (sorting alone) the four columns land at 33.2 B/row and **miss the ≤ 30 acceptance by 11 %**. | **high — it is the acceptance** | Segment 0 is a gate, not a task: the probe runs and reports before segment 1 starts. If it fails, the honest outcomes are (a) accept a ≤ 35 B/row acceptance and say so, or (b) drop SearchPhrase (0.34 GB of dictionary for 7.4 B/row of payload — the worst ratio of the four) and re-run the election. Both are the lead's call, not this design's. |
| R2 | **This risk retired.** The cardinality was the pending unknown; it is now measured at 53.77 M distinct, **1.7× above** the prefix extrapolation, and §4.1 is priced on the measurement. | — | The residual is the ±0.81 % HLL error, which moves nothing. |
| R3 | **The spill exceeds free disk at 100 M.** ~9.5 GB before in-run dedup for URL. **The machine now holds 75 GB free** after rebuild #2 (69.6 GB of DB), and the campaign will want rebuild #2 kept for the query legs. | medium, and immediate | The §5.5 preflight refuses and names the shortfall; columns are processed one at a time; the spill directory is configurable so it can be pointed at another device. The lead should decide, before segment 1's 100 M run, whether older campaign artefacts come off the disk. |
| R4 | **The trie resolver hop makes point reads and reconstruction slower.** Every fat string value read becomes a dictionary lookup. | high, for segment 5 | The election's factor-4 margin (§10.4); the per-trx resolver holds a `ReadView`-shaped block cache so a scan with locality pays one block decode per ~700 values; the gate measures point-lookup and full-reconstruction latency *before* the segment ships, and the plan's rule "point/row-path queries never slower" is the acceptance. |
| R5 | **The trie depends on a structure the projection introduced.** A secondary index becoming load-bearing for primary data. | high, architectural | The dictionary already lives in the **NamePage** sub-trie (`NamePage:173`, `reserveProjectionValueDictionaryKeys:1316`), i.e. resource metadata beside the name dictionary — not in the projection's blob store. Naming values there is the same move the trie already makes for *names*. The registry of §3.4 is what makes it a resource structure rather than a projection one. If the reviewers reject the inversion, segment 5 dies and M1 needs T1-c's page schema instead (plan §7). |
| R6 | **`B < entryCount` after the first maintenance commit silently degrades every ordering arm.** | medium | Not silent: a `PROJ_DIAG` line and a counter; the decline reason names the boundary. The re-rank pass (§5.8) is the remedy and is cheap for an already-global column. |
| R7 | **Rank order costs id-lane bits.** ≈ +5 B/row over four columns (25 bits for URL where intern order might pack 14–16). | low | Priced in §4.1; §11.1 is the recovery. |
| R8 | **The pass takes the write transaction for its duration.** The resource is unavailable for writes for the length of the pass (**[A]** 10–25 min at 100 M). | medium | Stated as a limitation. It is a bulk reorganisation; an online variant is a different brief. |
| R9 | **`stringOpVerdict` remains `O(D)` for `contains`** until segment 6. | medium | Segment 4's cache + parallelism is the interim; §4 of the plan budgets q20 at 1–2 s, which needs the parallel sweep to reach ~1 s at the measured `D = 18.36 M` — **[A]**, measured in segment 4's leg. |
| R10 | **Measuring P2 against a confounded baseline.** Rebuild #2's **storage** is measured (§1.2) but its **query leg has not run**, and the 21:14 q3 residency regression (B6's share tightening to `min(max/8, headroom/4)`) is unresolved. | high, procedural | The storage half of §4 is already anchored on rebuild #2 and needs nothing further. The **speed** half of the acceptance cannot be judged until rebuild #2's clean leg exists and the q3 regression is either fixed or excluded; §8's last bullet says why P2 must not be A/B'd against a confounded residency rule. |

---

## 15. What this design deliberately does not do

- It does not change the load. `ProjectionBulkLoad`, the streaming front and the existing election are untouched;
  P2 cannot regress the ingest (rebuild #2: 2,741 s).
- It does not add format-version machinery. Four layout changes (§3.1, §3.3.1, §3.4, §3.5), all in place, with the
  affected golden pins re-recorded by name under the sticky-codec reset rule.
- It does not introduce a hash-only mapping anywhere on the correctness path (§5.6).
- It does not collect garbage in the dictionary (§9.5).
- It does not mention a column name, a field list or a query anywhere in main code. The harness
  (`io.sirix.query.bench.*`) remains the only place that knows what URL is.
