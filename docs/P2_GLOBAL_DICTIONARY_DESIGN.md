# P2 — Resource-wide dictionaries for fat string columns

Status: **DESIGN, for review. No code written.** Brief B7 of `docs/STORAGE_AND_SPEED_PLAN.md` (§3 "P2", §6 "B7").
Tree: branch `codex/clickbench-port-rebased-20260827`, HEAD `03e3ed583` (wave 3 committed as `c0d2e8ee5`).
Every number below is one of: **[M]** measured by this brief or quoted from `CAMPAIGN_PROGRESS.md` with its
timestamp, **[D]** derived arithmetically from an [M] with the derivation shown, or **[A]** an assumption, each
carrying the experiment that settles it.

---

## 0. The result, after review

**Status: revised after two independent reviews and the lead's rulings.** What the reviews changed is worth stating
before anything else, because two of the changes moved conclusions rather than wording:

- **The dictionary's directory was priced at zero.** A dictionary is four persisted structures, and the forward
  hash index costs ≈ 8.7 B/row post-codec at the measured cardinality — on its own enough to break the ≤ 30 B/row
  acceptance. §3.3.2 is new: a **rank-ordered dictionary does not need a forward index**, because "which id holds
  this value" is a binary search over the reverse index. Dropping it over the ordered prefix is what makes the
  acceptance reachable, and §4.1 prices the world where reviewers refuse (it misses at every ratio).
- **Segment 5 moved ahead of the serving work.** It delivers −16.7 of the −23.9 GB. Order is now 0 → 1 → 5, then
  2–4.

The rest, in one paragraph. Promotion to `COLUMN_KIND_STRING_GLOBAL` declines at 100 M not because the memory is
needed but because the planner has **no cardinality estimate** and defends against `distinct == rows`
(`ProjectionIndexBuilder:2128-2131`, `:356-364`): the reservation is `4 × rows × (avg + 52)` = **57 GB for URL
alone** against `min(maxMemory/8, 2 GiB)`. Fixing it needs a **measured** cardinality and a build whose heap is
bounded by a spill — both only reachable in a **post-pass**, because the property P2 needs, *ids that sort like
their values*, is global and cannot be produced by a streaming mint. Rank-ordered ids turn ordering operators into
integer work: `MIN(URL)` becomes `min(id)`, `URL < 'x'` becomes an id range that zone maps prune exactly, the
top-k heap comparison becomes `Long.compare`, and the `O(entryCount)` verdict sweep survives only for
`contains`/regex. On measured inputs — **53.77 M distinct values across the four columns** and rebuild #2's own
**69.63 GB** — the projection half reaches ≤ 30 B/row **only if both** front-coded value blocks and the dropped
forward index hold (23.9 B/row; 32.6 with the index kept, 33.3 without front coding), and it lands at 62.4 GB,
so **M1's ≤ 50 GB needs the trie's string region to name the same ids**: that region is 19.31 GB of the 69.63 GB
file and 87 % of it is these four columns. With it, 45.7 GB.

On speed the document deliberately promises nothing yet: **rebuild #2's 43-query leg is running as this is
written**, and it is the only defensible baseline. §6 says which arms change and why; what they are worth is the
leg's to say. One first-draft claim is withdrawn outright — capped `ORDER BY … LIMIT` on a global column is
**already served today** (`EXEC:16377-16385`, `PCS:549-561`); what rank order makes newly possible is the
*uncapped* form, and what it makes fast is the capped one.

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

**Wire changes:** one appended field in one node record (§3.1), the value block's entry framing (§3.3.1), and the
**absence** of the forward hash index over an ordered prefix (§3.3.2) — all three in segment 1; a NamePage registry
(§3.4) and a new `StringRegion` encoding kind (§3.5) in segment 5. **The projection's column segments do not change
at all** — `COLUMN_KIND_STRING_GLOBAL` already encodes as a FOR
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
final int orderedPrefixCount = source.remaining() >= Integer.BYTES ? source.readInt() : 0;
```

**Why "absent" is decidable — the real evidence.** `NodeStorageEngineReader:697` does
`reusableBytesIn.reset(data, 0)` with `data` the record's own slot segment, and the slot is **exactly sized**:
`PageLayout.allocateHeap(page, size)` (`PageLayout:659`) allocates precisely `size` bytes, and
`PageLayout.getRecordOnlyLength(page, slotIndex)` (`:857`) returns the record's byte length from the directory,
minus the DeweyID trailer when one is stored. A 28-byte header therefore leaves `remaining() == 0` after its five
fields. The sibling `VALUE_DICTIONARY_VALUE_BLOCK` arm in the same enum already depends on the same property,
bounding a claimed size against `source.remaining()` before allocating.

The default is *semantically correct*, not merely safe: a header written before P2 describes a dictionary the
streaming mint built, whose ids are in intern order and whose ordered prefix is genuinely empty. Bumping `VERSION`
was rejected — it would make every existing dictionary decline through `unknownLayout`, a compatibility break
bought for nothing. **W14 pins the behaviour anyway** (a header written by a `git show HEAD:`-compiled writer reads
back as `orderedPrefixCount == 0`), because a layout argument that is right in prose and wrong on a
DeweyID-carrying resource is worth ten minutes of test.

Invariants, all checked in the constructor beside the existing ones: `0 <= orderedPrefixCount <= entryCount`;
`orderedPrefixCount > 0 ⇒ entryCount > 0`; it is **never decreased** by an append (an append raises `entryCount`
only), and it is set to `entryCount` exactly once, by the rank pass, in the same transaction that wrote the ranked
run. One existing invariant is **relaxed** by §3.3.2: `(entryCount == 0) != (forwardRootKey == 0 && reverseRootKey
== 0)` becomes "a zero `forwardRootKey` is legal exactly when `orderedPrefixCount == entryCount`".

Serialization site: the `VALUE_DICTIONARY_HEADER` arm of `NodeKind` (`:1672-1703`). The writer emits the field
**unconditionally**; the reader takes it **defensively**, per the snippet above. Those are not in tension — the
asymmetry is the whole mechanism, and it is what lets a P2 build read a pre-P2 resource without a version bump. The
golden pins covering a dictionary header are re-recorded by name in the same commit, under the sticky-codec reset
rule.

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
the sign of `byteLength`. That discrimination works *here* because `byteLength` is a length that is never negative
and the record is small, self-delimiting and read by exactly one arm — it is not offered as a general substitute
for a kind byte (see §3.5, where a new encoding kind is the right answer instead):

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
`lengthTable` — do not change by a line, and no read kernel learns that the stored form moved.

**A block holds at most 256 values, not ~700** [M]: `MAX_BLOCK_VALUES = ValueDictionaryValueBucketNode.VALUES_PER_BUCKET
= 256` (`ValueDictionaryValueBlockNode:49`, `ValueDictionaryValueBucketNode:38`), and a bucket spans exactly 256
ids. For these columns the block is therefore **value-count bound, never byte bound**: 256 URLs at 90 B is 23 KB
against a 64 KiB cap. Consequences, both of which this design got wrong before the review caught it:

- URL's dictionary is **71,737 blocks** (18,364,684 / 256), not ~26 K; a decode is ~23 KB of `arraycopy`, and the
  `ReadView`'s 16 decoded blocks cover 4,096 consecutive ids (`GVD:120`).
- **Front coding's runs are 256 values long.** The shared prefix resets at every block boundary, so the ratio is
  what front coding achieves over 256 sorted neighbours, not over the whole column. That is still the regime where
  it pays — 256 lexicographically adjacent URLs share host and path prefixes — but it is the number segment 0 must
  measure, and it is why segment 0 measures **written dictionary bytes**, not a ratio taken over an unbounded run.

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

#### 3.3.2 The directory — and dropping the forward index over the ordered prefix

**The first draft priced the dictionary as `D × avgLen` and charged its directory nothing. That was wrong, and it
is the finding that decides the brief.** A dictionary is four persisted structures, not one
(`NodeKind` arms 39/41/57/58/59, `GlobalValueDictionaryRadix:25-27`):

| structure | per entry | per bucket / node | notes |
|---|---|---|---|
| `ValueDictionaryValueBlockNode` (reverse values) | `int32` offset (4 B, replaced by front coding's two uvarints) + the value bytes | 12 B header per 256 ids | the part §4.1 priced |
| `ValueDictionaryValueBucketNode` (reverse directory) | — | 16 B + 16 B per block, per 256 ids | ≈ **0.13 B/entry** |
| reverse radix (`REVERSE_PATH_BYTES = 3`) | — | over `D/256` buckets | small |
| **`ValueDictionaryHashBucketNode` (forward index)** | **`long hash` 8 + `int id` 4 = 12 B** | 25 B framing per bucket | **the missing cost** |
| **forward radix (`PRIMARY_PATH_BYTES = 3` over a 24-bit bucket space)** | — | one node per occupied 8-bit stride | **the second missing cost** |

`hashBucket(hash) = (hash >>> 40) & BUCKET_MASK` (`GlobalValueDictionaryRadix`) gives a **24-bit, 16.78 M-slot**
primary bucket space. At `D = 18,364,684` the load factor is 1.09, so under a Poisson model
**[D]** `16.78 M × (1 − e^(−1.094)) ≈ 11.2 M buckets are occupied, most holding a single entry`:

```
  hash buckets   11.2 M × 25 B framing  +  18.36 M × 12 B payload   =  279 MB + 220 MB  =  499 MB
  forward radix  ~65,536 third-level nodes × ~2.3 KB                 =              ~151 MB
                                                                      ------------------------
  URL forward index, raw                                                            ~650 MB
                                                                       = 35.4 B per entry
```

The 12 B payload is **~incompressible** (40 random hash bits plus a rank id that is uncorrelated with hash order);
the 25 B framing is a near-constant record and compresses hard. **[D]** post-codec ≈ 296 MB for URL ≈ 16 B/entry,
which over the four columns' 53.77 M entries is **≈ 867 MB = 8.7 B/row** — larger than the front-coded *values*.

**The design's answer: a rank-ordered dictionary does not need a forward hash index.** The forward index answers
"which id holds this value", and over the ordered prefix that is a **binary search over the reverse index** —
`O(log(D/256))` ≈ 16 bucket/block reads, using structures that already exist. So:

```
  forwardRootKey == 0  and  orderedPrefixCount == entryCount   ⇒  probe by binary search, no forward index at all
  forwardRootKey != 0                                          ⇒  the forward index covers the UNORDERED TAIL only
```

A maintenance append mints above the boundary and inserts into a forward index built over the tail alone, so a
freshly rank-passed resource carries **zero** forward-index bytes and one that has been appended to carries an
index proportional to the appends, not to `D`.

Two obligations this creates:

- **The header invariant must be relaxed.** `ValueDictionaryHeaderNode`'s constructor asserts
  `(entryCount == 0) != (forwardRootKey == 0 && reverseRootKey == 0)`, which forbids a live dictionary with no
  forward root. It becomes: a zero `forwardRootKey` is legal exactly when `orderedPrefixCount == entryCount`. That
  is one predicate, and W15 is its witness.
- **Probes get slower and it must be said.** A literal probe costs ~16 page reads instead of ~5 radix decodes.
  That is free on the query path — `probe` is called **per literal, not per row** (`EXEC:8437` `globalStringPredicate`,
  `EXEC:13322` conditional-else literals) — but the maintenance interner
  (`ProjectionIndexChangeListener:3183-3193`) probes **per new value**, so incremental maintenance on a
  rank-ordered column pays ~3× per newly interned value. Stated as a cost, accepted, and bounded by the fact that
  such a column's maintenance is already the slow path.

If the reviewers reject dropping the forward index, §4.1 prices that world too: it costs **+8.7 B/row** and the
≤ 30 B/row acceptance is then missed at every front-coding ratio.

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

### 3.5 `StringRegion` — a new encoding kind 3 for the `GLOBAL_ID` lane (segment 5 only)

The first draft took bit 3 of `tagMeta` and shifted the dictionary size into bits 4.., which silently reinterprets
every existing kind-2 region. **The review is right that this is the wrong shape**, and the right one is already
established in this very file: `ENC_VARINT_FRAMED = 2` (`page/pax/StringRegion.java:151`) was itself added as a new
encoding kind beside `ENC_DICT_BITPACKED_ZM = 0` when the framing changed. A new kind is not format-version
machinery — it is a discriminated union whose tag byte already exists and is already dispatched on
(`StringRegion:294`, `:381`).

```
  ENC_GLOBAL_ID_FRAMED = 3          // identical to kind 2, plus per-tag GLOBAL_ID lanes

  byte    encodingKind = 3
  byte    tagKind                   // must be TAG_KIND_PATH_NODE; kind 3 with TAG_KIND_NAME is corrupt
  byte    flags                     // bit0 = array-element staging ran (unchanged from kind 2)
  uvarint retainedTagCount
  uvarint suppressedTagCount
  per retained tag:
    svarint parentDictDelta         // unchanged
    uvarint tagCount                // unchanged
    uvarint tagMeta                 // UNCHANGED MEANING: bit0 plain lane, bits1-2 length width,
                                    //   bits3.. dictionary size
    uvarint globalLane              // NEW, kind 3 only: 0 = ordinary tag (kind-2 semantics),
                                    //   1 = GLOBAL_ID lane
  per suppressed tag:
    svarint tagDelta
  per retained tag with globalLane == 0:   length table + entry bytes, exactly as kind 2
  per retained tag with globalLane == 1:
    uvarint idBase                  // FOR base: the minimum id under this tag on this page
    byte    idWidth                 // bits per id, 0..32; 0 means every id equals idBase
    byte[]  ids                     // (tagCount * idWidth + 7) / 8, LSB-first, in slot order
  byte[]  valueDictIds              // DICT-lane tags only, exactly as kind 2
```

Kind 2 stays byte-for-byte what it is; a page with no `GLOBAL_ID` tag is still written as kind 2, so the kill
switch's byte-identity proof is "kind 3 is never emitted", not "kind 3 decodes the same". A reader that meets
kind 3 without the lane compiled in declines the page rather than misparsing it, which is what a kind byte buys and
a repurposed bit does not.

Preconditions, each refused loudly at encode time: `tagKind == TAG_KIND_PATH_NODE`; the tag's `pathNodeKey` is in
§3.4's registry; `globalLane == 1` is never combined with the plain lane on the same tag (rank *is* the id, so a
plain lane would assert a second, contradictory identity); a value the dictionary does not hold is a build error.

**Slot payload.** Value-elision reinjection (`page/PageKind.java:4795-4832`) rebuilds a record's heap payload as
`[flag:1][length:varint][storedBytes]` and *deliberately copies stored bytes verbatim* because "no symbol table is
reachable at deserialize time" — an FSST-compressed value is already handed to the record in a form it cannot
decode alone (`node/json/StringNode.java:758-763` decodes on first access with a table it was given). The global
lane is the same move one step further:

```
  flag byte  0 = raw UTF-8   (unchanged)
             1 = FSST        (unchanged)
             2 = GLOBAL_ID   (NEW)                        // payload is [varint id], no bytes
```

`StringNode.getRawValue()` gains a third arm that resolves through a **resolver bound above the page** (§10.5), and
throws a typed error when none is bound — never returns the id bytes as a value.

## 4. Storage arithmetic at 100 M — recomputed after review

> **What changed in this revision.** The first draft priced the dictionary as `D × avgLen` and charged its
> **directory** nothing (§3.3.2). Adding it moves the projection half from "comfortable" to "met only because the
> forward hash index is dropped over the ordered prefix". The M1 target is also restated: the plan of record
> (`STORAGE_AND_SPEED_PLAN.md` §2) says **≤ 50 GB (expected 45–50)**; the ledger's "≤ 45 GB" is from superseded
> draft 3 and is not the target.

### 4.1 The projection half

**Id lanes** [D from M]. FOR bit-packed over the leaf's `(min,max)`; `min` is 0 whenever a leaf has an absent cell,
so the width is the resource-wide id magnitude, not leaf locality: 25 + 25 + 24 + 23 bits =
**12.125 B/row = 1.21 GB**.

**Value region** [D from M, on an [A] average — see below]:

```
  18,364,684 × 90.46  +  19,966,360 × 81.61  +  9,411,056 × 111.50  +  6,031,488 × 56.17  =  4.68 GB raw
```

> **[A], flagged by the review and accepted.** Those averages are **occurrence-weighted** (measured over every row),
> but the dictionary stores each value once, so the right multiplier is the **distinct-weighted** mean length. The
> two differ, and the likely sign is that distinct values are *longer* — short and empty values recur far more than
> long ones — so 4.68 GB is probably an **under**-estimate. *Experiment, cheap and exact enough:* take a 1-in-64
> hash-prefix sample of values (287 K for URL — fits trivially in memory), dedupe it exactly, and take the mean
> length of the distinct sample; it is an unbiased estimator of the distinct-weighted mean. S1 also reports the
> figure exactly, for free, on its first real run. **Not run yet: the 43-query leg owns the machine.**

**Directory** [D, §3.3.2]: reverse buckets and block headers ≈ 0.17 B/entry → 53.77 M × 0.17 = 9 MB ≈ **0.1 B/row**.
The forward hash index is **not built over the ordered prefix**, so it contributes 0 for a freshly rank-passed
resource; keeping it would add **≈ 8.7 B/row**.

| front-coded value-region ratio | values | + ids | + directory | total | vs ≤ 30 B/row |
|---|---|---|---|---|---|
| 0.20 | 9.4 B/row | 12.1 | 0.1 | **21.6** | met, 28 % headroom |
| **0.25 (design figure [A])** | 11.7 | 12.1 | 0.1 | **23.9** | **met, 20 % headroom** |
| 0.30 | 14.0 | 12.1 | 0.1 | **26.2** | met, 13 % |
| 0.37 — break-even | 17.3 | 12.1 | 0.1 | **29.5** | met, exactly |
| 0.45 (sorting alone, no front coding) | 21.1 | 12.1 | 0.1 | **33.3** | **missed by 11 %** |
| 0.25 **with the forward index kept** | 11.7 | 12.1 | 8.8 | **32.6** | **missed by 9 %** |

**Two things must both hold for the ≤ 30 B/row acceptance: front coding at ≤ 0.37, and no forward hash index over
the ordered prefix.** Neither is a nicety; the last row shows the directory alone breaks it. Against today's
**95.7 B/row** [M] the projection half at the design figure is **9.57 GB → 2.39 GB, −7.2 GB**.

**Segment 0's gate is therefore restated:** it measures **written dictionary bytes for a real column — value blocks
plus every directory record — divided by rows**, not a value-block compression ratio. A ratio taken over the values
alone is exactly the measurement that hid the directory in the first place.

### 4.2 The trie half — measured, and required for M1

Rebuild #2 measured the trie's string region at 100 M: **written 19.31 GB = 193.1 B/row**, raw 296.3 B/row, LZ77
0.652 (`load.log:106`) [M]. The corpus says the four fat columns carry **258.7 B/row** of decoded payload (§1.1)
[M], so they are **87.3 % of the region's raw bytes** [D]. The rest is the other 28 string fields
(`OBJECT_NAMED_STRING` is 3.2 G slots = 32 per row, `load.log:95`) [M].

Under the `GLOBAL_ID` lane those 258.7 B/row become four ids at ~3.03 B = **12.1 B/row raw** [D]:

| | raw B/row | written B/row | 100 M |
|---|---|---|---|
| string region today [M] | 296.3 | 193.1 | **19.31 GB** |
| after: residue (37.6 raw, LZ77 0.652) [D] | 37.6 | 24.5 | 2.45 GB |
| after: id lanes (12.1 raw, **[A]** LZ77 0.85 — ids compress poorly) | 12.1 | 10.3 | 1.03 GB |
| **string region after** [D] | **49.7** | **34.8** | **3.48 GB** |
| | | | **−15.8 GB** |

Plus `stringDictSketch` (1.17 GB written, `load.log:108` [M]): a `GLOBAL_ID` tag has no per-page string dictionary,
so it has nothing to sketch — **[D] −0.9 GB** at the same 87 % share. **Trie half ≈ −16.7 GB.**

### 4.3 M1

Target, from the plan of record (`STORAGE_AND_SPEED_PLAN.md` §2): **M1 ≤ 50 GB, expected 45–50.**

| | 100 M | vs M1 ≤ 50 GB |
|---|---|---|
| **rebuild #2, measured** [M] | **69.63 GB** | |
| − projection half (§4.1) | −7.2 → **62.4 GB** | **missed** |
| − trie lane (§4.2) | −16.7 → **45.7 GB** | **met, 4.3 GB of margin, inside the expected 45–50 band** |

**The projection half alone does not reach M1; the trie lane delivers −16.7 of the −23.9 GB.** That is the sentence
this design exists to establish, and it is why §12 now stages segment 5 *before* the serving segments.

Caveats on the 45.7 GB, all named:

- Three **[A]**s remain: the front-coding ratio (0.20–0.25), the id-lane compression (0.85), and the
  distinct-weighted mean length. M1 still holds at a front-coding ratio of 0.45 (→ 47.2 GB) and at id compression
  1.0 (→ 46.1 GB); it fails only if several miss together, which segment 0 catches first.
- Nothing counts the `OverflowPage` class's 17.45 GB, which holds the projection's segments **and** genuine trie
  overflows above `MAX_RECORD_SIZE`. Long Titles and URLs that overflow today become 3-byte ids and stop
  overflowing — a further, unquantified gain left out.

### 4.4 What P2 does not pay for twice

The dictionary is counted **once**. If the trie lane ships, the same ~1.2 GB serves both the projection's id lanes
and the trie's — which is why the two halves belong in one brief, and why the trie lane is cheap *given* the
projection half: one encoding kind, a registry, and a resolver over a structure already paid for.

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
                  read the column's DICT segment (<= 1024 entries, already deduped per leaf)
                  for each entry e:  VALIDATE well-formed UTF-8 (§5.4), then emit (value, leafId, localId)
                deduplicate inside a bounded run buffer; when it fills, sort the run
                (UTF-16 order, §5.4) and spill it; carry an HLL and the exact distinct
                byte total (the distinct-weighted mean length §4.1 needs).

S2 MERGE+RANK   k-way merge the runs in UTF-16 order.
                for each distinct value v, in order:
                  rank := rank + 1
                  appender.accept(v)                      // mints exactly `rank` — §5.3
                  for each (leafId, localId) of v:  emit (leafId, localId, rank) to a triple spill
                the appender rotates + COMMITS every 16,384 values (§5.3, §5.5).
                It holds NO probe front — a merged sorted stream never probes.
                at the end: header.orderedPrefixCount := header.entryCount, and
                header.forwardRootKey := 0 (§3.3.2, no forward index over an ordered prefix).

S3 SORT TRIPLES external sort the (leafId, localId, rank) triples by (leafId, localId).
                Fixed-width 12-byte records; ~1.14 GB for URL at 100 M; a 2-pass merge.

S4 REMAP        stream row groups in physical order, consuming the matching triple run:
                  build the new BODY long lane from the leaf's local ids via the triples
                  re-encode the column, drop DICT / BLOOM / DICT_HASHES
                  putRowGroupAsColumnSegmentSlots(slot, encoded, changedColumnWords, keysChanged=false)
                COMMIT every rowGroupsPerCommit row groups (§5.5).
                FINAL commit only: flip the column's kind in the metadata blob and record
                the header key — so a crash mid-S4 leaves a still-per-leaf column (§5.5).
```

### 5.3 `intern` in rank order mints the rank — but the append path must be front-less

`GlobalValueDictionaryWriter.intern` assigns ids densely from 1 in call order (`:246-259`, `nextId()` at `:479`),
and `GlobalValueDictionaryRadix.append` lays values into id-ordered blocks. **Feeding the sorted distinct stream to
the existing writer therefore produces `id == rank` with no change to the minting code.** That much of the first
draft stands.

**What does not stand is reusing `flushStreamingDictionaryGeneration` verbatim.** The review is right: that method
(`ProjectionIndexBuilder:1711-1745`) promotes a raw `GlobalValueDictionaryWriter` into a `StreamingGlobalDictionary`
on its first call, and that constructor **builds the resident probe front and seeds it with every entry**
(`:1779-1800`, `copyValueToProbeFront` per id). Using it would reintroduce, entry by entry, exactly the
`D × (22 + avgLen)` structure this brief exists to remove — 5.86 GB for the four columns (§1.3).

So S2 uses a **front-less append path**, which is a smaller thing than the streaming one, not a bigger one:

```
  RankPassDictionaryAppender          // owns nothing across generations except the header key
    generation : GlobalValueDictionaryWriter   // bounded by MAX_DISTINCT_ENTRIES_PER_APPEND = 16,384
    headerKey  : long                          // 0 until the first flush

    accept(value):
      generation.intern(value)                 // ids are dense within the generation
      if generation.entryCount() == MAX_DISTINCT_ENTRIES_PER_APPEND:  rotate()

    rotate():
      headerKey = (headerKey == 0)
          ? generation.flush(namePage, dbType, writer, log)
          : generation.flushAppend(baseHeader(), namePage, dbType, writer, log)
      generation.release()
      generation = new GlobalValueDictionaryWriter(column, budget)
      commit()                                 // see §5.5 on the intent log
```

**No probe front, and none is needed** — a merged sorted stream emits each distinct value exactly once by
construction, so nothing is ever probed. That is worth stating plainly: **the rank pass does not merely bound the
structure whose memory made promotion decline; it has no use for it.** `StreamingGlobalDictionary` and its front
stay exactly as they are for the streaming path they were built for.

The generation writer's own budget is `MINIMUM_BUDGET_BYTES` plus the generation's values — ~2.4 MB at
`16,384 × (52 + avgLen)` — so the `AdmissionPolicy` is `FAIL_CLOSED`: a refusal here is a defect, not a decline.

### 5.4 UTF-16 order from unsigned byte order — the transform

The engine's collation is UTF-16 code-unit order (`ValueDictionaryEntryNode.compareUtf16Range`), which differs from
UTF-8 byte order in exactly one place: a supplementary character (U+10000+, 4-byte lead `0xF0..0xF4`) sorts *after*
U+E000..U+FFFF (3-byte lead `0xEE`, `0xEF`) in UTF-8 bytes, but *before* it in UTF-16, because it is written as
surrogates `0xD800..0xDFFF`.

**Precondition: the values are valid UTF-8.** The whole argument rests on it, so it is a checked precondition and
not an assumption. `0xEE` and `0xEF` can only ever be **lead** bytes of valid UTF-8 (continuations are
`0x80..0xBF`), and `0xFE`/`0xFF` never occur in valid UTF-8 at all. Under that precondition the byte-wise
substitution

```
  0xEE -> 0xFE      0xEF -> 0xFF      every other byte unchanged
```

is unambiguous, and **unsigned byte order over the transformed bytes is exactly UTF-16 code-unit order.** The
transform is applied in the comparator (or materialised into the sort key), never stored. Order within
`0xE0..0xED` is untouched; `0xF0..0xF4` now precede `0xFE/0xFF`, matching surrogates preceding U+E000.

This makes S1/S2 a plain byte-order external sort — SIMD-comparable, no code-point decoding in the merge — while
being *provably* the collation the query engine uses.

**S1 validates, and a violation is a build error.** Every value entering the extract pass is checked for
well-formed UTF-8 (correct continuation counts, no overlong forms, no encoded surrogate `0xED 0xA0..0xBF`, no
`0xC0/0xC1/0xF5..0xFF`), and a violation **fails the pass by name** rather than being sorted into a silently wrong
order. This is cheap — one pass over bytes S1 is already copying — and it is the only thing standing between a
malformed value and an ordering that `compareUtf16Range` would disagree with. `ValueDictionaryEntryNode`'s
`decodeCodePoint` already assumes the same well-formedness on the read side, so P2 is checking an invariant the
existing code relies on rather than inventing one.

W1 (§13) is the proof obligation. Its mutation is "remove the substitution", and its **generator must include
unpaired and encoded surrogates and overlong forms** so that the validation arm is exercised, not just the ordering
arm.

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
| **the transaction intent log** | **bounded only by the commit cadence** | **the term the first draft omitted** |
| **peak** | **≈ 1.0 GB + one epoch's intent log** | independent of `D` and of `rows` **only because of the commit cadence below** |

Sized as `min(HeapHeadroom.plannedShareBytes, sirix.projection.globalDict.rankPass.bufferBytes)`. **`plannedShareBytes`
is consulted here only as a sizing hint for one build-side buffer — it is not a residency rule, and §8 says why P2
must not revive one.**

**The intent log is the term that was missing, and without it "≈ 1.0 GB independent of `D`" is simply false.** Every
`put` in S2 and every `putRowGroupAsColumnSegmentSlots` in S4 stages a dirty page in the transaction's
`TransactionIntentLog`, which is not released until commit. Over a whole column that is the entire rewritten
dictionary plus every rewritten row group — for URL roughly 1.7 GB of dictionary records and 1.2 GB of segments —
held at once. The design therefore **commits per epoch**:

- **S2 commits after each `rotate()`**, i.e. every 16,384 distinct values. The dictionary is append-only and each
  generation is already an immutable segment, so that is exactly the boundary `flushAppend` defines: a crash
  between generations leaves a shorter but internally consistent dictionary, with `orderedPrefixCount` still at its
  old value and therefore no reader believing the prefix is sorted.
- **S4 commits every `sirix.projection.globalDict.rankPass.rowGroupsPerCommit` row groups** (default **1,024**; at
  ~24 KB of rewritten segments per row group that is ~25 MB of log per epoch). A crash mid-S4 leaves some row
  groups converted and some not — handled because the column's kind flip and the header's `orderedPrefixCount` are
  published in the **final** commit, so until then every reader still sees a per-leaf column and the partial work
  is invisible.
- Peak intent log is therefore **one epoch, ≈ 25–50 MB**, and the `-Dsirix.til.diag` census staying flat across the
  pass is W6's second assertion.

`GlobalValueDictionaryRadix.reservationBytesForAppend` already models `pageAndIntentLogBytes` as
`2 × (encodedBytes + records × RECORD_STRIDE)` for a *single* append and preflights it through
`ensureAppendWorkspaceFitsBudget`. The pass adds nothing to that model — it only stops accumulating it across
generations.

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

**A re-rank cannot renumber ids in place on a multi-revision resource, and the first draft implied it could.**
§9.1 says an id is never reused and never retired, because a revision that stored id `k` must still resolve `k`. A
re-rank that renumbers would break exactly that for every older revision. The design therefore names two explicit
contracts and asks the reviewers to pick:

- **(a) `(orderedFrom, orderedTo)` instead of a single boundary.** The header carries a *range* of ids that is
  sorted. A re-rank appends a freshly sorted run of the tail's values **as new ids** and publishes the new range;
  old ids keep resolving, the column's leaves are remapped in the same pass, and the superseded ids become dead
  entries. Cost: the dictionary grows by the tail at every re-rank and dead entries accumulate — §9.4's
  monotonicity price, paid again.
- **(b) Re-rank is a single-revision operation.** It renumbers in place and is legal only on a resource with one
  revision (or one whose history is being discarded anyway), refusing by name otherwise. Cheap, exact, no dead
  entries, and honest about being a bulk reorganisation of a resource nobody is time-travelling in.

**Recommendation: (b)** for this campaign — rebuild #2 has exactly one revision, and (a) buys a capability nothing
has asked for at a recurring cost. (a) is on the record so the choice is deliberate rather than discovered later.
Either way the `orderedPrefixCount` field of §3.1 is what the *first* pass needs, which is all segment 1 depends
on; the range form is a widening of that same field, and W12 pins whichever is chosen.

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

> **Evidence tags.** Every line number and quoted guard below is **[M]** — read from the tree at `03e3ed583`.
> Every *effect* claim is **[D]** (it follows from the guard) or **[A]** (it predicts a runtime that has not been
> measured). **There is no defensible speed baseline yet:** the only per-query numbers in the plan are from the
> pre-wave-3 build, and rebuild #2's 43-query leg is running now. No timing target in this section is asserted;
> the leg supplies the baseline against which segments 2–4 are judged.

### 6.1 Arms that must be BUILT (they do not exist)

| # | operator | today [M] | P2 |
|---|---|---|---|
| A1 | ungrouped `COUNT(DISTINCT s)` — **q5** | `EXEC:11521` `projectionNumericDistinct` gates on `== COLUMN_KIND_NUMERIC_LONG`; `EXEC:11692` `parallelDistinctPresentStrings` gates on `== COLUMN_KIND_STRING_DICT`. Kind 5 declines both, so the projection is abandoned and the corpus rescanned. | OR every leaf's present ids into one `long[(D>>6)+1]` bitset and popcount. `D = 6.03 M` for SearchPhrase → **754 KB** [D]. Exact by construction: the id *is* the identity. **Preconditions (all three, from the review):** ① `handle.columnSparseClean(col, …)` must hold — it is `false` when the column ever saw a present-but-unrepresentable value (JSON null, object/array, kind mismatch, `ProjectionIndexRegistry:557-568`), and such a column would give a wrong count; ② the **empty-sequence contract** — `count(distinct-values(()))` is `0`, so a column with no present row must return `0`, not decline and not `1` for the absent-id 0 slot, and id 0 is never a member (`PIRGP:1537-1540` stores 0 for absent); ③ budgeted against the heap share, falling back to `GroupDistinctAccumulator`'s bounded arm above it. |
| A2 | ungrouped `MIN`/`MAX(s)` | `EXEC:3774-3776` `tryProjectionStringMinMax` gates on `== COLUMN_KIND_STRING_DICT`; the caller falls to a full typed document scan (`EXEC:3666`). | With `B == entryCount`: `min(id)` over present cells, then one `valueAsString`. Same three preconditions as A1, plus a fourth: the emitter resolves exactly one id, and if that resolution answers `GlobalValueDictionary.ID_UNKNOWN` or the `ReadView` is `null` (`GVD:143-157` refuses an incomplete or moved-revision dictionary) the arm **declines** — it must never emit an id, a null, or a guess. `fn:min(())` is the empty sequence, so a column with no present row declines to the interpreter rather than inventing a result. With `B < entryCount` the tail ids are compared with `compareIds` and the prefix with integers — one pass, two tiers. |
| A3 | row materialisation | `EXEC:16784-16809` — the kind switch has no case 5, so `default: return null`. | **Adding `case 5` is necessary but not sufficient, and the first draft claimed it would serve q23. It would not.** `ROW_MAT_MAX_ROWS` (`EXEC:16922`, default 1,000,000) is a cap on the store's **TOTAL rows**, not on the query's `LIMIT`: its javadoc says *"Stores above this many TOTAL rows decline"*. At 100 M rows `executeRowMaterialize` declines before any column kind is examined. **So A3 is scoped to what it can honestly do — it makes `SELECT *` over a global column work on stores under the cap — and lifting the cap for `LIMIT`-bounded plans is its own step (segment 3b) with its own acceptance, because it means making the route streaming or limit-aware rather than eagerly materialising.** q23 at 100 M is that step's problem, not A3's. |

### 6.2 Arms that become integer arms (the rank dividend)

| # | site [M] | today | P2 |
|---|---|---|---|
| B1 | `PIRGP:284-295` `isOrderedLongKind` excludes kind 5 — *"a dictionary id orders by first intern, not by value"* | — | The kind predicate stays exactly as it is (a static predicate that consults a page would be a trap). A **new** call-site test `handle.globalIdsAreOrdered(col)` = `header.orderedPrefixCount == header.entryCount` admits kind 5 where order is needed. |
| B2 | `PCS:540-547` `sortColumnsOrderable` returns `false` for kind 5 → `PCS:481-483` throws; **and** `EXEC:16409-16410` `if (anyStringKey && !(sliced && limit >= 0)) return null;` **and** `EXEC:16442-16443` `if (anyStringKey) return null;` | an **uncapped** `ORDER BY` on a global column leaves the projection | All three relax together on `globalIdsAreOrdered` — the review caught that relaxing `PCS` alone changes nothing, because the executor refuses before the kernel is reached. `anyStringKey` becomes "any string key that is not an ordered global column". |
| B3 | `PCS:671-675` disables best-first for a global first key; `PCS:1079-1085` `leafBestFirstKeys` reads extrema only for `STRING_DICT`; the zone prune at `PCS:742` is gated `keyKind[0] == KEY_NUMERIC` | a global top-k walks **every leaf in document order** with neither best-first nor zone pruning | Ordered ids make the BODY zone map `(min,max)` a *value* range, so `leafBestFirstKeys` uses `columnZoneRange` and the numeric prune applies. **This — not admission — is what q25/q26 gain** (see the §0 correction below). |
| B4 | `PCS:1194-1222` `compareKeyAt` → `GVD.compareIds`, two `sliceSlot` resolutions per comparison | the heap comparison is a dictionary access | `Long.compare(a,b)`. The `ReadView` is still opened, for emission only. |
| B5 | `EXEC:13148-13160` + `EXEC:17384-17400` `GroupOrderPlan.resolve(… keyIsNumeric = numericSingleKey && !globalSingleKey …)` | a global group key is not "numeric", so an `ORD_KEY` spec resolves to `null` | `!globalSingleKey` becomes `!(globalSingleKey && !globalIdsAreOrdered)`. **The first draft claimed this unlocks `DenseGlobalGroupAggTable`; the review is right that it does not, and the claim is withdrawn.** `EXEC:12808-12810` already sets `numericSingleKey = true` for a plain global key, and the ClickBench group-bys order by the **aggregate**, not the key (q12/q13/q33/q34 are all `ORDER BY c DESC`), so `GroupOrderPlan.resolve` already returns a plan and the dense arm at `EXEC:15156` — wrapped by `EXEC:15145 if (orderPlan != null)` — is **already reachable today**. What B5 actually changes is narrower and worth stating exactly: `ORDER BY <the global key itself>` in a group-by, `HAVING` on a query whose only order key is the group key (`EXEC:13172-13174`), and transformed keys (`EXEC:13165-13168`). **No ClickBench query is in that set** — B5 removes a general limitation, not a benchmark number. |
| B6 | `PIBS:5706-5717` `zoneSkip` returns `false` for any predicate carrying `stringLitBytes` | correct today: an id bound says nothing about a value bound | **Unchanged.** It stays the single authority and stays right — a `contains` verdict still carries its literal. What changes is upstream: B7's ordering predicates no longer carry one. |
| B7 | `EXEC:8331-8348` builds an `O(entryCount)` verdict for `STR_LT/LE/GT/GE` | | Two binary searches over the ordered dictionary produce `lo`/`hi` and the predicate becomes a numeric range with **no literal retained**, so it prunes exactly — the treatment `EXEC:8385-8386` already documents for translated `EQ`. |
| B8 | `EXEC:12884-12899`; `EXEC:13245-13255` — a global deferred extremum, regex key, composite component or string-length operand forces the **whole-leaf byte arm** | the query hydrates every column of every leaf | With ordered ids the grouped extremum is a numeric min/max lane, so `anyDeferredGlobal` stops forcing `stringSlicedArm` off; `CompositeGroupIdentity:86-94` already gives a global component one exact lane. |

> **§0 correction the review required.** Capped top-k on a global column is **not** new: `EXEC:16377-16385` already
> admits kind 5 for a sliced, `limit >= 0` scan and `PCS:549-561` `topKSortColumnsOrderable` already admits it once
> a `ReadView` exists. q25/q26 are served today — slowly, because every heap comparison is a dictionary access
> (B4) and the plan gets neither best-first nor zone pruning (B3). What rank order makes newly *possible* is the
> **uncapped** `ORDER BY` (B2); what it makes *fast* is the capped one.

### 6.3 Arms that keep declining, and why

- `EXEC:8148-8153` numeric/FP/decimal comparison against a global column — an id is not a quantity. Unchanged.
- `EXEC:2459-2465` `SUM`/`AVG` over a global column. Unchanged.
- `EXEC:12647-12652` `fn:string()` over a global key — dict-only until a measured need appears.
- Any ordering arm when `B < entryCount` (§3.2): falls back to `compareIds`, as today.
- `EXEC:8187-8194` `OP_ARRAY_CONTAINS` (STRING_SET only). Unchanged.
- Any A1/A2 serving on a column that is not `columnSparseClean`. Unchanged and load-bearing.

### 6.4 Cost that P2 *introduces*, and must answer for

| site [M] | cost at the measured `D` [D] | answer |
|---|---|---|
| `GVD:239-278` `lengthTable(mode)` — `new int[entryCount+1]` and one `sliceSlot(id)` per id; called at `EXEC:12952` for `AVG(STRLEN(URL))` (**q28**) | an `int[]` sized by `D` (**73.5 MB** at 18.36 M) plus a `D`-iteration loop, per query, per worker | The per-id work is *not* a record decode — `sliceSlot` resolves through the retained bucket and block caches (`GVD:423-473`), so a sequential sweep costs ~`D/256` block decodes (71,737 for URL) and `D` cached reads. The cost that matters is the array and the loop. Build the table by walking **blocks** (`valueOffset`/`valueLength` are O(1) off the prefix-offset table) and charge the `int[]` against the query's own accumulator (§8) so the arm **declines** rather than allocating. |
| `EXEC:14353-14358` regex group key — `valueAsString` per distinct id (**q29**, `REGEXP_REPLACE(Referer, …)`) | **19.97 M** `String` materialisations + regex matches | Already once per *distinct value* rather than per row (100 M → 20 M, 5×). Bound it with the same accumulator and decline above it; a persistent derived-key lane is a follow-up. |
| `EXEC:17262-17274` `globalDistinctBitmaps` sizes the bitmap from the **row count**, not the header | `rows/8` = 12.5 MB per group at 100 M | **Over-sized, not wrong** — the review's correction. `rows` is a sound upper bound on distinct values and the comment says so; sizing from `entryCount` (`D/8` = 2.3 MB for URL) makes it 5× smaller at the cost of one header read. A tidy-up P2 should make while it is here, not a defect. |
| leaf id-lane width | rank order destroys leaf locality: 25 bits for URL where intern order might pack 14–16 | **≈ +5 B/row over four columns = 0.5 GB** [D] — priced into §4.1 and bought deliberately. A cascade encoding of the id lane (`SchemeSelector` is not applied to `encodeForBitPacked`, `ProjectionIndexRowGroupCodec:109-128`) is the measured follow-up. |

---

## 7. The verdict strategy

`GVD:313-336` `stringOpVerdict` is a scalar `for (id = 1..entryCount)` sweep, single-threaded on the coordinator
thread, **uncached**, and rebuilt on every serving *attempt* — `convertPredicateLeaf` is reached from 15 distinct
`extractConjunctivePredicates` call sites in `EXEC`. At the measured `D = 18.36 M` for URL that is the query's
critical path (q20, q22, q23).

Three measures, in the order they should land:

1. **Ordering ops stop needing it** (§6.2 B7). `STR_LT/LE/GT/GE` become id ranges. This removes the sweep from
   every ordering predicate at a stroke.
2. **Cache it — and the key matters.** The verdict is a pure function of the dictionary content and the predicate,
   so the key is **`(revision, headerNodeKey, entryCount, op, literalUtf8)`** [D]. `revision` and `entryCount` are
   both required and neither is redundant: a header key is stable across revisions while the dictionary behind it
   grows (`flushAppend` keeps `baseHeader.getNodeKey()`, `GlobalValueDictionaryWriter:716-741`), so a cache keyed on
   the header key alone would serve a shorter revision's bitset for a longer dictionary — a silently truncated
   verdict, which reads as "no match" for every appended value. `entryCount` also happens to be the bitset's
   length, so the key carries its own shape check.

   **Simpler and stronger: hang the cache on the `ReadView`.** A `ReadView` is already revision-bound and
   re-checks the revision before every operation (`GVD:143-157`, `ensureRevision`), so a verdict memo held there
   *cannot* outlive its dictionary and the composite key collapses to `(op, literal)`. The cost is that a view is
   per-worker and per-operator today, so the memo has to move up to something the query owns — which is the same
   object the accumulator of §8 needs. **Recommendation: one per-query object holding both.**

   **Eviction: dropped.** The first draft said "16 entries, evict-largest" and could not justify it. A query has a
   handful of distinct string predicates; the natural bound is *per query*, and a per-query cache needs no eviction
   policy at all — it dies with the query. If a session-level cache is ever wanted, its policy has to be argued
   from a measurement, not asserted. Witness: a build counter that reads 1 across a query's serving attempts, whose
   mutation (cache disabled) reads > 1.
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

**The real fix for `contains` is a build-time filter — but its granularity has to be refitted, and the review
caught why.** The first draft put a 2 KiB 3-gram Bloom filter on each value block. **A block holds at most 256
values** (§3.3.1), which for URL is ~23 KB of bytes carrying on the order of 3–8 K distinct trigrams; a filter with
a usable false-positive rate would be several KB — *tens of percent of the block it guards*. Per-block is the wrong
granularity.

So segment 6 is specified as an **open design point with the trade named**, not as a number:

- the filter must cover a **group of blocks** (a natural unit is 64 blocks = 16,384 consecutive ids, matching the
  `ReadView`'s bucket cache), where ~50 K distinct trigrams at 8 bits is ~50 KB per 16,384 values ≈ **3 B/value,
  ~3 % of the dictionary** — viable, at the cost of a coarser skip;
- skip granularity and filter size trade directly against each other, and the right point depends on the needle
  selectivity the workload actually has;
- **the deciding measurement** is the distribution of matching ids across the id space for representative needles:
  a needle whose matches are spread uniformly skips nothing at any granularity, and one whose matches cluster (as
  a host substring does, once URLs are sorted by value — which is exactly what rank order gives) skips almost
  everything. **Rank order is what makes this filter plausible at all**, and it is also why segment 6 cannot be
  designed before segment 1 is measured. This is the general form of the plan's "per-leaf 3-gram blooms"
fallback, lifted to the place where each value exists once. It needs one new record kind and is therefore a
segment of its own (§12, segment 6).

---

## 8. Residency, pinning, and the fact that R1 is now OFF

> **This section was written against a world that no longer exists.** As of commit **`5b4a39ef2`** (2026-08-30
> 22:48) headroom-gated residency is **opt-in**: `-Dsirix.projection.residency.headroom` now defaults to `false`.
> The 100 M leg it was built for measured what it cost — same database, same code, only the flag moving: **q3
> 23.4 s cold / 21.1 s hot with it on against 2.4 / 0.09 with it off** (hot == cold being the signature of
> retaining nothing), and **q4 never finished**, thrashing at 12.6 cores and 16.9 GB RSS with a 763 s concurrent
> mark cycle, against 1.50 / 1.46 off. [M, from the commit message]

Three consequences for P2:

1. **`HeapHeadroom.plannedShareBytes` is not a live budget any more.** It still exists and still computes
   `min(max/8, headroom/4)`, but with R1 off it no longer gates the column store's retention. P2 must not write any
   arm that *assumes* a residency rule is in force, and must not quietly re-enable one.
2. **The first draft's "charge it against `plannedShareBytes`" was a double-spend, and the review is right.** It
   proposed three independent tests against one share — the verdict bitset, the length table, and the regex key
   materialisation each checking `plannedShareBytes` on their own. Three arms each asking "am I under the share?"
   and each answering yes will together allocate three times the share. **The fix is one accumulator, not three
   tests:** a per-query `DerivedHeapBudget` object with `tryReserve(bytes)` / `release()`, threaded through the
   arms that build `O(D)` state, whose ceiling is *one* configured figure and whose reservations compose. It is the
   same object §7.2 wants for the verdict memo, so it is one object, not two.
3. **What the accumulator's ceiling should be is a decision, not a default.** Reusing `plannedShareBytes` is
   tempting and wrong for the same reason R1 was turned off: `min(max/8, headroom/4)` at an 8 GB heap is ~1 GB, and
   a rule computed from *live* headroom makes a query's admission depend on what other queries happen to be doing —
   which is precisely the state-dependent decline the campaign has been fighting. **Recommendation: a static,
   explicitly configured ceiling** (`sirix.projection.globalDict.derivedHeapBytes`, default `maxMemory/16`),
   independent of live headroom, so an arm's decision is reproducible across runs. Segment 2 should not ship a
   headroom-derived figure without the lead's ruling.

What is unchanged and still true:

- **The dictionary read side is disk-resident and nobody pins it.** A `ReadView` retains 256 slot triples, 16
  buckets and 16 decoded blocks — a fixed footprint whatever `D` is (`GVD:105-121`, `:160-200`) [M]. At 256 values
  per block (§3.3.1) that is ≤ ~1 MB and covers 4,096 consecutive ids. `ProjectionResidencyScope` pins *column
  lanes* in a `ProjectionColumnStore` (`ProjectionResidencyScope:103-145` → `store.acquirePin`) [M]; a `ReadView`
  is not a store lane and pinning it would be a category error — it is a cursor, not a fill.
- **A global column is already priced correctly in the store**: `ProjectionColumnStore:1002-1004` charges
  `rows*8 + presence` for a long lane, and its javadoc says why counting it as weightless would be wrong [M]. P2
  changes none of that. It does *remove* the `STRING_DICT` arm's `dictByteLen × DICT_DECODED_EXPANSION(=4)` charge
  (`:1010-1024`) for these columns — the residency win that comes free with the storage win.
- **P2 makes the fat columns cheaper to hold**: 8 B/row of ids instead of 4 B/row plus four times the dictionary's
  stored bytes. That is true whether or not any residency rule is on, and it is the one thing here that does not
  depend on the flag.

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
passes **§9.4's update-rate test — the same measured criterion, not a second one that resembles it.** §9.4 requires
an analytical shape because the dictionary is append-only and dead entries accumulate under updates; the candidate
set must use exactly that test, so "which resources may be promoted" and "which resources can afford monotonicity"
cannot drift apart. The first draft wrote "revision count is 1", which is a *proxy* for the update rate and would
refuse a perfectly good read-mostly resource that happens to have three revisions. (The pass separately requires no
open write transaction for its own duration — §5.7 — but that is a concurrency precondition, not an election
criterion.)

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

## 12. Staged plan — a gate, then 1 → 5, then the serving segments

Each segment is independently revertible, has its own kill switch whose "off" state is proven byte-identical against
a `git show HEAD:`-compiled build, and lands with its witnesses.

| # | segment | acceptance | kill switch |
|---|---|---|---|
| **0** | **The dictionary-bytes gate** (no product code). A bench-package probe that builds a real dictionary from a 5 M-value sample in three forms — intern order, rank order, rank order front-coded — and reports **written bytes for every persisted record: value blocks, value buckets, the reverse radix, and (for the intern-order control) the forward hash index and its radix.** Blocks hold 256 values, so the front-coding runs are 256 long (§3.3.1). | **written dictionary bytes ÷ rows ≤ 17.3 B/row** for the front-coded, forward-index-free form (§4.1's break-even at ratio 0.37). **Above it, segment 1 does not start.** A *value-block ratio* is explicitly NOT the acceptance — measuring that is what hid the directory in draft 1. | n/a |
| **1** | **The rank pass, offline, one column** — `orderedPrefixCount` in the header, front-coded value blocks, S1–S4, the election, the disk preflight. No executor change; the column serves exactly as a global column serves today (equality, grouping), just with ordered ids. | At 1 M on a synthetic fixture: the four fat columns' post-codec bytes match the §4.1 arithmetic ±10 %; W1–W6, W10, W12, W14 green; the load path byte-identical. | `-Dsirix.projection.globalDict.rank=false`, `…globalDict.frontCoding=false` |
| **5** | **The trie lane** — registry, encoding kind 3, flag byte 2, the resolver. **This is −16.7 of the −23.9 GB, so it comes before the serving segments.** Go/no-go gate first: the resolver cost (R4) — measure point-lookup and full-reconstruction latency against the lane before writing the encoder, on a 1 M resource whose dictionary segment 1 already built. | The plan's rule holds — *point/row-path queries never slower*; reconstruction round trip byte-identical with the lane on and off; the 100 M leaf class falls by ≥ 12 GB; W11 green. | `-Dsirix.page.stringRegion.globalIdLane=false` |
| **2** | **The integer arms.** §6.2 B1–B8: `globalIdsAreOrdered`; `sortColumnsOrderable` **together with** `EXEC:16409` and `EXEC:16442`; best-first + zone prune; `compareKeyAt`; `keyIsNumeric`; the id-range translation of ordering predicates. (No dense-arm claim — see B5.) | Differentials under `STRICT_SERVING` for q25/q26/q13/q14/q16/q17/q33/q34; W3, W7 green. Speed judged against rebuild #2's leg. | `-Dsirix.projection.globalDict.orderedArms=false` |
| **3** | **The three missing arms.** §6.1 A1 and A2 (each with `columnSparseClean`, the empty-sequence contract and the `ID_UNKNOWN` decline) + A3 **scoped to stores under `ROW_MAT_MAX_ROWS`** + the `globalDistinctBitmaps` sizing tidy-up. | q5 served from the id bitset; A1/A2 differentials green under `STRICT_SERVING`; W7 green. Timing targets come from rebuild #2's leg, not from this document. | `-Dsirix.projection.globalDict.newArms=false` |
| **3b** | **Lift `ROW_MAT_MAX_ROWS` for `LIMIT`-bounded plans** — its own step because it means making covered-row serving streaming or limit-aware rather than eagerly materialising (`EXEC:16915-16922`). Not P2-specific; P2 only exposes the need. | q23 materialises at 100 M without an eager 100 M-row buffer. | `-Dsirix.projection.rowMaterializeMaxRows` |
| **4** | **The verdict**: per-query cache keyed `(revision, headerNodeKey, entryCount, op, literal)` (or hung on the `ReadView`) + bucket-partitioned parallel sweep, and the one per-query derived-heap accumulator of §8. | q20 improved against rebuild #2's leg; W8, W9, W13 green. | `-Dsirix.projection.verdict.parallel=false`, `…verdict.cache=false` |
| **6** | **Grouped 3-gram filters** for `contains` — granularity refitted per §7 (per-block is not viable at 256 values/block); design point opens only after segment 1's measurements. | q20/q22 improved against rebuild #2's leg at a filter cost ≤ 5 % of the dictionary. | `-Dsirix.projection.globalDict.blockFilter=false` |

**Order: 0 → 1 → 5, then 2 → 3 → 3b → 4 → 6.** The review's staging ruling, and it is right on the numbers:
segment 5 delivers **−16.7 of the −23.9 GB** and is the only reason M1 is reached, while segments 2–4 buy speed
whose baseline does not exist yet. Doing 1 → 5 first settles the *storage* outcome in one rebuild before any
serving code is written.

**§16's screen comes before segment 5 is committed to.** If a bigger leaf (L1) recovers the same bytes through
ordinary per-leaf dictionaries, segment 5's resolver is not worth building; the windowed-dedup measurement decides,
and it costs one quiet core-hour.

Segment 0 is a gate with no product code. **Segments 1 and 5 must land together in one 100 M rebuild** — they share
the dictionary, and measuring either alone would mis-attribute its bytes.

---

## 13. Witnesses, each with the mutation that must fail it

| # | witness | mutation that must fail |
|---|---|---|
| W1 | `RankOrderIsUtf16Order` — a generated value set spanning ASCII, U+0800..U+D7FF, **U+E000..U+FFFF** and **supplementary** characters; the ranks produced by S2 equal the order of `ValueDictionaryEntryNode.compareUtf16Range`. The generator **also emits malformed input** — unpaired and CESU-style encoded surrogates (`0xED 0xA0..0xBF`), overlong forms, `0xC0/0xC1/0xF5..0xFF` — which S1 must reject by name (§5.4). | remove the `0xEE→0xFE / 0xEF→0xFF` substitution → the PUA-vs-supplementary pair inverts; remove S1's validation → an encoded surrogate sorts into a position `compareUtf16Range` disagrees with, and the differential catches it. |
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
| W15 | `ForwardIndexIsAbsentOverAnOrderedPrefix` (§3.3.2) — after the rank pass the header's `forwardRootKey` is 0, no hash-bucket or forward-radix record exists in the reserved run, and a literal probe still resolves the right id by binary search. | keep building the forward index → the written dictionary bytes rise by the §3.3.2 arithmetic (assert **bytes**, not the answer, so the mutation is visible rather than merely slower); relax the header invariant without the `orderedPrefixCount == entryCount` condition → a streaming-built dictionary is accepted with no way to probe it. |
| W16 | `RankPassCommitsPerEpoch` (§5.5) — the `-Dsirix.til.diag` census stays flat across a pass whose dictionary and segments together exceed the configured intent-log ceiling. | remove the per-epoch commit → the log grows monotonically and the fixture exhausts its budget, proving the cadence is load-bearing. |
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
| R4 | **The trie resolver hop makes point reads and reconstruction slower.** Every fat string value read becomes a dictionary lookup. | high, for segment 5 | The election's factor-4 margin (§10.4); the per-trx resolver holds a `ReadView`-shaped block cache — 16 decoded blocks covering 4,096 consecutive ids at 256 values per block (§3.3.1) — so a scan with locality pays one ~23 KB decode per 256 values, while a *random* point read pays one per lookup, which is exactly what the gate must measure; the gate measures point-lookup and full-reconstruction latency *before* the segment ships, and the plan's rule "point/row-path queries never slower" is the acceptance. |
| R5 | **The trie depends on a structure the projection introduced.** A secondary index becoming load-bearing for primary data. | high, architectural | The dictionary already lives in the **NamePage** sub-trie (`NamePage:173`, `reserveProjectionValueDictionaryKeys:1316`), i.e. resource metadata beside the name dictionary — not in the projection's blob store. Naming values there is the same move the trie already makes for *names*. The registry of §3.4 is what makes it a resource structure rather than a projection one. If the reviewers reject the inversion, segment 5 dies and M1 needs T1-c's page schema instead (plan §7). |
| R6 | **`B < entryCount` after the first maintenance commit silently degrades every ordering arm.** | medium | Not silent: a `PROJ_DIAG` line and a counter; the decline reason names the boundary. The re-rank pass (§5.8) is the remedy and is cheap for an already-global column. |
| R7 | **Rank order costs id-lane bits.** ≈ +5 B/row over four columns (25 bits for URL where intern order might pack 14–16). | low | Priced in §4.1; §11.1 is the recovery. |
| R11 | **The dictionary's directory was priced at zero in draft 1.** Corrected in §3.3.2/§4.1: the forward hash index alone is ≈ 8.7 B/row post-codec, which on its own breaks the ≤ 30 B/row acceptance. | **high — it changed the design** | The design now drops the forward index over the ordered prefix and probes by binary search, and **segment 0's gate measures written dictionary bytes including every directory record**. If reviewers reject dropping it, the acceptance must be renegotiated (§4.1's last row). |
| R12 | **Probe latency on the maintenance path.** Binary search costs ~16 page reads per newly interned value against ~5 radix decodes today (§3.3.2). | medium | Query-path probes are per literal, not per row, so they are unaffected. Incremental maintenance on a rank-ordered column is already the slow path; if it becomes the binding constraint, the tail's own forward index can be widened to cover the prefix at the §4.1 cost, per resource, by measurement. |
| R13 | **`ROW_MAT_MAX_ROWS` blocks q23 regardless of P2** (`EXEC:16922` caps on TOTAL store rows). | medium | Segment 3b, with its own acceptance. P2 must not claim q23 until that lands — the first draft did. |
| R8 | **The pass takes the write transaction for its duration.** The resource is unavailable for writes for the length of the pass (**[A]** 10–25 min at 100 M). | medium | Stated as a limitation. It is a bulk reorganisation; an online variant is a different brief. |
| R9 | **`stringOpVerdict` remains `O(D)` for `contains`** until segment 6, whose granularity is itself an open design point (§7). | medium | Segment 4's cache + parallelism is the interim. No target is asserted: the plan's 1–2 s for q20 predates wave 3, and the honest baseline is rebuild #2's leg. |
| R14 | **R1 (headroom-gated residency) is OFF as of `5b4a39ef2`** and P2's derived-heap arms must not quietly reinstate a headroom rule (§8). | medium | One per-query accumulator with a **static** ceiling, not N independent tests against a live share. Segment 2 does not ship a headroom-derived figure without a ruling. |
| R10 | **There is no speed baseline yet.** Rebuild #2's **storage** is measured (§1.2); its 43-query leg is running now, and the residency regression that motivated it has since been resolved by turning R1 off (`5b4a39ef2`). | high, procedural | The storage half of §4 needs nothing further. **Every timing target in §6/§7 is withheld on purpose** — segments 2–4 are judged against the leg now running, not against the plan's pre-wave-3 numbers. |

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

---

## 16. The L1 alternative to segment 5

The lead asked for this comparison before segment 5 is committed to, and it is the right question to ask.

### 16.1 Why the trie's string region is expensive at all

The region costs **193.1 B/row written, 296.3 raw** [M] not because string values are large but because the
per-page dictionary has nothing to dedupe. A leaf is `NDP_NODE_COUNT = 1024` slots
(`settings/Constants.java:131-134`), a ClickBench row is **106.2 records** [M: 10,617,283,271 records /
99,997,497 rows], so a page holds **9.66 rows** [M: 10,351,305 document pages]. A per-tag dictionary over ~9.7
URL values sees ~9.7 distinct values and buys nothing — B5-d's review measured exactly this in the other
direction, finding the dictionary's `16 B/tag + 4 B/entry` framing to be pure loss at that width, which is why
B5-c added the **plain lane for all-distinct tags** (`StringRegion:118-125`).

So there are two ways to stop storing the same URL hundreds of times:

- **Segment 5**: keep the leaf at 1,024 slots and make the region name a **resource-wide** id.
- **L1** (plan §5, "Wave 3 — L1"): raise the slot exponent so a leaf holds enough rows for the **per-leaf**
  dictionary to work. `slotExponent = ceil(log2(target × avgNodesPerRecord))`; at 2^17 = 131,072 slots a leaf holds
  **1,234 ClickBench rows**.

### 16.2 What each costs

| | segment 5 (global ids in the region) | L1 (bigger leaves, per-leaf dictionaries) |
|---|---|---|
| **byte gain** | **−16.7 GB** [D, §4.2] — the fat tags' bytes become 3-byte ids, and the sketch goes with them | **unknown — this is the whole point of §16.3.** Bounded above by the same −16.7 GB and below by 0; it is exactly the windowed-dedup measurement |
| **new wire form** | encoding kind 3 + a NamePage registry (§3.4, §3.5) | none — the region encodes as it does today |
| **read path** | a **resolver bound above the page** (§10.5): `PageKind`'s reinjection has no reader, so `StringNode.getRawValue()` gains a third arm and every value materialisation goes through the dictionary | **unchanged.** This is L1's real attraction: no resolver, no new failure mode on the primary read path |
| **revision story** | the dictionary is append-only forever; dead entries accumulate; §9.4 restricts promotion to analytical resources | unchanged — a per-leaf dictionary lives and dies with its page |
| **blast radius** | the projection index and one region kind | `NDP_NODE_COUNT` appears in **18 source files** [M]; arena size classes; COW granularity; every page-shaped assumption |
| **hard obstacle** | none known beyond the resolver | **`MAX_SLOTTED_PAGE_CAPACITY` is 256 KiB** (`KeyValueLeafPage:311`, the largest `FrameSlotAllocator` size class). A 1,234-row leaf carries ~320 KB of string payload alone [D: 260 B/row × 1,234] before any other region, so **L1 at this target requires raising that ceiling** — new allocator size classes, and a COW unit of ~1 MB |
| **cost paid by everyone** | only resources that elect a global tag | **every write, every point read, every history read** on a resource with a large exponent: COW rewrites the whole leaf, a point lookup pulls a ~1 MB page, `SLIDING_SNAPSHOT` merges bigger fragments |
| **already scheduled?** | this brief | plan §5 "Wave 3 — L1", **parked** (plan §8) pending a measured gate |

Two honest asymmetries that the table does not capture:

- **L1 helps every region, not just strings.** Bigger leaves amortise the ~1.21 B/record fixed per-page overhead
  B5-c measured and would shrink the number, zone-map and name-key regions too. If it works, its total gain is
  larger than the −16.7 GB compared here.
- **Segment 5's gain is bounded and known; L1's is neither.** That asymmetry is the reason §16.3 exists.

### 16.3 The one measurement that decides it

**Distinct values per window of `W` consecutive rows, for `W ∈ {10, 1234, 10000}`, for each of the four columns.**

- `W = 10` reproduces today's leaf and is the control: it should come back at ≈ 10 (all distinct), which is the
  measurement that explains the 193.1 B/row.
- `W = 1234` is L1 at 2^17. If URL comes back at ~1,100 distinct per 1,234 rows, the per-leaf dictionary
  deduplicates ~11 % and **L1 cannot replace segment 5**. If it comes back at ~300, the dictionary removes ~75 %
  of the payload and L1 is genuinely competitive.
- `W = 10000` bounds what any realistic leaf size could achieve, so a disappointing `W = 1234` can be told apart
  from "the whole approach is hopeless".

**Judge it post-codec, not on raw dedup.** The string region is LZ77'd at 0.652 [M] and LZ77's window already
catches values repeating within a page, so a per-leaf dictionary's *marginal* gain over the codec is smaller than
the raw distinct ratio suggests. The decision rule should therefore be: run the windowed counts first (cheap), and
only if `W = 1234` shows a large dedup, encode one synthetic 1,234-row page through the real `StringRegion` +
`PageKind` codec and compare written bytes against today's 9.66-row page. The first step is a screen; the second is
the answer.

**Cost and scheduling.** One more single-core pass over `hits.json.gz` — the existing `$S/agents/b7/card3.py`
already extracts the four fields per row and hashes them; the change is to replace the global HLL with a rolling
exact set per window, which is ≤ 10,000 entries and therefore trivial in memory. ~40 minutes, one core.
**Deliberately not run: the 43-query leg owns the machine, and CPU beside a timing leg corrupts it** (the ledger's
21:10 rule). It should run in the first quiet window, before segment 5 is committed to.

### 16.4 Recommendation

**They are not exclusive, and the honest recommendation is to run §16.3's screen before committing to either.**
If `W = 1234` shows a large dedup, L1 is the better route for this particular cost — no resolver, no append-only
dictionary on the primary read path, and it helps the other regions too — and segment 5 should be dropped in its
favour, leaving P2 as a projection-only lever that does not reach M1 alone. If it shows a small dedup, L1 cannot
substitute here (whatever its other merits) and segment 5 is the only route to M1 in this track. Committing to
segment 5's resolver before that screen is run would be spending the design's riskiest change without knowing
whether a cheaper one was available.
