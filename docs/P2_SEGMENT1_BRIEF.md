# P2 segment 1 — the projection's resource-wide dictionary, with the acceptance corrected

Supersedes the acceptance in `P2_GLOBAL_DICTIONARY_DESIGN.md` §12 for segment 1. Everything else in that
document still stands, including §18's post-mortem of why the original gate failed and §16's ruling that
bigger leaves are not a substitute for this work. Read both before starting.

## Why the acceptance moved

The original 17.3 B/row came from sizing the value region as `D × occurrence-weighted mean length`. A
dictionary stores each value once, so the multiplicand must be the **distinct-weighted** mean — measured
over the whole corpus at URL 184.01 B against the occurrence-weighted 90.45 (2.03×), Referer 136.60,
Title 132.79, SearchPhrase 65.27. The raw distinct value region at 100M is **7.750 GB, not 4.68**, and the
dedup factor is 3.34×, not 5.5×. The cardinalities were right to 0.1–1.1 %; only the length term was wrong.
No compression closes a 1.66× error in the multiplicand, which is why the gate failed 1.6× in the best
shape that was actually built and weighed.

## Acceptance (restated, and to be measured on disk, never derived)

| quantity | acceptance | basis |
|---|---|---|
| values + directory, 100M | **≤ 27 B/row** | best shape measured in the gate: 27.4 B/row (front-coded, no forward index over the ordered prefix, in-record LZ77) |
| projection half total, 100M | **≤ 40 B/row** | the above plus the 12.1 B/row of id lanes |
| saving against today | **≥ 5.0 GB** | gate measured −5.62 GB on the projection half |
| every query, hot and cold | **no per-query regression** | the standing rule; a lever that saves bytes and costs query time does not land |

Measured means: written bytes of every dictionary record including the directory, divided by rows, from a
real build through the product writer — never a value-block compression ratio, which is the measurement
that hid the directory in the first draft.

## What to build

The post-pass of §5, unchanged in shape because rank order is a global property:

1. **S1** extract per-leaf dictionary entries, validating that every value is well-formed UTF-8 and failing
   by name if not (`decodeCodePoint` throws on CESU-8 surrogates and overlongs, so a byte-order merge would
   otherwise silently rank inputs the collation refuses).
2. **S2** k-way merge in UTF-16 order using the proven substitution `0xEE→0xFE`, `0xEF→0xFF`, which makes
   unsigned byte order exactly `compareUtf16Range`. Use a **front-less appender** — not
   `flushStreamingDictionaryGeneration`, which rebuilds the very probe front this work exists to remove
   (`ProjectionIndexBuilder:1784`, `:1794`, `:1846-1881`), and a merged sorted stream has no use for it.
3. **S3** sort `(leafId, localId, rank)` triples.
4. **S4** remap and drop `DICT` / `BLOOM` / `DICT_HASHES` through the existing column-scoped slot path.

With, from the gate's measurements:

- **Front-coded blocks of 256** plus the **in-record LZ77 third form** — both already implemented and
  round-trip verified in `scratchpad/agents/p2s1/src/io/sirix/node/P2GateFrontCoding.java`, which is
  directly reusable and already carries the corrupt-record bounds (decoded size accumulated and refused
  before allocation, shared prefix refused above the previous entry's length, suffix region bounded).
  A negative count discriminates the compressed form, so the decoder needs no switch.
- **No forward hash index over the ordered prefix.** "Which id holds this value" is a binary search over a
  reverse index already sorted by value; the forward index survives only over the unordered tail.
  Dropping it is worth far more than the design estimated — measured 64.7 B/entry at D=275K and 173 B/entry
  at D=2.62M, because every bounded append writes fresh forward radix nodes under COW.
- **Per-epoch commits** (S2 per rotation, S4 per 1,024 row groups, the kind flip only in the final commit),
  or the "≈1.0 GB peak independent of D" claim is false: one transaction would retain the whole dictionary
  plus 97,654 re-encoded row groups in its intent log.
- **Single-revision re-rank only**, with the versioned path correct and its cost named, per §5.7.1.

## Witnesses that must exist before it lands

- **W17** the binary-search probe answers identically to the hash probe at every divergence point: both ends
  of a value block and of a 256-id bucket, the empty string, `MAX_VALUE_BYTES`, a spilled oversized value,
  a long-shared-prefix neighbourhood, absent-before-first / absent-after-last / absent-between, and a
  `valueHash` collision pair. Absent must answer `ID_ABSENT`, never `ID_UNKNOWN` and never a neighbour's id.
- **W18** the maintenance interner's cost measured, not predicted (the gate predicted ~3×); segment 1 cannot
  be accepted with that field empty.
- **Kill switch** byte-identical against a `git show HEAD:`-compiled build, digests recorded.
- **Differentials** for every executor arm whose behaviour changes, each A/B'd against its own decline
  rather than against the suite total.

## Order of work

1. Land it at **1M** with the full measurement: written dictionary bytes per row in both shapes, the
   projection half, and a 43-query leg with per-query comparison. Report before going further.
2. Only then a **100M rebuild and leg**, which is the acceptance of record. Budget ~46 min for the load and
   ~40 for the leg, and nothing else may run on the machine during either.

## After this: the trie lane

Segment 5 — the trie's string region naming the same ids — is worth about **−15 GB** against segment 1's
−5.6, and is what makes M1 possible at all. It needs a per-transaction resolver because `PageKind`'s
reinjection has no reader, and it is gated behind a point-read and reconstruction measurement, since it puts
a resolver hop on the primary read path. Do not start it until segment 1 is measured at 100M.

## Ruling 2026-08-31: the acceptance is a FRESH BUILD, and the retrofit is not the route

Recorded after `impl-p2s1` proved S1–S4 end to end at 1M (Title: 977 leaves, 188,548 per-leaf entries →
**73,970 distinct**, 5.5 s total, **43/43 query dumps byte-identical** afterwards) and measured that the
converted database **grew** 1,094.5 → 1,105.1 MB.

**The growth is correct and structural.** Sirix is append-only. Dropping a column's `DICT` / `BLOOM` /
`DICT_HASHES` removes the *reference* from the new leaf version; the superseded bytes stay at their durable
offsets so revision N still reads. A post-pass conversion of an existing database therefore costs bytes and
buys query shape. **Anyone proposing a conversion pass as a storage lever must answer that sentence first.**

**But the retrofit was never the acceptance route.** `requireVirginTreeForInitialBuild`
(`ProjectionIndexHOTStorage:205`, called from `ProjectionBulkLoad.begin:315`) forbids re-running the builder
over a *populated* projection tree. A freshly loaded database is a virgin tree, and "Order of work" item 2
above always meant loading a new 100M database from the corpus. That path is open.

**The real blocker is the promotion gate, and 40 of its 52 bytes are the index we are deleting.**
`projectedGlobalDictionaryBytes = rows × (avgValueBytes + PER_ENTRY_OVERHEAD_BYTES)` with
`PER_ENTRY_OVERHEAD_BYTES = 52` (`ProjectionIndexBuilder:305`) against `min(heap/8, 2 GiB)`. Two defects:
it projects cardinality as **`rows`** rather than distinct (Title at 1M is 2.5× dedupped, and nobody has ever
measured the true corpus cardinalities — the recorded ones come from a 20k-row head sample); and its 52 B/entry
is, per its own javadoc, *offsets 8 + lengths 4 + two hashes 16 + 24 B of open-addressed table slots* — i.e.
**the forward hash index P2 removes**. The gate blocking the storage win prices the memory cost of the
structure this work exists to delete.

That 52 B is inherent to a **streaming** build: one pass must answer "have I seen this value" per row, so its
peak heap grows with D. A **sort-based pre-pass does not** — S1's spilling run buffer and S2's k-way merge have
a peak independent of D and emit the dictionary already rank-ordered, which is the form that front-codes. The
`globaldict-promotion-budget-crossover` at ~6–18M rows is an artifact of the build shape, not of the data.

**Fresh-build shape:** pre-pass → finished rank-ordered dictionary → one projection build writing final ids.
Nothing is superseded, so the file does not grow, and S3/S4 are not on that path (they remain the retrofit for
existing databases). The builder must not reintroduce a hash probe to answer value→id: S1 visits values in row
order and can emit the id lane once ranks exist — ~1.6 GB of sequential spill at 100M for four columns.

| clause | instrument |
|---|---|
| ≤ 27 B/row values+directory | sum of row-group descriptor `byteLen` — **accepted**, and stronger than a ratio |
| ≤ 40 B/row projection half | same |
| **≥ 5.0 GB saving** | **file size of a freshly loaded 100M database vs a freshly loaded HEAD one** — never a retrofit delta |
| no per-query regression | unchanged |

Measured against the shipped default (`sirix.page.overflow.compress=false`, opt-in since `8cfeb2207`), so the
gate's 9.57 GB baseline stands; report the compression-on arm too, since P2 changes which bytes remain in that
class and may change that lever's verdict.

## 1M measurement, 2026-08-31 — storage strong, latency fails, LANDS DISABLED

Baseline forced to per-leaf with `-Dsirix.projection.globalDict=never` so all four fat columns start as
`STRING_DICT`. Instrument: sum of row-group descriptor `byteLen`, as ruled above.

| | URL | Title | Referer | SearchPhrase | Σ |
|---|---|---|---|---|---|
| before (per-leaf DICT+BLOOM+DICT_HASHES+lanes) | 32.878 | 23.367 | 27.180 | 2.390 | **85.815** |
| after (id lanes only) | 2.346 | 2.155 | 2.282 | 1.820 | **8.603** |
| rank-ordered dictionary | 13.138 | 4.279 | 12.375 | 0.488 | **30.280** |

Whole projection index **131.838 → 54.625 B/row**. The dictionary figures reproduce the segment 0 gate's
numbers to within the appended `orderedPrefixCount` int per generation — two independent implementations
agreeing, which is worth more than either alone.

**Correctness: 43/43 dumps byte-identical twice** — after converting Title alone, and after all four.

**≤ 27 B/row is UNDECIDED, not met.** Measured 30.28 at 1M where D/rows = 0.28 against 0.18 at 100M, so the
100M regime is easier and the clause probably passes there — but that is a slope through two scales, the exact
error that killed gate 0 (§18.4). Carry it open until 100M measures it.

**The latency clause FAILS and a fresh build fails it identically.** Cold Σ 12.134 → 33.861 s (+179 %, 20/43
regress >10 %), hot Σ 7.433 → 25.664 s (+245 %, 15/43). Worst q22 0.033 → 4.306, q21 0.085 → 4.294,
q5 1.098 → 9.249, q28 0.458 → 4.721. The `# served` counters name the cause: `projectionCountDistinct` 2 → 1,
`groupDistinct` 6 → 5, `groupAggregates` 28 → 26, and q5/q21/q22 fall to `route=NONE`.

**The general lesson: a storage lever that changes a column's KIND relocates it to a different set of serving
arms, and the missing arms are invisible until measured.** Segment 1 moves four columns into kind 5, whose
grouped and distinct arms are segment 2's work. No build route changes this.

**RULING: segment 1 lands DISABLED** — no pass, no promotion, no regression, clause satisfied honestly rather
than waived. Before segment 2 is scoped, two screens: (a) convert **URL alone, rank-ordered**, against the
stock default where AUTO already elects URL as kind 5 *intern*-ordered — same column, same kind, same arms, so
a decline there indicts **rank ordering itself** (a gate keyed on `globalIdsAreOrdered` or on the forward
index) rather than a missing arm; (b) a per-column screen over all four, since a partial default that
regresses nothing beats a complete one that cannot ship — Title alone is worth 16.9 B/row and URL 17.4 of the
46.9 B/row total.

## Screens, 2026-08-31 — rank ordering is free, and segment 2 is two items

**(a) Rank ordering costs nothing.** Stock 1M default (URL kind 5, intern-ordered, forward index) against the
same database with URL converted rank-ordered, every other column kind 2 in both — so ordering and the absent
forward index are the only variables. **`# served` identical field for field** (countDistinct 6,
groupAggregates 84, groupDistinct 18, sortedScans 12, `route=NONE` on q42 in both, already NONE at baseline),
43/43 dumps byte-identical, cold +3.9 %, hot −0.7 %. The arms did not merely cope — they did not notice.
**Nothing is keyed on `globalIdsAreOrdered` or on the forward index existing.**

**(d) Per-column screen from the shipped default, one column each:**

| arm | cold Δ | hot Δ | route=NONE | dominant cost |
|---|---|---|---|---|
| URL rank-ordered | +3.9 % | −0.7 % | 1 (unchanged) | **clean** |
| Title | −0.9 % | +1.0 % | 1 (unchanged) | **clean** |
| Referer | +39.8 % | +52.6 % | 1 (unchanged) | q28 0.26 → 4.71 s |
| SearchPhrase | +120.2 % | +156.3 % | **4** | q5 0.08 → 5.53 s |

Referer keeps every served route identical and is still 40 % slower — one query, §6.4's `AVG(STRLEN)` length
table (`new int[entryCount]` plus a D-iteration sweep per query). Only SearchPhrase actually loses arms.
**So segment 2 is two items: §6.1's A1 (ungrouped COUNT(DISTINCT)) and §6.4's length table** — not "build
all of §6.1", which is what the +179 % four-column number implied.

**Target default: promote URL + Title, leave Referer and SearchPhrase per-leaf until those two items exist.**
Nothing to enable yet — the pass is a retrofit and the builder still declines promotion at 100M — so segment 1
still lands disabled; the screen defines the target.

**Mixed kinds are legal, asserted not assumed:** `RowGroupDescriptor.kindsAgree` compares the whole kinds
array across LEAVES (the invariant S4's per-column commit preserves), not columns against each other, and
every screen arm above is a mixed store (URL kind 5 beside Title kind 2) serving 43 queries cold.

**Exact 1M cardinalities, the first this campaign has had** — these RETIRE the 20k-row head sample:

| column | distinct | distinct-weighted mean | head sample said |
|---|---|---|---|
| URL | 275,494 | 128.25 B | 184.01 |
| Referer | 227,319 | 125.58 B | 136.60 |
| Title | 73,970 | 170.80 B | 132.79 |
| SearchPhrase | 18,316 | 56.90 B | 65.27 |

They do not merely refine the sample, they **reorder** it: Title is the longer of the two at 1M. Distinct-
weighted length grows with D so the 100M values may yet approach the sampled ones — but the ≤27 B/row
acceptance was derived from those sampled lengths, which is one more reason it stays **undecided**.

## OPEN: the streaming global-dictionary path costs ~9× its dictionary

Observed in the same run and **not yet diagnosed**. Stock `auto` 1M build **1,091.8 MB** against **612.6 MB**
for `globalDict=never` — the shipped default is **78 % larger** than the same corpus with global dictionaries
off — and the delta is almost all OverflowPage, **134.7 → 589.2 MB (4.4×)**, to hold a URL dictionary that
weighs **54 MB** when the rank pass builds it and ~33 MB per-leaf.

This outranks the rest of the queue because it may invert P2's premise: the thesis is that a resource-wide
dictionary is smaller than per-leaf ones, and the *streaming* implementation of that idea measures
dramatically larger. Candidate causes: abandoned promotion-sample garbage, orphaned generations, or the COW
rewrite per bounded append the segment 0 gate already priced at 64.7 B/entry (D=275K) and 173 B/entry
(D=2.62M). It also gates the fresh-build plan, which promotes columns globally at 100M — a 9× tax there adds
hundreds of GB instead of removing five. Time-boxed to a diagnosis: attribute the 454 MB by page class and
index type, name the owner, report. Do not difference two builds for any storage claim until this is closed.

## W18 closed, 2026-08-31 — 26.2× → 2.36× for 0.162 B/row

`ValueDictionaryBlockIndexNode`: one record per dictionary, keyed from a `blockIndexKey` appended to the header
and read defensively like `orderedPrefixCount`. Partitions on **reverse-bucket** boundaries (256 ids) rather
than block boundaries — nearly the same partition but **total by construction**, so a spilled oversized value
needs no special case. Each entry holds the shortest prefix of its range's first value that still orders after
the previous range's last, cut at a UTF-8 code-point boundary and **verified against the comparator before
use**, falling back to the whole value when the short form does not separate. Loaded once per `ReadView`.
It is an accelerator only: with no array the range is the whole ordered prefix — slower, never wrong.

| probe | ns/probe | ratio to hash probe |
|---|---|---|
| stateless per-id read | — | 39.1× |
| cached `ReadView` | 390,982 | 26.2× |
| **+ separator array** | **33,802** | **2.36×** |

**Bytes counted INSIDE the clause**, per the ruling: URL 13.138 → 13.212, Referer 12.375 → 12.432,
Title 4.279 → 4.308, SearchPhrase 0.488 → 0.490. **Σ 30.280 → 30.442 B/row** — 161,512 B total, 0.5 % of the
dictionary, for an 11× probe improvement. Still UNDECIDED against ≤27 pending 100M.

**Four killing mutations**, and the first two initially SURVIVED: the W17 fixture never called
`buildBlockIndex`, so every search took the correct-but-unindexed fallback and the range logic never ran —
the same "witness proves less than it claims" failure as the comparator-prefix fixture, in a new place. Fixed
by building the array in the fixture and asserting `getBlockIndexKey() != 0` as a positive engagement witness.
Now `blockOf` `<=` → `<` fails at rank 257 (the first id of the second range) and a separator cut one byte
short with verification bypassed fails at rank 249. **Rule: every witness needs a positive assertion that the
mechanism under test is engaged — ask what the test would do if the feature quietly did not run.**

End to end: the rank pass builds the array in S2, and converting Title on the stock 1M database still gives
43/43 dumps byte-identical with `route=NONE` unchanged at 1.

**Deferred deliberately:** the heap-output LZ77 decode defect (`SirixLZ77Codec` silently taking the Java
decoder, 3.0 vs 16.9 GB/s). It divides both probe arms and so changes no acceptance; the absolute win belongs
to the maintenance interner. Follow-up, not a W18 fix.

## Kill switch proven, 2026-08-31 — and the byte half caught a defect nothing else could

Against a `git archive HEAD`-compiled control, fresh 1M ClickBench load, switch off:

| page class | HEAD | with segment 1, switch off |
|---|---|---|
| KeyValueLeafPage | 477,867,584 | 477,867,584 |
| OverflowPage | 664,392,355 | 664,392,355 |
| NamePage | 453 | 453 |
| RevisionRootPage | 514 | 514 |
| **total** | **1,166,924,421** | **1,166,924,421** |

Plus route identity: `# served` identical field for field, all 43 `route=` lines identical, 43/43 dumps
identical.

**THE DEFECT ONLY THE BYTE HALF COULD FIND.** The first run differed by 2 bytes on KeyValueLeafPage. The cause
was §3.1's own instruction — *"the writer emits the field unconditionally; the reader takes it defensively"*,
described there as "the whole mechanism". **That is right for compatibility and wrong for a kill switch:** the
4-byte `orderedPrefixCount` and 8-byte `blockIndexKey` were written into every dictionary header even on a
build that had never run the pass, so 12 raw bytes reached the codec and emerged as 2. A route witness would
have passed. A dump comparison would have passed.
Fixed by a **conditional trailer — both fields together or neither**, emitted only when either is non-zero and
read as a pair when the exactly-sized slot has room for both. Read independently they would misparse a zero
prefix count followed by a non-zero index key. Eliminating the delta beats accounting for it, which is what
the restated clause had settled for.

**Gating:** one switch, `-Dsirix.projection.globalDict.rank`, **default off**, covering the pass AND the block
encoder together — the compact forms only pay in rank order, so letting them be enabled independently would be
a way to change bytes without changing anything else. **Decoders are never gated**: a resource written with
the switch on stays readable with it off, or the switch would be a data-loss lever rather than a kill switch.
W17 passes in **both** switch states, so the differential covers both storage forms rather than only the
default's.

**A SHA of `sirix.data` is not a witness** — commit timestamps live in the file, so two runs of the *same*
build differ. Per-page-class written bytes is the strongest byte-level pin this store admits. (Second
independent confirmation; the first came from the flush-refusal work.)

**STILL OPEN:** the defensive-read witness (the conditional trailer *reduces* the hazard to 12 bytes of slack
rather than 4 but does not close it — it still rests on exact slot sizing on every path, and a garbage value
equal to `entryCount` would make `isFullyOrdered()` true for an intern-ordered dictionary, i.e. silent
wrong-order output on an old database); the header-anchor route witness; the maintenance-anchor pin; the
orphaned-Bloom byte accounting; the two `overflow.compress` arms.

## USER RULING 2026-08-31: no users — formats change freely

> "we have no users, we can simply switch formats"

Backward compatibility is **not a requirement**. Every database in existence is a benchmark or test artifact
regenerable from its corpus, so complexity spent preserving an older layout is pure cost: more code on a
serialization path, a wider misparse surface, and a length probe per record done for nobody.

**Consequences for this work:**
- **Write and read new fields unconditionally.** The conditional trailer (both fields or neither, emitted when
  non-zero, read as a pair) is a compatibility mechanism and should collapse to plain unconditional
  read/write. That deletes the pairing rule, the `remaining()` probe and an entire misparse class.
- **The defensive-read witness is CANCELLED.** It existed to prove a pre-P2 resource still reads correctly;
  the hazard it guarded (a padded slot letting an old header read garbage as its ordering boundary, making
  `isFullyOrdered()` true for an intern-ordered dictionary) cannot occur if no old-format record is read.
- **Rebuild instead of migrating.** Extends the standing "no format-version machinery, reuse V0" ruling and
  supplies its reason.
- **Unchanged:** the kill switch, which gates BEHAVIOUR — and decoders are still never gated, since a switch
  that made written data unreadable would be a data-loss lever whatever the compatibility policy. The witness
  reverts to route identity exact plus a byte delta accounted for exactly (12 B × dictionary headers), an
  unexplained byte being a failure.

The §3.1 finding above keeps its value as an observation — *"writer unconditional, reader defensive" is right
for compatibility and wrong for a kill switch* — it is simply moot here, which is the best way for a finding
to be retired.

## Orphaned Bloom chunks — and a blind spot in the acceptance instrument

**Measured, fixed, verified 2026-08-31.** Converting all four fat columns from per-leaf `STRING_DICT` to
rank-ordered `STRING_GLOBAL` left their Bloom bytes **byte for byte unchanged**: Title 376,503 · URL 742,839 ·
Referer 615,799 · SearchPhrase 85,943 = **1,821,084 B at 1M** (~180 MB at 100M) — written, paid for, reachable
by nothing, and sitting inside the clause this work is trying to get under.

**Why ordinary maintenance cannot clean it, and why the obvious fix is not one:**
`ProjectionBloomChunks.rewriteTouchedChunks` skips any column whose kind fails `isStringKind`, and that
predicate is `STRING_DICT || STRING_SET` — it excludes `STRING_GLOBAL`. So the instant the flip lands,
maintenance stops looking at that column's chunks forever. **Calling `rewriteTouchedChunks` with the NEW kinds
would skip the column for exactly the same reason.** The chunks must be dropped explicitly at the flip, by the
code doing the flipping: `ProjectionBloomChunks.dropColumn(storage, column, physicalRowGroupCount)`, called by
S4 before the kind flip in the same commit. Witnessed both ways — "released 5 Bloom blobs" per column, and
76,279 B remaining afterwards, all of it MobilePhoneModel, which is still `STRING_DICT` and correctly
untouched. 43/43 dumps byte-identical.

### The instrument has a blind spot, and it changes what the numbers mean

**Bloom chunks live in their own slots, not in row-group descriptors, so the descriptor-`byteLen` sum — the
instrument accepted for ≤27 and ≤40 B/row — never saw these bytes, in either direction.** That ruling was
made without knowing this. Consequence, stated plainly:

> **Every ≤27 / ≤40 B/row figure reported so far is a LOWER BOUND, not a total. Anything addressed by its own
> slot rather than by a descriptor entry is unmeasured by what we have been using.**

Enumerating the other structures with that property is required before any 100M number is taken as the answer.
It is also the first place to look for the OPEN 454 MB anomaly above: a gap between what the file weighs and
what we believe we store is exactly the shape of a slot-addressed structure nobody is counting.

## The 454 MB anomaly, DIAGNOSED 2026-08-31 — one radix node per entry

Same build, same corpus, differing only in `-Dsirix.projection.globalDict`, 1M rows, URL the one global
column (D = 275,494):

| arm | OverflowPage bytes | OverflowPage count | total |
|---|---|---|---|
| `never` (all per-leaf) | 134,694,208 | 26,220 | 612,569,426 |
| `auto` (URL streaming global) | 589,163,463 | 249,350 | 1,091,805,899 |
| **delta** | **+454,469,255** | **+223,130** | +479,236,473 |

**The page count names the cause.** 223,130 extra pages for 275,494 entries is **0.81 pages per entry**,
averaging **2,037 B** each. `ValueDictionaryRadixNode.FANOUT = 256`, and the builder allocates
`new long[FANOUT]` — **2,048 B of child pointers per node, mostly empty.** Measured 2,037 against a
theoretical 2,048 is as close as an attribution gets. The streaming path pays **1,650 B per dictionary entry**
to store values averaging 128 B, because it writes about one 2 KB radix node per entry and every one of them
is durable in an append-only store.

**The same column, done by the rank pass: 16,821,659 B = 61 B/entry — 27.0× less** (durable directory sizes,
`s1db-urlonly` vs `s1db-never`; an over-estimate, since a retrofit also leaves the superseded per-leaf bytes).
It writes **no forward index at all** — sorted blocks plus the separator array.

**A hypothesis of mine, killed and recorded as such.** I predicted per-rotation whole-radix rewrites, since
`ProjectionBulkLoad` flushes a generation at every drain. Raising `-Dsirix.autoCommit.nodes` 8× (1,048,576 →
8,388,608) did **not** reduce OverflowPage at all: rotations in the parallel-bulk path are driven by
coordinator epochs, not the auto-commit threshold, and the cost is per ENTRY, not per generation. That arm is
also not a clean A/B — `core9` was rebuilt between the two loads — so its only safe reading is "no reduction".

**Consequences.**
1. It does **not** affect the 100M ClickBench number: promotion declines above ~6–18M rows, so that database
   is entirely per-leaf.
2. It **is** a shipping-default storage defect on any resource small enough for AUTO to promote — the stock
   1M build is **78 % larger** than the same corpus with the feature off.
3. **It clears the fresh-build route**, which is why it was chased: the pre-pass writes a rank-ordered
   dictionary with no forward index, so it does not inherit the tax.
4. **It re-prices the promotion gate.** That gate's `PER_ENTRY_OVERHEAD_BYTES = 52` is a HEAP estimate of this
   same forward index; the STORAGE cost of building it is **1,650 B/entry — 32× what the gate believes it is
   admitting.** Any re-derivation of the gate has to price both.

## Deliverable 1, 2026-08-31 — the first fresh build SMALLER than its baseline

Route: load with `-Dclickbench.projection=false`, a pre-pass builds both dictionaries from sorted value files,
then the walking builder runs with `PrebuiltGlobalDictionary` injected. **No retrofit anywhere.**

| arm (all rebuilt with current code) | bytes | vs `never` |
|---|---|---|
| stock `auto` | 1,169,646,361 | +90.6 % |
| `never` | 613,610,513 | — |
| **fresh, URL+Title rank-ordered** | **573,331,369** | **−40,279,144 (−6.6 %)** |

`never` reproduced its earlier byte count **exactly** (613,610,513 twice), so the baseline is deterministic and
the delta is real. Column store confirms the shape — URL and Title kind 5 with **BODY only**, `DICT`/`BLOOM`/
`DICTHASH` all zero (2.346 and 2.155 B/row); Referer is the untouched control, still paying **27.180 B/row**
and therefore a bigger prize than either column converted.

**THE BLOCK CODEC WAS OFF.** `FreshBuildMain` never sets the master switch, so every dictionary block reached
disk PLAIN — no front coding, no LZ77. **573.3 MB is the floor with an uncompressed dictionary**; that lever
is entirely unspent.

**Correctness: 43/43 result files byte-identical.** A rank-ordered dictionary reassigns every id, so any query
leaking an id would have shown here.

**Latency: overall a win, per-query clause NOT met.** Min across 3 legs × 5 tries, interleaved: cold Σ 12.206
vs 12.701 s (−3.9 %), hot Σ 6.707 vs 6.899 s (−2.8 %). (A single 3-try leg said −8.3/−12.4 % and overstated
it; the 3-leg min is the number to quote.) Regressions q20 +766 %, q21 +132 %, q22 +82 %, q23 +51 %,
q38 +100 %; wins q5 −87 %, q33 −61 %, q27 −57 %, q17 −40 %, q39 −29 %, q28 −15 %, q42 −1.7 % (q42 is 5.5 s so
its −98 ms is most of the suite total). q16 touches **neither** converted column and swung 0.264 → 0.064 across
legs — recorded as unstable, deliberately not attributed.

**Cause of q20–q23, and the hypothesis to test first.** Those keep the SAME route in both arms, so it is cost
per value, not a routing flip: `GlobalValueDictionary.ReadView.stringOpVerdict` loops
`for (int id = 1; id <= entryCount; id++)` over all 275,494 entries per execution, uncached. But note the
arithmetic — **`never` sweeps 406,029 per-leaf URL entries, MORE values than the global arm's 275,494, and is
8.7× faster.** Fewer values cannot be slower unless the per-value cost is structurally different, which points
at the loop resolving **per id** rather than block-at-a-time — the same defect that made W18 39× before the
cached `ReadView` took it to 26×.

So: **sweep block-wise (one fetch per 256 values), do not start with a cache.** A verdict cache leaves the
first execution paying full price and the clause is per-query hot *and* cold. It also disarms the prediction
that front-coding will make these worse — true if decoding per id, much smaller if each block is decoded once
and scanned. **Run the codec-on arm after the sweep, not before.** This is on the critical path, not segment 2
backlog: without it the 40 MB cannot ship.

**Do not extrapolate −6.6 % to 100M.** D/rows is 0.28 here against 0.18 there, and fitting a slope through two
scales is what failed the original gate.

## CORRECTION to Deliverable 1: the codec was ON, and it is 80 % of the win

The section above says the block codec was off and 573.3 MB is an uncompressed floor. **Both claims are
wrong.** The codec state was inferred from `FreshBuildMain`'s source rather than verified against the running
build; a call-site counter settled it. The tell that should have been believed immediately: the "codec-on"
comparison arm came out **byte-identical to the byte** — an A/B that matches exactly is telling you the two
arms are the same arm, not that the effect is zero.

**Corrected matrix** (same code, same corpus, written high-water bytes):

| arm | bytes | vs `never` |
|---|---|---|
| stock `auto` | 1,169,646,361 | +90.6 % |
| `never` | 613,610,513 | — |
| fresh rank-ordered, codec **OFF** | 605,475,801 | −1.3 % |
| fresh rank-ordered, codec **ON** | **573,331,369** | **−6.6 %** |

Isolation (one load forked, pre-pass only): dictionary 49,816,216 B plain → 17,671,784 B with the codec
(**0.355**, inside the 0.331–0.372 band the segment-0 gate measured across three cardinality decades — a third
independent confirmation the gate path and the product path are the same path). The projection half is
**identical to the byte** in both arms. Codec election in a real build: `calls=1416 won=1415`.

**Attribution, stated so it cannot mislead:** the ordering buys 8.1 MB directly **and makes the other 32.1 MB
possible** — front coding cannot exist over an intern-ordered dictionary. "Rank alone = 1.3 %" is numerically
true and causally wrong; the codec's shrink is the yield of the ordering, not an independent lever. There is
no configuration where "rank on, codec off" is the right ship, which is why both stay behind ONE switch.

**Still open before commit:** name the mechanism by which the codec ran with `sirix.projection.globalDict.rank`
unset. The kill-switch contract is that bytes cannot change without the switch; either the injection path arms
the encoder through another door (then the contract must say so) or the gate does not cover the fresh path
(then it is a gating bug, opt-in tool or not). The 43-query leg above was run on the codec-ON database, so its
regressions describe the shippable configuration; q20's fix is being decomposed (block decodes per execution,
decode vs expansion vs scan) before choosing between a block-wise sweep and a verdict cache — the cache alone
leaves cold at 52 ms and the clause is hot AND cold.

### Gating question CLOSED (2026-08-31, from both ends)

The mechanism: the switch arrived as `-Dsirix.projection.globalDict.rank=true` on the invoking command line —
both `fresh.sh` and `load1m.sh` pass `"$@"` straight to the JVM, so it is invisible in the launcher's source,
which is exactly where the state was (mis)read. Two independent confirmations: a code trace (`ENABLED` is a
`static final` read of that one property; the only serialization arm is `NodeKind:1954`, and the only
`setProperty` in the tree is a test's setup/teardown), and a measured witness — `FreshBuildMain` with the
switch unset runs `ENABLED=false, calls=0 won=0`, plain blocks, 605,475,801 B; with it set, `calls=1416
won=1415`, 573,331,369 B; same driver, same corpus, switch the only difference. **No second door; the gate
covers the fresh path; the coupling contract holds.** The original mis-report was a configuration the
operator had set and then inferred from source — the same failure as the codec-state error, one step earlier.

A permanent fork-JVM gate test (property set before class load, both switch states, written form asserted) is
authorized to touch `build.gradle`, following the existing contract style at `:155`; setup-time `setProperty`
is NOT an acceptable shape, since `ENABLED` is `static final` and that test would rot on class-load ordering.
Commit `d8f3c99dc` was amended (unpushed) to `df9bdf6c4` to state the gating truthfully.

## q20 decomposed (2026-08-31): the cost is re-decoding 36 MB per execution, not per-id resolution

The per-id-re-decode hypothesis is **falsified by counter**: 1,083 LZ77 decodes per execution against 1,077
blocks (99.9 % native), because `sliceSlot`'s 16-slot block cache makes an ascending sweep fetch each block
once. The decomposition (min of three interleaved runs, codec on): CONTAINS 61.05 ms = EQ 26.56 (block load +
decode + iterate) + substring scan 34.49; the coded-vs-plain EQ gap is 11.10 ms of decode + front expansion.
The sharp finding: **the same substring scan over the same ~36 MB costs 7.8 ms on warm plain bytes and 33.0 ms
on freshly decoded ones** — every execution re-decodes and re-allocates the whole dictionary, so the scan runs
over cache-cold memory. `never` wins structurally because its per-leaf dictionaries are small, stable and
already resident, NOT because it sweeps fewer values (it sweeps more: 406k vs 275k).

Block-wise sweep landed (`afbf8c3b3`): worth 5–13 %, witnessed by a no-shared-code oracle (the pre-pass's
sorted value file, ops evaluated by `java.lang.String`) — four ops × 275,494 entries × both storage forms,
zero mismatches; 43/43 leg unchanged. Two harness errors self-caught by re-running: an EQ misread (predicted
25 ms recovery, got 2 — both paths pay block deserialization) and a warm-up-contaminated first measurement
that looked like a regression.

**Ranked fixes, residency FIRST (reordering the earlier cache-first instinct — residency is the only lever
that fixes cold):**
1. **Budget-bounded block residency** — decoded blocks resident by FIT, never unconditionally (1M: ~36 MB,
   trivial; 100M: URL alone ~2.3 GB decoded, all four columns 7.75 GB against an 8 GB query heap), LRU or
   generation-pinned under the same budget regime as the other caches, decode-on-demand as the witnessed
   fallback. Worth the 11.1 ms decode plus most of the ~25 ms cache-cold scan penalty.
2. **Verdict cache** keyed (headerKey, revision, op, literal) — 34 KB per literal at 1M, ~2.3 MB at 100M;
   takes repeats to zero at every scale and protects hot at 100M where full residency may not fit.
3. Vectorized scan last — warm plain scan already runs 4.6 GB/s.

**Flagged in advance:** even with residency, q20 COLD at 1M may floor at ~10–15 ms against `never`'s ~6 —
the 1M shape of a design whose payoff is O(distinct) vs O(rows) and which meets `never` on different terms at
100M (18M distinct vs 95M per-leaf entries). If it materialises it goes to the user as a per-query exception
with both scales' arithmetic, not buried and not hunted as a defect.

## ⚠ MEASUREMENT CONTAMINATION NOTICE (2026-08-31, late)

**Every absolute suite total quoted in this document for Deliverable 1 and after ran on a rig whose compiled
classes were ~4 hours stale** — 43 source files across 12 commits newer than the rig, including
`6cfc78dc3` (q42's in-kernel order plan: q42 6.081 → 0.078 s on a current build), the headroom revert, and
the overflow-compression pair. Caught when recompiling ONE file into the rig halved the suite and the whole
gain was q42, which the change being tested could not touch.

Status of each claim pending the full-rebuild re-run:
- **Storage matrix (573.3 / 605.5 / 613.6 / 1,169.6 MB): expected to survive** — all arms shared one
  classpath and the dictionary-deciding files were freshly compiled; being re-verified on a clean rig.
- **Relative latency statements** (q20 regresses ~8×, substring quartet regresses, sweep worth 5–13 %):
  survive as relatives — both arms ran the identical stale rig.
- **Absolute totals** (cold Σ 12.206 vs 12.701, hot 6.707 vs 6.899, and every Σ before them tonight):
  **unreliable, do not quote** — inflated ~6 s in both arms.
- **The `stringOpVerdict` decomposition stands** — `VerdictCostMain` calls freshly compiled core directly.
- **The verdict cache is UNMEASURED** — its first leg conflated it with the accidental one-file upgrade, and
  a fresh executor per try means it may never have survived to a second execution at all.

Lesson, permanent: **an overlay rig with per-file compiles rots silently; a leg must refuse to run when any
tracked source is newer than the rig's classes.** The guard belongs in the leg script, not in memory.

### Contamination RESOLVED (2026-08-31, rebuilt rig)

**Storage: both arms reproduced byte-for-byte** on the clean rig (`never` 613,610,513, rank+codec
573,331,369) — the matrix and the −6.6 % stand. **Corrected suite totals: rank BEATS `never`, cold Σ 6.695 vs
7.387 (−9.4 %), hot 2.689 vs 3.086 (−12.9 %)** — the stale rig had inflated both arms ~6 s and compressed the
ratio. 43/43 byte-identical on both rank arms.

**The per-query clause still fails, with a re-baselined set** (all rank-vs-never, current rig): q9 0.156 vs
0.029, q21 0.080 vs 0.025, q18 0.123 vs 0.070, q20 0.061 vs 0.012, q22 0.084 vs 0.040, q23 0.067 vs 0.047.
q9 and q18 were *wins* on the stale rig — every stale per-query claim is discarded, not adjusted.

**The executor-scoped verdict cache is reverted as worthless**: a fresh executor per try gives it a
structural 0 % hit rate (with it, hot 2.728 / q20 0.061; without, 2.689 / 0.058). Its apparent vindication
was entirely the accidental one-file upgrade. Any future cache lives in a holder that outlives executors and
is evaluated only after residency.

**Residency design ruling** (seam read): the `cachedProjectionDictionaryRecord` no-op for read-only
transactions guards the MEMO'S IMPLEMENTATION — a mutable LRU map whose read path mutates order, sound only
single-threaded — not the records, which are deliberately shareable (heap-materialized, flyweight-guarded,
COW-fresh keys). So the read side gets a SEPARATE cache: concurrent, per-resource, holding the decoded
front-expanded (offsets, bytes) block form keyed by block node key, generation header EXCLUDED (the one
stable-key rewrite; no writer hooks on the read path), budget from the cache-grant regime, decode-on-demand
as the witnessed fallback. `impl-ingest` reviews the diff before it lands (their seam; the specific question:
does anything in the flush lifecycle invalidate a decoded block under a read-only trx).

## Residency landed (fe02e228c): 58× on point reads, NOT the q20 fix — and the segment-5 tax is measured

**The staleness guard is in the script and fired on first use** (`rig-guard.sh`, exit 90, offending files
named; `rebuild.sh` the only sanctioned satisfier) — catching a `git restore` residue in the rig that neither
review nor memory would have.

**The segment-5 point-read tax, measured** (the trie lane's gating number): random id→value **24,043 →
417 ns (58×)**, sequential **140 → 75 ns**. The finding is the **320× spread between shapes** — a trie
id-lane materialising in trie order pays the random number, and at 24 µs segment 5 was dead; at 417 ns it is
arguable. Mechanism: direct-mapped decoded-block table, budget/`MAX_BLOCK_BYTES` slots, **no eviction policy
to get wrong** (collision overwrites; a miss re-decodes through the existing path). Default 128 MiB at the
measured knee (256 slots → 19.8 µs, 2,048 → 0.42 µs, flat beyond); the block table and bucket table sit in
series and both had to be sized. Eviction witness: 50,000 random reads at four budgets (16→2,048 slots)
against the no-shared-code oracle, zero mismatches — the small budgets are the point, they exercise the miss
path. 43/43 leg byte-identical.

**Residency does NOT fix q20** (0.059 with, 0.058 without): the verdict sweep walks every block once in id
order — 1,077 decodes regardless of table size; a one-pass sweep has nothing to reuse within an execution.
Predictable from the decomposition; predicted by neither builder nor lead. What q20 needs is what the dead
executor-scoped verdict cache also needed: **a per-(resource, headerKey, revision) holder that outlives
executors** — `readView()` today constructs a fresh view per call and dies with it. That holder is the next
build and absorbs three tenants under one budget: the decoded-block table (scope promoted), the verdict
bitsets keyed (op, literal), and **§6.4's length table, which is Referer's q28 blocker** — so Referer
converts AFTER the holder, or its leg measures a regression whose fix is already designed. Revision in the
identity is the invalidation (COW-immutable per revision; superseded, never invalidated in place); cold
stays honest — the first execution still decodes 1,077 blocks, and if post-holder q20 cold trails `never`,
it goes to the user as the flagged exception, not into a hunt.

**Corrected suite, current rig:** cold rank 6.913 vs never 7.387, hot 2.661 vs 3.086 — rank wins both
temperatures. Remaining hot regressions: q20 +47 ms, q22 +39, q21 +35, q23 +17 (q16 +45 touches neither
converted column). `impl-ingest` is engaged compile-only: the per-tag string-region instrument in
`PageSectionDiag` (segment-5 sizing questions (1)–(2)), and the flush-lifecycle review of `fe02e228c` plus
the holder diff. Machine ownership stays with impl-p2s1's legs.

## Verdict cache landed on the buffer manager (7044fc9a3): the quartet now BEATS never hot

**Why the first attempt measured zero, verified this time**: instrumentation showed 15 verdict sweeps across
a 43-query 3-try leg arriving on **12 distinct executor instances** (`StoreBoundExecutorCache` exists but this
runner never hits it) — an executor-scoped cache structurally cannot hit. The cache now follows the
`NamesCache` pattern on the buffer manager: `GlobalVerdictCacheKey(databaseId, resourceId, revision,
headerNodeKey, op, literalHex)`, Caffeine, **bounded by WEIGHT not count** (34 KB per verdict at 275k
distinct, ~2.3 MB at 18M — a count bound comfortable at one scale is hundreds of MB at the other).
`EmptyBufferManager` no-ops; missing is always safe.

Measured (min across three interleaved arm-pairs of five tries): **cold Σ rank 6.531 vs never 6.718, hot
1.230 vs 1.287 — rank wins both temperatures**; q20 hot 2.0 vs 11.0 ms, q21 8 vs 35, q22 14 vs 33,
q23 17 vs 43 (**2.4–5.5× faster than never**, from +49 ms behind). 43/43 byte-identical.

**Clause still unmet.** Hot: six queries >10 ms behind (q27 +75, q29 +30, q36 +23, q8 +21 — none touch the
converted columns; the set has shifted across legs, so stability must be established before they count).
Cold: **14 queries**, broad, mostly non-verdict — hypothesis: the per-leaf arm reads each leaf's dictionary
inside pages it already fetches, while the global arm adds a dictionary-page population (1M ratio 275k/406k
= 0.68, barely paying; 100M 18M/95M = 0.19, inverting). **Ruling: measure cold page reads per arm per query
before any escalation — the user gets a mechanism, not a hypothesis** — noting the global arm's leaves are
SMALLER, so net cold I/O could go either way, and the breadth of the cold set hints at changed row-group
page contents affecting non-dictionary queries too.

**The guard caught a live cross-teammate contamination**: four legs refused because impl-ingest was editing
`PageSectionDiag`/`StringRegion` (building the per-tag census = roadmap task 1 — impl-p2s1 must NOT
duplicate it) during measurement. Leg-freeze protocol now in force ("leg starting, hold edits" / "leg
done"), rebuild-before-each-arm-pair as backstop, legs run with every diag off. `rebuild.sh` additionally
fixed: `|| true` was masking javac failures and `set -e` was killing it silently.

**Order:** cold page-read counts + hot-set stability → length table (buffer-manager pattern; Referer's q28
blocker, the holder's third tenant) → Referer + full leg → clause verdict compiled for the user.

## impl-ingest delivers: the per-tag census instrument, and the fe02e228c review

**The census (task 1)** records inside the encoder's own per-tag loop on both string-region lanes — measured
write positions, not a formula — reporting per tag: leaves, values, local-dict entries, dictionary
length-table and VALUE bytes (what a resource-wide dictionary deletes), and the id lane at both the page-wide
and the tag's OWN width (the FOR price). Sub-gated behind `-Dsirix.pageSectionDiag.stringTags=true`, zero
cost off (both gates static final). **The load-bearing line is the census identity — framing + dictionaries +
id lane must equal the region's own encoded length, `residual=0 B (census EXACT)` or shout** — and it caught
its own two rounding defects while being built (per-tag bit rounding = −1; one-global rounding = +3; the lane
bytes now come from the encoder, per-tag BITS kept only for FOR pricing). Witness 5/5 including
bytes-land-on-the-OWNING-tag, plus a **negative control run**: sub-gate off ⇒ all five FAIL loudly. The
output prints the region's raw→written ratio because per-tag written bytes cannot exist (one LZ77 blob) —
**scale raw by that ratio before pricing anything**. Run: add both -D gates to a normal 1M load; map tag ids
to columns via PathDump.

**The review (task 2): fe02e228c is SOUND — no staleness path, structurally.** Decoded blocks are
fresh-allocated private heap adopted via the explicitly-named `takeOwnership` (public constructor CLONES);
block keys are write-once (`cursor.next()` mints all, including tail-extension); `readView` re-checks
revision after loading; tables are per-ReadView. Findings and rulings:
- **F1 (binding on the surviving holder):** cache the decoded block form ONLY, never the ReadView — it holds
  the `StorageEngineReader` and NamePage; retention pins a reader past its transaction. *"Cache the value,
  never the accessor."*
- **F2 (binding):** the separator array and `entryCount` are revision-scoped; under a surviving holder they
  sit UNDER the revision key, and the revision-changed→null check survives any short-circuit.
- **F3 (fix now):** `x & (SLOTS-1)` is a hash only for power-of-two SLOTS; nothing asserts it — silent
  degradation to a bad hash on a future edit. One static check.
- **F4 (fix FIRST):** the tables are allocated EAGERLY per ReadView — ~40 KB/view since 16→2048 (~125×),
  ×10+ `readView` sites per execution incl. an `aggViews[]` array ⇒ 1.5–3 MB of eager garbage per execution,
  **paid by exactly the queries the change did not help** (q20–q23 unchanged), on the allocation-rate axis
  that produced the 763 s concurrent mark at 100M. Fix: lazy allocation on first miss, plus an aggregate
  bound (the 1<<20 cap bounds the array, not the total — a mistyped budget makes ~28 MB/view legal).
- **F5:** inline `java.util.Arrays.fill` FQN (CLAUDE.md), and `READ_VIEW_BLOCK_CACHE_SIZE` now means
  "floor" — rename.

`7044fc9a3` (the verdict cache — the holder's first landed tenant) postdates the review base and goes to
impl-ingest against F1/F2. The census 1M run slots into impl-p2s1's next gap.

## Referer lands: three columns, 560.3 MB (−8.7 %), hot −27 % — and the regression set SHRANK

On the clean guarded rig, 43/43 byte-identical throughout:

| arm | bytes | vs never |
|---|---|---|
| `never` | 613,610,513 | — |
| rank+codec, URL+Title | 573,331,369 | −6.6 % |
| **rank+codec, +Referer** | **560,297,881** | **−8.7 %** |

Suite: cold Σ 6.351 vs 6.900 (−8.0 %), hot 1.126 vs 1.545 (**−27.1 %**). Column store 80.094 → 55.195 B/row.
Hot regressions **6 → 2** (q27 +18 ms, q38 +12); cold **14 → 8**. q28 — Referer's named witness — now BEATS
the per-leaf arm hot (0.210 vs 0.233; cold 0.649 vs 0.593): the length table rode the shared mechanism.
A third global column IMPROVING the clause reads as the q28 family shedding per-leaf Referer work everywhere,
outweighing the added dictionary pages — a reading, not a measurement; the cold page-read count per arm
settles it and is the next task.

**THE MARGINAL-COLUMN ECONOMICS, the number to carry into the ≤27 gate and every future election:** Referer's
projection side fell 27.180 → 2.282 B/row (−24.9 MB) but the file only shrank 13.0 MB — **its dictionary
costs ~11.9 MB, the first column where the dictionary eats more than half the saving** (227,319 distinct,
short values). Rule: **price a candidate column by (per-leaf bytes removed − dictionary bytes added), never
by the projection-side delta alone.** SearchPhrase (18,316 distinct, 56.9 B mean) gets this test before it is
ever converted, arms or no arms.

## 7044fc9a3 reviewed: F1/F2 PASS; five findings, one a latent wrong-answer bug

Census committed as `4db2bd1a5`. The verdict cache passes both binding constraints — bare `long[]` cached
(the value, never the accessor), keyed by the *validated* `view.revision()`, key complete for the predicate,
cross-resource correct. Findings and rulings:

- **V1 (FIX NOW — latent wrong answer):** the key omits `entryCount`, but the call site pairs a cached
  verdict with the CURRENT view's count. Revision freezes the dictionary only for a COMMITTED revision, and
  `JsonNodeTrx extends JsonNodeReadOnlyTrx` — the static type does NOT prove read-only. A query over an
  uncommitted write trx sees a stable revision while the dictionary grows: a verdict sized N riding with
  entryCount N+M is an AIOOBE or silently dropped rows. Fix: `entryCount` into the key — free, already read
  at the call site. *Revision is only a sufficient key while the dictionary cannot change under it.*
- **V2:** the shared `long[]` reaches every worker; "arms only bit-test" is true today, unverifiable
  tomorrow, and a mutating arm corrupts silently while passing every test. Ruling: a final wrapper exposing
  only `test(id)` (monomorphic, inlined, ~zero cost) plus the SHARED-never-mutate sentence at both ends.
- **V3:** the unused 2-arg `get(key, fn)` would run an O(distinct) sweep under the CHM bin lock (58 ms at
  1M, seconds at 18M, unrelated keys blocked). Dead + trap = **delete**.
- **V4 (MANDATORY before any 100M leg):** three cache budgets share one 8 GB query heap and nothing sums
  them — 64 MiB verdicts + **128 MiB decoded blocks PER VIEW** (10+ views/execution ⇒ >1 GB worst case at
  100M) + page caches. An accounting table joins the clause-verdict report; the per-view block budget is
  also the strongest argument for promoting the block table into the shared cross-execution holder.
- **V5:** unreachable `toSecondCache()`, and the Caffeine inline FQN gets a comment naming the
  `io.sirix.cache.Cache` collision (Java has no import alias; justified exception over silent one).

## Cold hypothesis CONFIRMED and fixed (5d4f33fa4): hot −33 %, cold at parity

**The count that settled it: 26,300 LZ77 decode dispatches per cold leg on the three-column build against
125 on `never`** — 26,175 extra dictionary decodes, because the read view retains decoded blocks only for its
own lifetime and the engine builds one view per execution. Fix: a buffer-manager cache of decoded dictionary
records consulted by `NamePage#getProjectionValueDictionaryRecord` after the writer memo — same shape as the
verdict cache; the generation header (the one stable-key rewrite) is **evicted at the same put-path point
that evicts the writer memo**, with revision in the key as the second line; weight-bounded in decoded bytes
(64 KiB value block vs a radix node's references), default 256 MiB.

| | decodes/leg | cold Σ | hot Σ |
|---|---|---|---|
| rank3 before | 26,300 | 6.351 | 1.126 |
| **rank3 after** | **2,455** | **6.281** | **0.902** |
| never | 125 | 6.307 | 1.354 |

**Three-column build: −8.7 % storage, hot −33 %, cold PARITY, 43/43 byte-identical.** Hot regressions are
down to one (q29 +20 ms — noting the hot straggler has changed identity three times, which smells of noise);
cold's 12-query set **shifts membership while its total stays flat, the noise signature**. Ruling: ≥5
interleaved pairs classify every remaining regression STABLE vs UNSTABLE; only stable members enter the
clause verdict. The holder landed without its ordered pre-review — local and revisable; impl-ingest reviews
`5d4f33fa4`+`7044fc9a3` from git now, including the generation-header eviction (the write-once exception, now
load-bearing twice), key completeness at the consumers, and whether this cache makes the per-view block
tables partially redundant (a V4 budget and deletion question). The V4 table gains a fourth budget: 256 MiB
records + 64 MiB verdicts + 128 MiB/view blocks + page caches vs the 8 GB query heap.

## Six-pair stability verdict — hot MET; cold reduced to TWO real regressions (and a −33 % corrected)

**Correction first: "hot −33 %" was one favourable 3-pair sample.** Six interleaved pairs, suite delta per
pair (rank3 − never): cold −0.605/+0.252/−0.053/+0.122/−0.165/+0.111 (mean −0.056 s); hot
−0.579/−0.006/−0.275/+0.319/−0.485/−0.115 (mean −0.190 s). **The spread exceeds the effect: honest suite
numbers are cold PARITY (−0.9 %), hot ≈ −15 %.** Policy: suite totals from six pairs or not at all;
per-query effects may be claimed from three.

**Per-query clause, tested as "in how many of six pairs does this query regress >10 ms":**
- **HOT: MET. Nothing is 6/6** (worst: q42 and q27 at 4/6, both with faster pairs).
- **COLD: exactly two 6/6 regressions, both URL substring predicates paying the FIRST verdict build:**
  q20 mean +43 ms [range +15,+72], q22 mean +22 ms [+17,+29]. The earlier twelve-query cold list was
  membership churn. The record cache cannot remove a genuine first touch — this is the predicted
  "cache fixes hot; cold pays one full build of the derived structure", measured and isolated.

**The two-scale arithmetic for the user's decision:** at 1M the first build sweeps 275,494 distinct against
the per-leaf arm's 406,029 entries (ratio 0.68) plus an extra dictionary-page fetch population → ~40 ms
behind on that one execution. At 100M it is ~18M distinct against ~95M per-leaf entries (**ratio 0.19**) —
the same arithmetic inverts and the global arm should win cold outright.

**Three-column build, final at 1M:** 560,297,881 B (−8.7 %), cold parity, hot ≈ −15 %, hot clause met, cold
exception = {q20 +43 ms, q22 +22 ms, first execution only}, 43/43 byte-identical every leg, decodes
26,300 → 2,455 (never: 125).

### USER RULING on the cold exception (2026-08-31): FIX COLD FIRST

The two-query first-execution exception (q20 +43 ms, q22 +22 ms) is **not accepted**; the no-per-query-
regression rule holds strictly hot AND cold, and **the 100M build waits** until both drop below 6/6 at
>10 ms under the same six-pair protocol that found them.

Plan of record: decompose the first execution (page I/O vs block decode vs cold-allocation scan vs bitset
build) BEFORE building anything; the likely fix is an **asynchronous dictionary warmer into the existing
buffer-manager record cache** — general (any kind-5 consumer benefits, dictionary warmup at open is normal
database behaviour), no new budget (fills an existing weight-bounded cache; warms what fits in id order at
scales where the dictionary exceeds it), never blocking, race-safe by construction (a mid-warm query decodes
the remainder through the existing path), behind the master switch, with an engagement witness via the
dispatch counters. If the decomposition says page I/O dominates instead, a dictionary-page prefetch is the
alternative — chosen by the table, not by preference. The precondition checklist (V1 wrong-answer fix, V2/V3,
F3/F4/F5, census run, V4 accounting) proceeds in parallel and gates 100M regardless.

## Convergent discovery: the record cache accepted uncommitted state — found twice, fixed once (4956fe5c0)

Within minutes of each other and independently: **impl-ingest's holder review** (H1: the cache omits the
`writeCopy || hasTrxIntentLog()` guard that `NamesCache` — the precedent it cites — has thirty lines away;
uncommitted records enter a resource-lifetime cache under a revision an aborted transaction's successor
reuses, and NOTHING clears buffer-manager caches on rollback) and **impl-p2s1's pattern audit** after fixing
V1 (a node key identifies content only for COMMITTED records). Fixed in `4956fe5c0` together with **V1**
(`entryCount` into the verdict key — ids past a stale bitset's end read as no-match: silently dropped rows,
no exception, no counter). impl-ingest verifies the fix mechanically against the extended baseline.

**The lesson, stated once:** *neither bug was reachable through the benchmark* — every leg is read-only, so
43/43 stayed byte-identical through both. **Review a cache's key against its invalidation story; a green leg
proves nothing about write-path interactions.** And H1's generalisation of V1: before asking whether the key
is complete, ask whether the value is cacheable AT ALL — no key makes a non-durable value safe.

**Also verified sound in the same review:** the generation-header eviction, on every path —
`NamePage.putProjectionValueDictionaryRecord` is the single choke point (the only `persistRecord` for that
offset in the tree), every writer routes through it including `flushAppend`'s stable-key rewrite, and the
remove precedes the persist inside the writer's own transaction. Recorded in the baseline as
verified-do-not-re-review.

**H2 ordered:** the record cache IS the shared L2 for decoded blocks, so `fe02e228c`'s 2,048-slot per-view
default was measured with nothing behind it — the knee is stale. Re-measure the slot-count curve WITH the L2,
shrink the per-view default to the new knee (discharging most of F4's eager allocation), and the V4
accounting then totals three real budgets instead of four overlapping ones.

Still open: V2 wrapper, V3 deletion, F3/F5, the census run, the §6.4 length-table answer, the V4 table —
and the critical path per the user ruling: the q20/q22 cold first-touch decomposition and fix.

### 4956fe5c0 verified per baseline row: V1 CLOSED; H1 ruled to the precedent's exact guard

**V1 CLOSED, better than specified:** `entryCount` in the key is the same `final` `ReadView` field later
paired at the call site — key and pairing cannot diverge even in principle.

**H1: the landed `instanceof StorageEngineWriter` guard closes the reachable exposure** (traced, not assumed:
a write trx's `getStorageEngineReader()` returns the writer itself), **but is ruled to change to the
precedent's form verbatim — `writeCopy || storageEngineReader.hasTrxIntentLog()` on both consult and
populate.** Two residual holes the type test leaves, stated with reachability rather than inflated:
`writeCopy` is a PAGE property no reader test can see (and `NamesCache:526`'s own comment records that a
reader-state test alone was found insufficient once before), and a delegate reader carrying a live intent log
is not a `StorageEngineWriter`. Neither is on the dictionary path today. The deciding argument is
consistency: **two caches in one file guarding with different tests is how the third one gets written
wrong.** V2/V3/V5 remain open per the same mechanical verification.

## H2/V4 resolved by supersession (c4ea56f16): the per-view block table defaults OFF

With the shared record cache present, the per-view table buys nothing the leg can measure: point reads
381 ns (16 slots) vs 312 ns (2,048), cold 6.786 vs 6.744 s, hot 1.288 vs 1.302 — **the spread within each
configuration exceeds the difference between them** (and a single leg said 1.752 vs 1.302 and would have
argued hard for the wrong answer; three legs each decided it). The shape argument outranks the numbers: a
per-view budget is claimed once PER VIEW, a shared cache once. The knob stays for contexts with no shared
cache; only the default moved.

**The V4 accounting table, now that deletion shortened it:**

| cache | budget |
|---|---|
| verdict cache (shared, weight-bounded) | 64 MiB |
| dictionary record cache (shared, weight-bounded) | 256 MiB |
| per-view block table | **0** (was 128 MiB × views) |
| **total claim vs 8 GB query heap** | **320 MiB = 3.9 %** |

Both caches degrade to re-reading, so the ceiling is true, not aspirational. **V4 CLOSED.**

Standing state: three global columns landed default-off behind the master switch — 560,297,881 B (−8.7 %),
2,455 decodes/leg, cold parity, hot ≈ −15 %, hot clause met, cold = {q20 +43, q22 +22 ms} first-touch,
43/43 every leg, 320 MiB memory ceiling. Open: H1's one-line guard-form change, V2/V3/V5 hygiene, the §6.4
length-table answer, the census run — and **the critical path, not yet started: the q20 first-execution
decomposition** that the user's fix-cold-first ruling gates 100M on.
