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

### V2/V3 closed (04a69f670) — V2 by an accepted substitution

**V3 as ruled:** the compute overload now refuses — `asMap().compute` would run a full O(distinct) sweep
under the CHM bin lock, stalling same-bin strangers; get-then-put duplicates a sweep at worst and never
stalls. **V2 by substitution, accepted:** instead of a `test(id)` wrapper — a method call in the kernels'
innermost per-row loop, which must be MEASURED against the three arms rather than assumed free — the cache
**copies the verdict on the way out**: the same invariant (nothing a consumer holds is the cached array) at a
place where the cost is a rounding error (34 KB at 1M / 2.3 MB at 100M per cache exit, ~250× cheaper than the
sweep it replaces). Substituting openly with reasoning, rather than quietly doing neither, is the standard.

**Correction to the same report's closing claim:** "everything closed except two segment-5 numbers" is wrong —
the USER'S FIX-COLD-FIRST RULING remains open (the report still calls q20/q22 a pre-authorised exception; the
user rejected the exception), along with H1's guard-form change, the census run, and the §6.4 one-liner. The
q20 first-execution decomposition is the critical path and had not started as of this report.

## The cold decomposition (critical path): first-touch is 86 %, fetch:decode ≈ 2:1 — the warmer wins

Instrument: the record cache itself — call 1 fetches and decodes, call 2 finds residency, so call1 − call2
IS the first-touch cost; CONTAINS vs EQ over the same resident blocks separates scan from iteration. Three
runs, stable:

| | codec ON | codec OFF |
|---|---|---|
| CONTAINS call 1 (cold) | 142 ms (decodes 1,085) | 104 ms (decodes 2) |
| CONTAINS call 2+ (warm) | 18.4 ms | 19.8 ms |
| **first touch** | **123 ms (86 %)** = fetch+deserialize 84 (68 %) + decode+expand 39 (32 %) | 84 ms |
| substring scan (warm) | 17 ms (12 %) | 18 ms |
| iterate + bitset | 1.7 ms | 2.4 ms |

**The design rule the table teaches: when cost A is a prerequisite of cost B, a fix removing A removes both —
"A dominates" argues FOR the warmer, not for prefetch.** A page prefetch strands the 39 ms of decode; the
warmer (which must fetch to decode) removes all 123. Selected by arithmetic, not taste.

**Two banked findings:** *once resident, coded and plain perform identically* (18.4 vs 19.8 ms) — the block
codec is a ONE-TIME expansion cost, not a scan tax, which is the opposite of the overflow-compression story
(there the OS cache held compressed pages and every scan re-decoded; here the decoded form has a home) and is
the argument that codec-on is shippable where that lever was not. And **77 µs per overflow-page
fetch-and-deserialize** joins the design constants.

**The warmer builds now** to the standing constraints (existing 256 MiB cache, async, master switch, id-order
partial warmth, F1 value-never-accessor), with three requirements: an engagement witness (warmed-block
counter >0 warm / 0 off), impl-ingest's two pre-positioned questions answered IN the javadoc (write-trx
behaviour; eviction race), acceptance by the six-pair protocol — q20 AND q22 below 6/6 cold.

**§6.4 RESOLVED BY STRIKETHROUGH: the length table does not exist and never did.** q28's 4.71 s → 0.210 came
from Referer becoming kind-5 served by the verdict + record caches. The design doc gets the strikethrough
with the warmer commit so no future reader implements it.

## The warmer exists — pre-reviewed peer-to-peer; three binding findings before the acceptance leg

Design verified by impl-ingest (read-only, mid-freeze): the warmer guards on `wtx != null` AND **opens its
own read-only transaction at a committed revision** rather than borrowing one — structurally ruling out the
uncommitted-state class rather than guarding against it; the eviction race is benign (some blocks resident,
others fetched, never wrong); and the warm budget is charged in the same currency as the cache's weigher
(`rawBytes().length`, the accessor returning the internal array by design).

**Binding before the six-pair acceptance leg:**
1. **Per-RESOURCE warm plan** — 192 MiB × 3 anchors = 576 MiB requested into a 256 MiB cache: the third
   dictionary evicts the first. One shared plan across anchors, stopping at the bound minus headroom.
2. **A residency counter joins the engagement witness** — blocks-warmed is blind to eviction churn (warm 96k,
   evict 60k, same number); blocks-still-resident is what moves at 100M.
3. **Per-anchor transaction dedupe** — 129 transaction LIFETIMES per leg, resource scope the fix.

**Caveat, verbatim, bounding what the acceptance proves: a green 1M leg is not evidence about 100M warm
behaviour.** At 1M all three dictionaries (~108 MB) fit the bound; at 100M URL alone is ~2.3 GB decoded, so
the warmer holds a fraction and the 100M cold story rests on the 0.19 sweep-ratio arithmetic, not residency.
The 1M leg answers the user's gate (q20/q22 below 6/6 cold); the 100M leg measures the rest. Two claims,
kept separate.

### USER DEADLINE (set 22:42, 2026-08-31): hard stop 06:00 Berlin

Schedule of record, planned backwards: warmer fixes + hygiene by ~00:30 → six-pair cold acceptance leg
(the user's gate: q20 AND q22 below 6/6 at >10 ms) by ~02:30 → **if green, the 100M fresh build launches,
last safe launch 03:15** (three columns, codec on, census riding it, against the standing 69.51 GB
baseline) → nothing new after 05:00; commit, clean tree, handoff section at the end of this file → stop at
06:00 regardless of state. A gate failure means no 100M and the failure analysis is the handoff — the rule
held, the numbers say what to fix next.

## THE GATE IS MET (821fedfa1): zero stable regressions, cold AND hot, nine pairs

Acceptance under the same pair-counting protocol that found the regressions: **stable (9/9) regressions
>10 ms — cold 0, hot 0**; suite medians cold −0.225 s, hot −0.269 s; 43/43 byte-identical. **q20 and q22 —
the two the user refused to accept — are wins in most pairs and stable in none.** One outlier pair (+4.14 s
cold) kept in, protocol robust by construction, medians quoted for that reason. Nearest survivor: q1 at 8/9
cold +31 ms — its pre-existing behaviour.

**The warmer's own first version created a regression, and the acceptance instrument caught it:** dedupe
scoped to the executor meant the warm walk repeated 43× per leg (95,912 touches for 2,327 blocks) — pure
cache hits, no decodes, but running beside queries it put a NEW stable +57 ms on q1. Fixed by scoping to
(database, resource, revision, dictionary). **A background optimisation is measured on the foreground it
competes with** — the lesson at one more layer.

Engagement witness: 2,327 warmed on, 0 off; decodes unchanged at 2,455.

**GO issued for the 100M full package** behind a five-item pre-launch checklist: binding warmer findings #1
(per-resource warm plan — at 100M three ~2.3 GB dictionaries into a 256 MiB cache without a shared plan
thrash at load time) and #2 (residency counter — the only number that says what the warmer bought at 100M),
H1's guard one-liner, F3/F5/V5 hygiene, and the launch protocol read. Launch shape: load with census gates
ON (banks the 100M shares), legs with ALL diags OFF, the standing baseline RE-LEGGED on the current build in
the same session (its recorded numbers are older code), minimum two interleaved pairs cold+hot with the
stability caveat named, storage number reported the moment the load completes.

### 821fedfa1 reviewed: flush-lifecycle NO HOLE; checklist gains item 6

The warmer widens nothing: it runs only with `wtx == null` on its OWN read-only transaction at a committed
revision, so it populates under `(db, res, committedRev, key)` while any writer evicts under the next
revision — the own-transaction choice is what makes the verdict true.

**Item 6 (the largest finding in the diff): `WARMED_DICTIONARIES` moves off its private static onto the
buffer manager.** `LocalDatabase.removeResource`'s own comment documents the hazard its clearing contract
exists for — resource recreated with the same ids — and the static is a third piece of `(databaseId,
resourceId)`-keyed state that contract cannot reach: delete → caches cleared → marker survives → recreate →
**the warmer returns 0 forever in that JVM**, self-healing only past 256 markers, which a single-resource
benchmark JVM never reaches. On the buffer manager it inherits `clearCachesForResource` for free. The
blocks-warmed witness would read healthy over that dead warmer (first incarnation's count) — the residency
counter (item 2) closes the third ran-vs-persisted blindness in one subsystem.

Also on the record: the 9/9-clean acceptance is *consistent with* W1 not biting at 1M, not evidence against
W1 — the green is silent on it. And the collaboration shape worth keeping: W2 was measured rather than
accepted (an estimate of "~96k redundant lookups" became a demonstrated +57 ms stable regression), and the
static was pre-flagged by its author rather than left for the reviewer to find.

### H1/F3/F4/F5 landed (9799bb10f); a 12× ratio misapplication struck; launch blocked on three

H1 verbatim (`writeCopy || hasTrxIntentLog()`, both consult and populate); F4 both halves — lazy first-miss
allocation AND the aggregate bound (the old `1<<20` cap bounded SLOTS, not bytes: 1<<20 × 64 KiB = 64 GiB
legal; a 512 MiB resident ceiling now also caps slots); F3's power-of-two static refusal; F5's import +
`READ_VIEW_TABLE_FLOOR` rename. Point-read oracle zero mismatches in BOTH budget configurations.

**Struck before it could propagate: "268.5 MB raw ≈ 13.7 MB written at wire/staging=0.051".** That ratio is
the whole staged body vs its wire form, misapplied to one section. The string region's own ratio is
**0.644** (census instrumentation, cross-validated against the 100M log's 0.652) — written region at 1M is
**172.9 MB**, slices scale by 0.644. The substitute number was 12× off.

**Launch remains blocked on exactly three small items** (every recent report has crossed them): W1 the
per-resource warm plan, W1b the residency counter, item 6 the `WARMED_DICTIONARIES` relocation to the
buffer manager. Commit hashes for the three, then launch per the prescribed shape.

### All seven pre-9799bb10f commits reviewed; the launch list is final at FOUR

Verdicts: **V4/H2 fixed better than prescribed** (per-view budget defaulted to 0 AND the knee re-measured
with the L2 present, honestly reported as inside the noise — which also disposes of the eager-allocation
half, ~40 KB → ~0.3 KB per view); **V3 fixed and verified unreachable**; **V2 substantially fixed with one
residual** — `get` clones, but the MISS path puts the array and hands the same reference onward, so the
populating caller aliases the cached array: fixed by one word, `put(key, verdict.clone())`, on the path that
already paid the 58 ms sweep.

**A ruling error, owned by the lead:** V2's original "the wrapper — enforcement at zero cost" was wrong — a
`test(id)` call in the kernels' innermost per-row loop is a bet on JIT inlining stated as a fact, in the one
place the codebase's rules demand measurement. It passed TWO reviewers (the lead who ruled it, the reviewer
who carried the phrasing) before the implementer caught it. **A ruling's cost claim deserves the same
verify-don't-assert treatment as any other claim — rulings arrive pre-trusted, which is the hazard.**

**Final pre-launch list, four items:** W1 (per-resource warm plan), W1b (residency counter), the
`WARMED_DICTIONARIES` relocation, the V2 miss-path clone. Hashes in, launch immediately — impl-ingest's
mechanical verification of `9799bb10f` + the four runs in parallel and gates the LANDING, not the launch.

### 9799bb10f verified; the ratio misread's ROOT CAUSE closed (3cd2de6cb)

H1 is character-for-character the `:526` test — the two caches in one file now answer the same question the
same way. F3's invariant is enforceable, F4's aggregate ceiling bounds BYTES (the old cap bounded array
length; a mistyped property could legally request 64 GiB of references).

**`3cd2de6cb` closes the door the 12× error came through rather than only striking the instance:** the
instrument printed two ratios lines apart — the body-wide `wire/staging` FIRST and unscoped, each region's
own `raw→written` after — so anyone scanning for "the ratio" hit the wrong one. The body line now states its
scope and that it applies to no region. **The generalised rule, third instance tonight: when a number is
misread, ask whether the OUTPUT invited it** (the residual line, the believe-the-residual contract, the
ratio scope — all the same move: fix the instrument, not the reader). Landed comment-only before the launch
so the build carries it.

### All four launch fixes verified GREEN (working tree); launch ordered

W1 better than prescribed — pass-scoped budget PLUS a 0.75 share so a warm pass cannot evict the
demand-loaded entries a query is about to want (a gap the reviewer had missed). W1b is a genuine re-probe
that can say "no": it walks the warmed keys and counts only what survives in the cache — the ran-vs-persisted
gap made visible. The marker joins `clearDictionaryCachesForResource` (all three dictionary caches cleared by
(databaseId, resourceId) on the deletion path). The V2 residual closed by the one-word clone on the miss path.

**Drift note, ruled first post-launch item:** `SirixVectorizedExecutor:825` re-declares
`GLOBAL_DICT_RECORD_CACHE_BYTES` (same property, same default) that `BufferManagerImpl:241` owns, and W1's
budget computes from it — two places, one truth, the shape `9799bb10f` just unified in `NamePage`. Fix: ask
the buffer manager for its bound. Degrades warming, not answers; the failure needs someone to edit one of
two defaults alone.

**Launch ordered: commit the four by name, then GO** — census gates on the load, diags off in legs, baseline
re-legged on today's build, two interleaved pairs minimum, storage number the moment the load completes.

## LAUNCH: all six confirmed with hashes (9234502b3, 9799bb10f) — extraction-first sequencing

W1 (pass planned as a whole: equal shares of 75 % of the record-cache bound), W1b (residency counter;
warmed 2,327 / resident 2,327 at 1M — equal as predicted, the gap is what moves at 100M), H1, F3/F5/V5,
protocol read (envelope, `expectedRows=99997497`, SIGTERM-not-KILL, marker-file waiting), marker relocated.
The V2 miss-path clone also landed — reader 1 no longer aliases the value it populated.

**THE FIND INSIDE ITEM 6, bigger than the item: `clearCachesForResource` was not sweeping the verdict or
record caches.** Both are keyed `(databaseId, resourceId, …)`; a resource deleted and recreated with the same
ids would have been served its predecessor's ANSWERS — the wrong-answer class again, found because relocating
the marker forced an end-to-end read of the clearing contract. All three dictionary caches are now swept
first in that method. The reviewer's marker finding turned out to be the smallest of three exposures behind
one door.

**Risk picture, flagged before launch:** disk 109 GB free against a ~64–70 GB write (the protocol's 160 GB
threshold predates the diet; margin 39–45 GB — workable, nothing deleted mid-run); and the fresh route's
one never-run-at-100M step — extracting sorted distinct values per column — is sequenced FIRST to fail fast,
~6 min/column measured by extrapolation from 3.4 s at 1M, run with a ~20 GB heap so an OOM would indict the
design rather than the room. Projection build budgeted at ~100 min as an honestly-flagged one-scale slope;
overrun falls to the load-only tier.

### 9234502b3 verified green; the post-run list; the trust-chain lesson completed

Baseline table fully green at the launch commits. **Post-run list (neither a blocker, neither touched before
the run):** the executor's duplicate `GLOBAL_DICT_RECORD_CACHE_BYTES` declaration (ask the buffer manager for
its bound), and the new dictionary sweep sitting outside `removeResource`'s try/catch while the comment below
argues sweeps must not depend on each other (the file's own standard applied to its newest line).

**The trust-chain lesson, now attached to all three roles:** the lead's ruling stated a cost claim as fact
("zero cost" — a JIT-inlining bet); the reviewer's prescription stated an inheritance claim from plausibility
("inherits the clearing for free" — the contract, unopened, swept neither new cache); the implementer's
premise read a codec state from a launcher. One family — trusting a description of code instead of the code —
and each catch came from whoever went and read it. Standing rule, all roles: **a prescription, a ruling's
cost claim, and a premise each take the same verify-don't-assert standard as a finding; if the check is not
worth making, hand over the claim unmade.** And a finding that leads someone to a bigger finding did its job
twice: the marker was the door to the wrong-answer sweep gap.

## The fail-fast fired: S1's arena overflowed int at 100M — fixed in 20 minutes instead of after a wasted load

All three extractions died with `NegativeArraySizeException: -2147483648`: the harness extractor's value
arena was a single `byte[]` with `int[]` offsets doubling on demand, and URL's distinct set at 100M is
3,374,063,038 bytes — the doubling overflowed `Integer.MAX_VALUE`. **This is the product's own historical
bug re-materialised in the harness**: the `arena.length << 1` int-overflow that killed load100c months ago
(recorded in `globaldict-promotion-budget-crossover`). Same class, new instance; growth arithmetic in `int`
is a recurring species. Fix: a chunked 256 MiB `byte[][]` arena with `long[]` offsets, values never
straddling a chunk; the flat gate path now REFUSES loudly past 2 GiB instead of overflowing.

**A false pass nearly accepted, self-caught:** the first "byte-identical at 1M" verification ran while the
compile was still failing one line further down — it had silently used the OLD class. Third staleness
instance tonight; a green result from a build not confirmed compiled is not a result.

**URL at 100M, the first 100M pre-pass ever:** 18,342,022 distinct / 99,929,734 present, distinct bytes
3,374,063,038, zero malformed UTF-8, 376 s. **Two independent confirmations:** 3,374,063,038 / 18,342,022 =
**184.0 B** — the gate's distinct-weighted mean reproduced by a different instrument — and the cold story's
sweep ratio is now MEASURED at 18.34M/99.93M = **0.184** (quoted until now as ~0.19 derived). Disk after
URL: 105 GB free; three value files ~7–8 GB total against an expected ~64–70 GB DB.

## All three columns extracted at 100M (18.5 min); load launched 23:36

| column | distinct | distinct bytes | present | dist-wtd mean |
|---|---|---|---|---|
| URL | 18,342,022 | 3,374,063,038 | 99,929,734 | 184.0 B |
| Title | 9,425,427 | 1,256,841,188 | 85,087,080 | 133.4 B |
| Referer | **19,720,797** | 2,689,028,780 | 81,032,736 | 136.4 B |
| Σ | | **7,319,933,006** | | |

**Three findings above the run itself:**
1. **The gate's central number holds: 7.32 GB measured against 7.75 predicted** (5.5 % low), by a different
   instrument, months of argument apart.
2. **Referer is the HIGHEST-cardinality column in the corpus** — 19.7M distinct against URL's 18.3M, at
   shorter values — inverting the all-night ordering assumption. With its dictionary already eating half its
   saving at 1M, the marginal-column test may go AGAINST Referer at 100M. **Decision rule pre-positioned: if
   the storage number decomposes to a net-negative Referer, the follow-up is a two-column arm next session —
   the fresh route is per-column configurable, so dropping a column is a rebuild, not a design change.**
3. The cold story's sweep ratio is measured on both ends now: 0.68 at 1M → **0.184** at 100M.

Load: document-only, protocol envelope, census gates ON, started 23:36, ~46 min expected. Disk 102 GB free
after 7.4 GB of value files; 32–38 GB of margin expected at completion. Sequence: load result → storage
number before any leg → projection build → legs; build overrun past 03:15 falls to load-only without asking.

## 100M LOAD COMPLETE (37.2 min, exit 0): document-only 52.15 GB vs the 69.63 GB with-projection baseline

Not like-for-like — the projection is building now — but the document alone is **17.5 GB under a baseline
that already carries its projection**. (1M ratio would put the finished build near ~64 GB; recorded as an
extrapolation, not a result. Build launched 00:15.)

**The 100M census, banked at zero extra cost, `residual=0 B`** — the last 1M-derived numbers are now
measured:

| quantity | 1M | **100M measured** |
|---|---|---|
| three converted columns' region share | 63.0 % | **59.0 % (17.49 GB raw)** |
| OriginalURL share | 8.5 % | **9.7 % (grows with scale — strengthens its case)** |
| temporal (incl. EventDate) | 18.1 % | **19.8 % (5.86 GB raw)** |
| id-lane FOR saving | 58.3 % | **60.0 %** |
| region split | 91.7 / 4.4 / 3.9 | **92.5 / 3.9 / 3.6** |

**The lever's mechanism, measured in one number each:** per-leaf duplication across 10.2M leaves —
URL 66.9M local entries for 18.3M distinct = **3.65×**, Title **6.5×**, Referer **2.9×**. (Referer lowest,
consistent with the marginal-economics concern: high cardinality, least dedup.)

The census header now states the body ratio applies to no region (`3cd2de6cb` doing its job on its first
100M outing). Disk 53 GB free; ~41 GB expected after the projection.

## Build failure: the scaffold's own refusal caught a chunk-transition bug in the hour-old arena fix

`PrebuiltGlobalDictionary` refused a URL the pre-pass had never seen — the loud refusal it was built to make,
and it was RIGHT: appending would have minted an id no reader could resolve. Root cause, four minutes after
fixing the error message to name the VALUE and not just the fact: **the chunked arena's `ensure` wrote the
next chunk's start into `offsets[count]`, which is simultaneously the END of the previous entry** — the pad
was attributed to the preceding value, corrupting ~12 values per column (one per 256 MiB chunk transition).

**Why every check passed: the 1M verification ran at a scale where the changed branch never executes** (35 MB
arena, zero transitions). "Byte-identical at 1M" was a green light from code whose new path never ran — the
hollow-witness shape again, self-committed four hours after writing the memory about it, self-named.

**Fix + witness:** starts and lengths in separate arrays; padding moves a write CURSOR no entry depends on;
chunk size made configurable SPECIFICALLY so a small run can force the boundary path — 1M at 64 KB chunks =
**539 transitions, byte-identical to the known-good file, `distinctBytes` unchanged** (confirming padding is
no longer counted as data). The knob's javadoc says it exists for the test.

**Full reload, not reuse:** the first build's pre-pass had committed 2.91 GB of dictionaries built from
corrupted values, and an append-only store cannot take them back — the postpass lesson applied to our own
artifact. Re-extract 00:42 → load → build; inside the 03:15 cutoff with the 04:00 load-only tier as
fallback. The 1M gate, all commits, and the 100M census predate the bug and are unaffected.

### 02:30 status: the reload reproduced BYTE-FOR-BYTE; build past the failure point

Two independent 37-minute loads → **52,152,369,152 B, digit for digit** — the strongest determinism evidence
of the campaign; the storage number will be a property of the BUILD, not the run. The corrected pre-pass
rebuilt all three dictionaries in 76 s (each count exactly one above the extractor's — the prepended empty
string), costing **2.91 GB on disk for 7.32 GB of distinct values = 0.40**, inside the codec's measured
0.355–0.42 band. The walking build passed its previous failure point with zero exceptions — the arena fix
confirmed by the run, not only the witness. ETA ~03:30 (flagged as a one-scale slope); leg-vs-bank decision
pre-made per the tiers; disk comfortable.

---

## Handoff — 2026-09-01, impl-p2s1

Written at 02:55 while the 100M projection build runs, so it is not typed under a deadline.

### What is proven

**The 1M per-query gate is MET.** Three columns (URL, Title, Referer) converted to resource-wide
rank-ordered dictionaries in a fresh build. Under the pair-counting protocol — nine interleaved
arm-pairs of five tries, a regression counted only if it appears in EVERY pair — there are **zero
stable regressions at either temperature**, medians −0.225 s cold and −0.269 s hot, and 43/43 query
results byte-identical to the `never` oracle at every step of every change.

| | bytes | vs `never` |
|---|---|---|
| stock `auto` | 1,169,646,361 | +90.6 % |
| `never` | 613,610,513 | — |
| rank, codec off | 605,475,801 | −1.3 % |
| rank + codec, 2 columns | 573,331,369 | −6.6 % |
| rank + codec, 3 columns | **560,297,881** | **−8.7 %** |

The two lines are not independent levers: front coding and in-record LZ77 exist only because the ids
are in collation order, so the ordering buys 8.1 MB directly and makes the other 32.1 MB possible.

**At 100M, banked and independent of the unfinished build:**
- Document-only load: **52,152,369,152 B**, reproduced **byte-for-byte** across two independent
  37-minute loads. The baseline that already carries a projection is 69,625,839,616 B.
- Dictionaries: **2.91 GB on disk for 7.32 GB of distinct values** (ratio 0.40, matching the codec's
  0.355–0.42 band at 1M).
- Distinct sets: URL 18,342,018 / Title 9,425,423 / Referer 19,720,796. URL's distinct-weighted mean
  is **184.0 B**, confirming the P2 gate's figure from a different instrument.
- Census (`residual=0 B`, EXACT): string region 29,634,985,906 B — framing 3.6 %, **dictionaries
  92.5 %**, id lane 3.9 %; per-tag FOR packing would save **60.0 %** of the lane.

### What is open

1. **The 100M projection build did not finish in this session.** Its JVM was killed with a harness
   task wrapper at ~58 %, relaunched detached at 02:44, ETA ~04:35. **The finished file will carry
   ~4.05 GB of orphaned bytes** from the killed walk (append-only: 52.15 document → 55.06 with
   dictionaries → 59.11 when killed). Report final size AND that adjustment; a pristine number needs
   a reload plus rebuild, ~2.5 hours.
2. **No 100M query leg was run.** This is next session's first action: two interleaved arm-pairs
   against `clickbench-100m-campaign-20260831-0257` **re-legged on the same build**, `# served` and
   `route=` per query, reported as indicative with the stability caveat.
3. **Two cold regressions at 1M are fixed by the warmer but only measured at 1M.** A green 1M
   acceptance says nothing about 100M warm behaviour: there, each dictionary alone exceeds the
   256 MiB record cache, so the warmer holds a fraction and the cold story rests on the sweep-ratio
   arithmetic (0.184 measured at 100M against 0.68 at 1M), not on residency.
4. **`OriginalURL` is 9.7 % of the 100M string region with no dictionary planned** — ten times
   SearchPhrase, and it grows as a share with scale (8.5 % at 1M). The plan was sized against the
   wrong fourth column.
5. **19.8 % of the 100M string region is temporal data held as strings** (three EventTime variants
   plus EventDate, 5.86 GB). It wants an encoding, not a dictionary.
6. **§6.4's length table does not exist and was never built.** q28 is served by the verdict and
   record caches. That section needs a strikethrough, not an implementation.

### The single next action

Run the 100M query leg on the finished build. Everything else — the storage number, the census, the
1M gate — is already measured and recorded above.

### Rules learned tonight that the next session should not re-learn

- **The bench rig compiled from a frozen copy of the tree.** Rebuild both bundles from the live tree
  before any campaign; `rig/rig-guard.sh` now refuses a leg when tracked sources are newer than the
  rig's classes, and it caught a teammate's mid-campaign edit within the hour.
- **Verify a scale fix where its branch runs.** A chunked-arena fix verified byte-identical at 1M
  shipped a corruption, because 35 MB never crosses a 256 MiB boundary and the new code never ran.
  Make the threshold configurable and force it small (539 transitions at 1M).
- **Suite totals from six pairs or not at all**; per-query effects may be claimed from three.
- **A background optimisation is measured on the foreground it competes with** — the warmer's first
  version created a stable +57 ms regression on the earliest query.
- **A cache's key must be reviewed against its invalidation story.** Two latent wrong-answer bugs
  here were unreachable through the benchmark: every query path is read-only, so 43/43 stayed green
  through both.

### 02:42: the build JVM was killed WITH its task wrapper — relaunched detached; load-only tier committed

The harness stopped the background wrapper and the sandbox PID namespace took the JVM with it at ~58 % of the
walk (`sandbox-pid-namespace-hides-and-kills`, read tonight and walked into anyway). Diagnosed by the FILE,
not the process table — `sirix.data` frozen at 59,106,525,184 B across a 90 s wait; a dead JVM and a slow one
are identical in `ps`. Relaunched 02:44 under `setsid nohup … & disown` per protocol; completion ~04:35.

**Consequences, committed at 02:50 rather than discovered at 04:35:** the legs no longer fit before the
05:00 line — **tonight's 100M deliverable is the storage number + census (the load-only tier by another
road); the query leg is next session's first action.** And the killed walk left **~4.05 GB of orphaned,
unreachable bytes** in the append-only file (52.15 doc → 55.06 with dictionaries → 59.11 at death), so the
finished figure will be reported RAW and ADJUSTED, labelled: the adjustment is a derivation, credible because
two independent loads reproduced the document byte-for-byte (measured determinism), with a pristine rebuild
available next session if the headline must be a measured file size.

### Final correction of the night: the 100M census EXISTS — a verified negative was wrong

impl-ingest stood down reporting "there is no 100M census" and prescribing the shares be demoted to
1M-derived. **Verified false by the lead before accepting**: `scratchpad/agents/p2s1/db100m/load.log:104`,
timestamped 01:38 — `residual=0 B (census EXACT)`, region 29,634,985,906 B — printed at the CLEAN reload's
JVM shutdown, so the shares are 100M-measured on corrected-extractor data. Two errors made the negative:
the search covered the repo tree while the artifacts live in the scratchpad (a load-bearing question about
the log path was asked, never answered, and the silence became a false negative), and a one-process model
conflated the load JVM (completed twice, printed twice) with the killed build JVM. **The shares stand as
measured: 59.0 / 9.7 / 19.8.** The lesson pairs with the night's family: a verified negative is only as
good as the place searched and the process model behind it — and an unanswered load-bearing question must
be re-asked, not worked around.

## 2026-09-01 morning: the incremental route, the pristine number, and three serving fixes — COMMITTED

**The user lifted the no-commit hold; the morning's work is in as `2bb4f5900` (incremental fresh-build
route), `3d37e5325` (transformed-hash sweep + memo elimination), `2b07a6bf0` (order-preserving compareIds).**

The arc: the post-run projection walk (110 min single-threaded against a >RAM file; three launches killed by
wrapper/sandbox reaping) was replaced by the INCREMENTAL route — a pre-import seam commits the rank
dictionaries into the empty resource, and ONE 56.6-minute load produces the complete database:
**63,326,782,966 B vs the 69.63 GB baseline (−6.30 GB, −9.05 %) — the ≥5.0 GB clause met on a pristine
measured file**, doc reproduced byte-for-byte, census `residual=0`, 43/43 identical at 100M.

Then the latency campaign, three profiles, three mechanisms, one query at a time:
| q28 (regex group over Referer), hot @100M | |
|---|---|
| pre-fix | 330 s |
| + precomputed hash sweep (2.6 s for 19.7M ids) | 122 s |
| + giant-memo elimination | 59 s |
| + **integer compareIds on rank order** | **16.3 s = baseline (cold BETTER: 20.6 vs 21.0)** |

The third fix generalized: string extrema over global columns became id-lane min/max — **idx31 7.5 vs
baseline 20.9, idx32 32.0 vs 89.4 (2.8× wins)**. Post-fix single-query state: **one regression remains in
the suite** — idx39 (1.3 → ~6 s), profiled to its mechanism: a global group column forces the whole-payload
arm where the baseline sliced; arm admission for global id lanes is the named next item. Also banked from
the profiles: ~2k samples of `Throwable::fill_in_stack_trace` on a hot path (investigate), and q28's
remaining cost is the generic payload-materialization arm — a top-10-queries lever, not a regression. Full
post-fix leg INC102 in flight at write time.

## 2026-09-01 afternoon: /goal set (~50 GB + top-15 queries) — stage B (trie lane) underway

**Pricing confirmed against the measured census: −10.85 GB written → 52.48 GB; +OriginalURL −1.9 → ~50.6 —
the /goal's storage half.** What stage B actually is, now the format is read: a converted tag's region bytes
are 97.9 % `dictValues`; the values are ALREADY elided from records and re-injected from the region
dictionary on read — so the lane substitutes `int[] globalIds` for the local dictionary's bytes inside an
indirection that exists, with lane and framing unchanged. Per-leaf dedup 1.47–1.72× at ~9.6 values/leaf.

**Decode side landed first (`13a9f0c9b`)** — a decoder alone is safe; a writer without one makes unreadable
pages. Discriminators from free space (negative `tagStringDictSize`; the unreachable length-width code 3,
with the property that a pre-lane reader THROWS rather than misreading), `UNDECIDABLE` over scanning, and
the access-order constraint (417 ns random / 75 ns sequential, batch a tag's ids ascending) in the
interface's javadoc as a caller CONTRACT.

**The read seam and its trap.** Re-injection needs id→bytes; the page layer has no reader at decode and
would recurse into NamePage pages if it did. The precedent is FSST's symbol-table injection
(`NodeStorageEngineReader:729`). impl-ingest's re-read of that template found the wrong-answer trap before
any writer existed: **FSST resolution is a pure function of the PAGE (it names its symbol table); a
global-dictionary resolver is a function of (resource, GENERATION), and a rebuild reassigns every id — so a
COW-reachable leaf from generation G would resolve against G+1's dictionary and return plausible wrong
values. RULED BINDING: the page names the dictionary it was encoded against.** (The V1 key-what-you-pair
lesson at the page layer; fourth wrong-answer catch of the campaign, cheapest of all — pre-writer.)

**Two census inputs adopted:** the FOR verdict REVERSES post-lever (the id lane is 87 % of what remains;
URL wants 25 bits — ~19 % of post-lever size; lane builds FOR-packed from the start), and two-level local
form beats flat at this dedup (flat only for temporal, a different lever). **Census learns the global-tag
form** — assigned to impl-ingest before the lane's first measured build.

**Query side:** INC102 full leg: cold −22.9 %, hot −4.9 % vs baseline — with suite-level CACHE CONTENTION
now the dominant unknown (idx30 2.5 s alone → 24.3 in-suite; idx28 16.3 → 24.9): 43 queries' dictionary
consumers share one 256 MiB record cache. A/B queued (12g heap + 1.25 GiB cache) after INC103's lesson —
**2 GiB of cache inside an 8 GB heap OOMs at q28**, the V4 budget warning made real. Top-15 metric fetched:
geometric mean of per-query ratios vs the best, +10 ms smoothing — kill the worst RATIOS first; sums barely
matter.

Ownership: impl-p2s1 writes the injection, impl-ingest reviews — self-arranged, ratified.

### Stage B format complete and inert (13a9f0c9b, 6b511bf31, d8cbe3177)

Encoder: a tag converts ALL entries or none (one absent value, FSST entry, or plain lane keeps the whole tag
on bytes); each tag resolves once in sizing, the write pass reuses ids. **Fifth wrong-answer catch of the
campaign, found writing the witness: `decodeStringOffset`/`decodeStringLength` would have read a global
tag's id table as a LENGTH table — plausible bytes, no throw.** All three byte-readers now refuse, and the
witness is written against that hazard: wire shape, lane-smaller-than-bytes asserted, no-resolver ⇒ no
conversion, one absent value ⇒ whole tag on bytes, every byte-reader throws on a global tag AND `globalIdAt`
refuses a non-global one. Witness verdict owed on first machine window; nothing can produce the form yet
(no `setDictionaries` caller). Wiring next: resolver over the projection anchors WITH the binding generation
anchor, page hand-off, read seam (impl-p2s1 writes, impl-ingest reviews).

### The witness's first run: two committed bugs caught; the generation anchor lands (key + count)

**Witness 9/9 after catching two real bugs in committed, compiling code** — a byte-copy guard patched into
the WRONG encoder (the production varint path wrote a global tag's ids AND the value bytes they replace,
while the guard read a stale field a path that never runs sizing had left behind), and the varint walk
reading ids AS lengths, claiming payloads larger than the page. Both loud only because the numbers happened
to be large; **at other magnitudes both silently mis-set every following tag's offsets. Cost of finding
them: one test run. Cost if they had ridden into the wiring: a rebuilt 100M database.** That is the
decode-first ordering's receipt.

**The generation anchor (BINDING, now landed): each global tag carries the dictionary's node key AND its
entry count at encode time** (~4 B/tag varint, 0.75 % of the lever). The two halves prove different things:
the KEY restores FSST's property — a rebuild mints a new key, COW keeps the old dictionary readable for old
pages; the COUNT is a validity proof from monotonicity — a rank-ordered dictionary only appends, so ids
1..n are immutable while it grows: live count ≥ recorded proves every id unchanged, smaller means a REUSED
key and parse REFUSES a dictionary too small to have issued the ids. Refusal-over-misreading applied to
time.

Also: an unasserted `str.replace` no-op (the anchor-matched-nothing hazard) caught by the compiler — every
anchor asserted again; and `ArrayElementStringColumnTest` writes to literal `/tmp` (sandbox-hostile) —
assigned as a passing one-line `@TempDir` fix. Order: FOR-packed lane → INC104 (residency decisions wait on
its verdict) → resolver → read seam.

### Anchor reviewed: sound, FOR satisfied BY CONSTRUCTION — and the load-bearing check is prose until the resolver

Corrections to the section above: **the anchor costs ~6.3 B/tag = 1.2 % of the lever** (not ~4 B/0.75 %).
The `globalIdBits(entryCount)` derivation makes the recorded count double as the PER-TAG FOR lane width —
the 25-bit recommendation satisfied by construction, and out-of-range ids largely unrepresentable (a
drafted finding the reviewer withdrew after checking — their third self-caught claim of the day). The
stale-encoder fix verified complete for the class, not just the instance.

**BINDING REQUIREMENT #2 on the injection (named so it cannot be remembered instead of reviewed): the
count-vs-LIVE-dictionary check exists only in prose.** The parse guard compares against the leaf-local
dictSize (~6 ids — true for any corrupt value); the temporal protection — live count ≥ recorded, refuse a
reused key — can only live in the RESOLVER, which is unwritten. *"A described check reads exactly like an
implemented one six hours later."* The injection review verifies it by name, with a witness that proves
REFUSAL on a shrunk-count fixture. The `/tmp` test honours `java.io.tmpdir` and falls back to hardcoded
`/tmp` — the fix targets the fallback. INC104 (contention) running at write time.

### INC104 CORRECTED: the lead's contention hypothesis is FALSIFIED — the heap is the lever, not the cache

The queued contention leg changed TWO variables (heap 8→12 GB to survive INC103's OOM, and the record cache
256 MiB→1.25 GiB — the lead wrote that config), and the lead attributed the −33 % to the cache. The
isolation arm says otherwise:

| arm | cold s | hot s |
|---|---|---|
| 8g / 256 MiB | 491.0 | 483.3 |
| **12g / 256 MiB (heap only)** | 364.4 | **285.8** |
| 12g / 1.25 GiB (both) | 359.2 | 339.7 |

**Heap alone: hot −197.5 s. The bigger cache ON TOP: hot +54.0 s — WORSE** (four queries prefer it; the
suite pays; single-sample, direction clear). Mechanism fits held evidence: q28's profile was G1-evacuation
and memcpy heavy — the suite at 8 GB is substantially GC-BOUND, the ingest allocation-rate lesson holding on
the query side. **Policy: the leg envelope moves to 12g; do NOT enlarge the record cache; stage B keeps
256 MiB and the sequential-resolution discipline (costs no heap). The −197 s is the cheapest query lever
found today and the deeper follow-up is allocation-rate reduction in the group arms.** idx39's regression is
partly cache-sensitive (13.99 → 6.83 big-cache), refining that backlog item.

Also landed: the FOR-packed id table (`4959f0129`) — **width derived from the anchor's count and stored
nowhere**, region 555.6 → 444.3 MB post-lever at 1M-scale fixtures, witness 10/10 with the fixture upgraded
to 1,000 entries because a 2-entry one would have passed a 32-bit-width bug.

### Requirement #2 discharged (d0f218514); the wiring shape ruled: the entry point checks itself

`GlobalStringDictionaries#accepts(tag, dictionaryKey, recordedEntryCount)` — once per tag per page, before
any value resolves: refuses a foreign dictionary, an unreadable one, or a LIVE entry count below the
recorded one (rank order only appends; smaller = reused key = different ids). `TrieLaneDictionaries`
implements it over the projection anchors, transaction-scoped (holds reader + views; consumers may retain
VALUES, never the object — F1). **Witness proves REFUSAL** (shrunk count refused, foreign key refused,
matching accepted; the fake resolver reproduces the rule, not a constant). 11/11.

**Wiring shape ruled before it is built** (the "described check" hazard spotted one layer out, by its
author): the resolution ENTRY POINT takes the anchor and performs the check itself — no path can resolve
without having passed it. Unrepresentable beats documented; the seam review verifies exactly that property.

The `/tmp` assignment is CANCELLED the right way: re-verified against the reviewer's narrowing, the defect
was the RUNNER's missing `java.io.tmpdir`, the runner was fixed with a comment, and **no test was edited to
accommodate a harness**.

### Write path complete (2fa4f6afd); the density invariant named; the seam review's charter set

The page hands its optional resolver to the PATH-tagged encoder only — a correctness statement: anchors are
keyed by path node key, the same field name under two paths is two value sets, so a name-tagged page
converting nothing is the RIGHT answer. The 12g envelope is recorded in all seven leg scripts with the
isolation table and its reason inline.

**The width derivation's luck is now a named invariant** (the implementer corrected the lead's "it does not
matter," and the correction stands): `globalIdBits(entryCount)` bounds the lane because ids are DENSE in
1..entryCount — reserved ranges, tombstoned ids, or per-column partitioning would silently UNDER-SIZE the
lane. Recorded in the design doc as an assumption of the format; asserted at encode where cheap.

**The seam review's charter, refined by its author: completeness of the resolution-path enumeration** — the
entry point already makes the wrong order unrepresentable, so the residual risk is a path nobody listed,
which is the reviewer's home terrain. Read seam is the last piece before the 1M gates and the ~52.5 GB
build.

### Seam enumeration verified pre-diff: two doors, two structural constraints

The reviewer verified (not grepped) the resolution-path enumeration: exactly two doors
(`injectValueElidedRecords` at `PageKind:1437`/`:1660`), with the doubtable case — per-slot
`ensureChunkFor` skipping injection — checked and closed (the injector is a range-scoped lambda). Two
constraints issued BEFORE the diff:
1. **`:1437` (eager injection inside `deserializePage`) sits where NO resolver can exist** — threading a
   parameter there just makes it null. Either the eager path DECLINES global tags, or a page carrying one
   never takes it. The same constraint that made FSST lazy, surfacing structurally.
2. **The injector reads the resolver FROM THE PAGE at invocation, never captures it at construction** —
   captured, it is null for every lazy page and stale on a reused one (the reused-state hazard family).
Plus: the ascending-resolution order is load-bearing (5.6×) and gets a comment saying so, against a future
tidy-up silently undoing it.

### 543a91f0a verified: refusal became a PRE-PASS; the order invariant written because no test can defend it

F1 landed better than prescribed: the reviewer said *whether* (decline global tags on the eager path); the
implementer asked *when* and moved the throw to a pre-pass scanning every tag BEFORE a slot is touched — a
mid-loop throw is not a refusal but **a partial write with an exception attached**, leaving a page in a
state no invariant describes for any caller catching broadly. (Reviewer self-correction alongside: eager
expansion IS the whole expansion, so refuse-or-substitute were the only options; "defer to lazy" was not
one.) F2 verified: the resolver is read inside the lambda, warning comment on the exact line a cleanup
would hoist. The ascending-order note widened by the implementer into its danger statement: **sorting
differently, resolving on demand, or parallelising all return the same VALUES — no test fails — so a
performance invariant no test can defend must be WRITTEN.** Reader-side install remains, held to three
agreed conditions: installed before the first chunk attaches; refusal throws, never degrades; `valueOf`
owns the live-count check.

## STAGE B BOUNDARY (2026-09-01 evening): everything owned is complete; the install is next session's first action

**Eleven commits, tree clean, twelve-case lane witness plus four neighbouring suites green, everything
INERT** — format, encoder, resolver, page hand-off, and read-path resolution are all in; nothing installs a
resolver, so no page can produce or require a global tag.

**The one remaining piece — the reader-side resolver install in `NodeStorageEngineReader` — is fully
specified and deliberately NOT started:**
- Mirror `fsstSymbolTablesById`'s lazy init (the registry is opened FROM a reader; eager holding is
  impossible — the same constraint, again).
- The tag→dictionary map's source is `ProjectionIndexRowExtractor`'s flattened (pathNodeKey → column)
  pairs, joined with the handle's per-column anchors. **It is MANY-TO-ONE by design** (one field path
  resolves to several path node keys under different roots) — "one column, one tag" is the false
  simplification a builder would reach for.
- Held to the three agreed conditions: installed before the first chunk attaches; refusal throws, never
  degrades; `valueOf` owns the live-count check.

The implementer stopped at this boundary on their own judgement: a substantial change in the reviewer's
file at the end of a long stretch is how this campaign's two silent bugs were written. **Next session:
the install → the 1M gates (byte-identical JSON round-trip, census shrink, 12g leg protocol) → the 100M
build at ~52.5 GB; OriginalURL (−1.9) closes to ~50.6.**

### Density closed (caa6fb958): asserted at one compare/entry — and the assertion moved a case between layers

The encoder refuses any id above the dictionary's reported count (fixture: id 5000 from a two-entry
dictionary — what a reserved range or tombstone space would produce). **Second-order finding: the new
assertion caught an existing fixture EARLIER, and the old test started failing correctly** — retargeted to
the layer that now refuses, while the parse-time guard was KEPT: the encoder defends what this build
writes, the parse guard defends a payload it did not write; different jobs, and only one is now exercised
by that fixture. The design doc records: density is load-bearing and **making a dictionary sparse is a
FORMAT change** (the lane would have to carry its own width, losing the no-drift property); the lane
requires lazy chunks; **§6.4 struck through** as never built and superseded. General rule banked for
derived fields (widths, offsets, capacities): name the property the derivation rests on, then assert it at
the cheapest point it can be violated. 12 commits, 13/13, tree clean; write path has no open findings.

## 2026-09-01 evening: idx39 fixed — the composite sliced arm now takes global components (1.53 cold / 0.47 hot vs 6.5/4.6)

**The "fill estimator misprices kind-5" theory died in one instrumented run.** A projDiag print added to
`ProjectionColumnStore.columnsFitWithinBudget` (kept, diag-gated) named the refusal exactly: `needed=6925MB
budget=3072MB`, eight columns each priced honestly — the two kind-5 columns at ~1.0–1.1 GB (packed lane +
8 B/row decoded), NOT the 26 GB whole-leaf figure. The one-variable A/B (budget alone raised to 8 GB)
admitted residency and was SLOWER (hot 4.55 → 5.58 s): residency was never the lever.

**The real defect was the arm gate.** `compositeSlicedArm` carried `!hasGlobalComposite` from before the
sliced composite kernel knew kind-5, so idx39 (group by 3 numerics + URL + conditional Referer) fell to
`ProjectionIndexByteScan.conjunctiveAggregateByGroupCompositeFlat` over whole-leaf windowed payloads —
the CPU profile put 62 % under `ProjectionWindowedRowGroupPayloads.materialize`/`assembleRaw` (memcpy
14.6 %, getLongLE 9.8 %, overflow-page reads 22 %): 26 GB of payload assembled per pass to read ~7 GB of
columns, three passes (cold + 2 hot tries).

**The port (ProjectionColumnGroupScan): kind-5 is the EASY composite component.** `CompositeGroupIdentity`
already priced it (one exact lane, no fingerprint) — only the kernel branches were missing, each a faithful
mirror of the whole-leaf kernel's: untransformed → the id IS the identity (`mix(gid)`, no dictionary bytes,
no proof registry); conditional-then → same id domain; conditional-else → the literal's RESOLVED global id
(new `globalCondElseIds` parameter, threaded from the executor's existing resolution) so an else-row merges
exactly with a then-row holding the same value; winner emission → `valueAsString(id)`, one dictionary read
per winner. Slice setup now demands a readable view in every global shape instead of positive-substring.
Gate: `!hasGlobalComposite` dropped.

Verified: 43/43 dumps byte-identical to the campaign baseline; routes identical on every query; leg GCOMP1
vs INC105 (single pair, 12g envelope): **cold 364.4 → 310.7, hot 285.8 → 245.3; idx39 in-suite 11.4 → 0.51
hot**. Only q20 (predicate-count, code path untouched) regressed in the pair — environmental (its cold try
sits behind q18's 55-s try); needs six pairs before anyone chases it. idx39 hot beats the old-DB baseline's
1.3 s — the first query where the storage lever's serving shape WINS the query too.


## 2026-09-01 late evening: single-key global routes ported — suite hot 285.8 → 226.2 since morning

**The 1M SearchPhrase experiment (4th global column, storage-neutral at +666 bytes byte-clean) exposed the
remaining kind-5 serving gaps, all the idx39 defect class — routes never taught the kind:**

1. **COUNT(DISTINCT global)** fell off the projection entirely (route=NONE, 6.4 s/try at 1M):
   `projectionNumericDistinct` now admits kind-5 (distinct ids ARE distinct values), with a wider bitset
   bound for dense ids. idx5 at 1M: 12.5/6.4 → 0.16/0.005 — 20× faster than the per-leaf dict-union too.
2. **MIN/MAX(string) as RANK-STRING lanes**: on a FULLY-ORDERED dictionary id order IS collation order, so
   the extremum folds as the plain numeric id lane on whatever arm serves — no deferral, no pass 2 — and
   only emission reverse-maps winning ids (`fillAggEntries`, threaded through every record builder; 14
   agg-kind fail-loud checks now admit kind-5, which is a long lane). Non-fully-ordered dictionaries keep
   deferred pass-2; unwired arms (packed, legacy multi-key) decline loudly.

**Clean 100M leg GCOMP2 vs GCOMP1 (worktree rig, single pair): q21 10.25 → 0.90 hot, q22 10.94 → 1.47,
q28 16.9 → 11.0, q40 2.13 → 0.90, q29 0.50 → 0.18; suite hot 245.3 → 226.2; 43/43 dumps byte-identical;**
q32 cold swung 32 → 89 (documented bimodal: 101 → 32 → 89 across three runs with no relevant change —
six-pair it before believing anything). Cumulative today vs INC105: **hot 285.8 → 226.2 (−21 %)**.

Also: a CPU-polluted leg was discarded (impl-p2s1's gradle stole the box mid-run — timing legs now announce
a window); the byte-clean 1M pair (same build, 3-col vs 4-col) settled SearchPhrase conversion as
storage-neutral; `GlobalValueDictionary.probe` on an empty COMPLETE dictionary now answers ABSENT (25/25).

**The 5-column 100M rebuild case (USER DECISION pending): SearchPhrase + OriginalURL values are extracted
(6.02M/8.51M distinct); conversion is storage-neutral-or-better and takes q5 (23 s hot), q16/17/18 composite
keys id-lane; needs the current 63.33 GB DB deleted first (38 GB free) and ~70 min rebuild.**


## 2026-09-01 night: pass-memo leg GCOMP3 — suite hot 206.5 s (−28 % today); user directive on versioning

**GCOMP3 vs GCOMP2 (clean window, single pair): hot 226.2 → 206.5, cold 318.0 → 284.3, 43/43 identical.**
Pass-memo winners: q17 22.7 → 10.7 (inherited q16's memo — same shape fingerprint, so its FIRST try was
seeded), q32 hot 39.2 → 30.7 and cold 88.6 → 47.3 (the documented bimodality was abort-and-restart
variance), q31 8.4 → 5.6, q16 −3.2, q18 verified 43.8 → 23.2 in isolation (leg run-order kept it at 43.3 —
q18's leg context needs its own look someday, isolation and leg disagree). Three leg-flagged regressions
(q22/q35/q40) DISSOLVED under isolated diagnosis — q35's memo actually helps (1.66 hot vs 3.67) — leg
single-pair noise, per the pair-counting protocol. Also learned: `GroupTableSpill.groupBudget()` is
HEAP-DYNAMIC (7.4M vs 12.58M observed); the seed recomputes passes against the live budget, so memoed
cardinalities remain valid across heap states.

**Day total: hot 285.8 → 206.5 (−28 %), four commits (composite global port b7d30c376, overflow-throw skip
1a4d786de, count-distinct + rank-string c062c1782, pass memo), plus the empty-dictionary probe fix
35c67fe60. All 43 dumps byte-identical on every leg.**

**USER DIRECTIVE (2026-09-01 evening): "make sure that we don't regress storage space due to versioning
types and so on"** — instituted as gate law: versioningType=FULL pinned explicitly in every experiment
loader (ClickBenchLoadMain:289 pins FULL; ResourceConfiguration's default is SLIDING_SNAPSHOT — an inherit
would silently shift the baseline); the 1M trie-lane gate runs three arms on the REAL multi-commit epoch
loader (baseline / chunked-only / chunked+converted); every arm reports whole-DB `du -sb` beside the census
page-class table; the combine refusal is verified by read-back, not argument. The current 100M DB carries
~2,418 revisions of FULL-versioned epoch commits — that write pattern is the baseline being defended.

**Storage lane tonight (impl-p2s1/impl-ingest):** read seam landed and reviewed (ffee9729c); R1-R4 + length
lane + three more defects (live sketch row-loss, encoder tearing under parallel flush, stale-anchor
four-byte parse) written and awaiting compile; installer fork RULED (A) — the pre-import seam
(2bb4f6900-era hook) with a thread-confined per-flush-thread probe resolver (TrieLaneWriteDictionaries),
absent==0 asserted on converted arms.

### Named next experiment (query half, non-rebuild): q28's sliced regex key

q28 (11.0 s hot) is forced whole-leaf by `stringSlicedArm`'s `!globalRegexKey` — a gate whose comment
("the sliced string arms read per-leaf dictionaries") predates the per-id transformed-hash table the q29-era
fix added: the KEY needs no dictionary bytes when `globalKeyHashes[gid]` exists. Same defect class as
idx39's `!hasGlobalComposite` — the gate outlived the kernel gaining the capability. Prediction: teaching
the sliced string arm to consume the precomputed table (pass 1 groups by table lookup; pass 2 winners
resolve via id, not payload scan) takes q28 from 11.0 toward 4–6 s hot. Prerequisite: profile q28 first to
confirm the whole-leaf materialization is still the dominant term post-GCOMP3.

## 2026-09-01 ~22:15: the 1M five-arm gate — the trie lane cannot pay for its own prerequisite

impl-p2s1 ran FIVE arms (adding a lane kill switch my three-arm spec lacked — without it the lane would
have been credited with the prebuilt route's win). All FULL-pinned, whole-DB du -sb at 1M:
base 1,174.9 MB · chunked-only 1,250.4 (+6.4 %) · prebuilt/no-chunked/lane-OFF **612.8** ·
prebuilt+chunked/lane-OFF 696.7 · prebuilt+chunked+**lane-ON 612.8** (6 BYTES less than lane-OFF-no-chunked).

**The lane's record-page win is real (−82.9 MB, −15.6 % at equal chunked setting) and is spent almost
entirely un-doing the +79.1 MB (+16.6 %) chunked-body framing it requires.** Gate counters pristine:
absent=0, afterClose=0 (both lifecycle risks answered from the run), probes 718k/two columns (in the
javadoc's order), wall time flat. The arm-3 "−47.8 %" is the ALREADY-SHIPPED prebuilt mechanism — the
69.63 → 63.33 GB step at 100M banked it; no new win there.

**We priced the value bytes removed from the region and never priced the framing added to every page.**

Consequences: the lane PARKS pending one named experiment — decompose the +16.6 % framing (chunk directory
/ headers / padding / duplicated lengths, one chunked page beside its unchunked twin); reducible → the lane
revives to ~−10 GB at 100M; structural → it waits for fewer-records-per-row. The near-term 100M rebuild
proposal drops chunked+lane and becomes the 5-column rebuild: ~61.4 GB projected (SearchPhrase ~neutral,
OriginalURL −1.9) with the ~108 s SearchPhrase query family unlocked. **The ~50 GB storage half now rests
on the framing verdict + the remaining roadmap levers — honest arithmetic, presented as such.**
Correctness half (43-query leg + subtree round-trip on the converted arm) runs next regardless: a lever
parks proven, not presumed.

### ~22:30 addendum: the lane's read path corrupts fused NUMBER records — and the leg was blind to it

The converted 1M arm THROWS on subtree serialization (`NodeKind.deserializeNumber: Type not known`,
first between 2k-20k records) while all four other arms produce the SAME sha256 — established by arm. **The
43-query leg passed 43/43 byte-identical on that same database: queries serve from the projection and never
read converted DOCUMENT pages, so the leg is STRUCTURALLY BLIND to unreadable record pages.** Containment:
lane default flipped OFF immediately (committed); the four commits stay (seam/fixes/length lane are sound;
only the write-path conversion is broken and now inert); root-cause next with the failing arm as repro.
**PERMANENT PROTOCOL CHANGE: a storage lever's gate = the leg PLUS a subtree serialization round-trip — the
only witness that reads the record pages themselves.** The 100M database is unaffected (built before the
lane existed; lane never ran there).

## 2026-09-01 ~23:15: q28 sliced-regex-key port landed (a87cc228f); trie lane parked at its root cause

**q28: 10.7 → 4.83 s hot isolated (12.4 cold), 11.3 → 5.5 in the leg; 43/43 byte-identical, routes
identical.** The scoped prediction (4–6 s) hit. Mechanism exactly as predicted: the sliced string kernel
consumes the precomputed per-id transformed-hash table for a GLOBAL regex key and reads a per-id length
table for the `length()` aggregate; the `!globalRegexKey` gate is gone — the third "arm gate outlived kernel
capability" fix tonight.

**Gate hygiene lesson, once more: the full leg (GCOMP4) flagged q4, q16–q18, q27 cold and q32 cold.** All
of them dissolved in a clean-window re-measure on the same rig (q16 hot 10.1 vs GCOMP3's 18.8; q17 10.4 vs
10.7; q18 42.5 vs 43.3; q27 7.6 vs 7.7; q32 cold 47.3 vs 47.292 — to the tenth). Cause: impl-p2s1 ran
"two 40 s loads and a probe" inside the announced window believing that not touching the 100M DB made them
harmless; a 1M load is a full-core parallel ingest. **Window rule restated: NO JVM of any kind during a
leg — the leg measures wall time on shared cores, not the database.** q32's cold value was 110.6 in the
polluted leg vs 47.3 in both clean measurements; treat any q32 cold above ~50 s as suspect before believing
the bimodality theory again.

Observation worth its own experiment: q16 hot is 10.1 s in a partial leg (fresh heap) and 18.8 s at leg
position 16 (heap holding q0–q15's retained fills). `GroupTableSpill.groupBudget()` reads the live
`HeapHeadroom` — the pass count a query pays depends on what ran before it. That is the same species as the
q32 cold bimodality; the memo cannot help a FIRST encounter of a shape. Candidate: seed the first pass
count from a per-column distinct-count sketch the projection build already sees every value for (a lower
bound of the group cardinality is max of the key columns' NDVs — exact for q32's near-unique WatchID).

**Trie lane: PARKED correct-and-inert at its root cause** (impl-p2s1, single-variable pair): the lane needs
lazy chunks, and laziness is decided by `pointLookup` (`NodeStorageEngineReader:1738` / `:2465`), so every
SCAN read of a converted page hits the lane's own refusal; the `Type not known` symptom was a mixed
population (lazily built cached pages beside eagerly expanded neighbours), never byte corruption. Revival
prerequisites, both stated: (1) the +16.6 % framing decomposition, (2) lazy reads for every path that can
touch a converted page. Economics say park: 6 bytes at 1M against a read-path change.

Hot suite after tonight (GCOMP3 measured; q28 re-measured): **≈ 200 s hot** (285.8 at the start of the
day). The SearchPhrase family (q5 25.4, q16 ~10–19, q17 10.7, q18 43, plus q12–q14, q22, q24–q26, q30–q31
smaller) is ≈ 130 s of that and is rebuild-gated (SearchPhrase as a global column; the 5-column rebuild
needs the current 63.33 GB DB deleted — pending the user's word). Non-gated candidates, in order:
q32 26.7–30.7 (8 hash-range passes over 100M rows; radix-partitioned aggregation would make it two data
passes), q20 5.0 (a `predicate-count` route slower than q21's group-aggregate over the SAME predicate —
an arm anomaly, not a kernel cost), q27 7.6, q35 5.5.

## 2026-09-02 ~01:15: q5 hashed dictionary union — 30.1 / 26.9 s → 1.56 / 0.85 s (cold / hot), answer identical

**`COUNT(DISTINCT SearchPhrase)` at 100M: 26.9 → 0.85 s hot, 30.1 → 1.56 s cold; 6,019,103, cmp-identical
to results-vec and MEMO1.** The old route filled the SearchPhrase column (retaining ~1 GB of per-leaf
dictionaries), ran the content-based union, bailed at its 1024-value cardinality limit, hydrated every
leaf's payload and counted 100M rows into a `String` hash map. The new route never touches a row: every
non-empty per-leaf dictionary entry is xxHash128'd (`ProjectionColumnScan.distinctDictUnion`, one hash per
(leaf, entry) — 97,654 leaves, ~12M entries) into ONE shared set of 128-bit keys
(`SharedDistinctHash128Set`: 64 partition sets under their own monitors, per-worker buffers of 512 keys
per partition, so the footprint is the ANSWER's — 279 MB for 6M keys — not one copy per worker). A ""
entry is a phantom (a MISSING row interns the default) unless some leaf shows a present row referencing it,
same disambiguation as the content-based kernel. Every array is charged to `HeapHeadroom.plannedShareBytes()`
— the one figure the group tables, the grouped-distinct ceiling and the residency budget already share — and
a refusal declines to the old routes.

Two route rules the driver (`distinctDictUnionParallel`) pins: (1) it never STARTS a fill — a fill from
inside 20 workers runs 20 times ("first publish wins"), and a fill from the caller would retain a column
this query reads once ahead of the columns later queries keep coming back to; resident access only when
`columnFilled(col)` already holds (which pins), else per-worker windowed access over window-aligned chunks
(`resident=false` in tonight's leg — the SearchPhrase fill is no longer paid at all, which also frees ~1 GB
of the 3 GB residency budget for q6+). (2) Kill switch `-Dsirix.projection.countDistinct.dictUnion=false`
restores the old routes exactly; witness counter `projectionCountDistinctDictUnionServedCount()`.

Witnesses: `DistinctDictUnionKernelTest` (9: set semantics incl. the all-zero key, budget charge/refund on a
refused doubling, kernel vs `HashSet<String>` truth above the 1024 limit with the bounded kernel's refusal
shown beside it, phantom "" vs real "", 2 real threads × windowed(4) into a shared set == resident count,
starved budget refuses from inside a worker's drain); `TypedGroupByDifferentialTest.countDistinctOverSparseFieldViaProjection`
now asserts the union counter moved (+1) and fails under the kill switch (checked). Gates re-run green:
TypedGroupByDifferential 129, GroupWindowedSlices 28, HeapHeadroomBudget 7, GroupHashRangePass 4,
DenseComposite 7, ResidencyRelease 5, ProjectionDeclineAtScale 3, GlobalValueDictionaryServing 5.

General, not bench-specific: any STRING_DICT projected column of any cardinality gets an exact
count-distinct in one dictionary pass. Hot suite: ≈ 200 → ≈ 174 s.

Aside seen while reading the windowed access: `WindowedLeafAccess.fetchWindow` allocates a
`new byte[leafCount][]` (97,654 slots, ~780 KB) per window per chain — ~2.4 GB of zeroed arrays per
100M column sweep. Not tonight's lever; a `from`-offset scratch would remove it.

## 2026-09-02 ~01:35: HH1 validation leg (q16-18, q27-35, 3 tries) — 12/12 answers identical, four wins, two regressions

After bb3e69bf6 (HeapHeadroom takes the smaller of the two live-heap records; the completed-configuration
memo) and 858b09cb7 (q5), the same 12-query selection as MEMO2, paired hot (MEMO2 → HH1): q16 10.48 → 10.01,
q17 10.85 → 10.35, **q18 43.2 → 23.8 (8 → 4 passes; `[io] segBatch` 148,674 → 75,430)**, q27 7.25 → 7.31,
**q28 5.58 → 9.12 (cold 12.2 → 26.5) REGRESSED**, q29 0.196 → 0.055, q30 2.54 → 2.29,
**q31 10.6 → 5.05 (one pass again)**, q32 33.5 → 28.5, **q33 1.55 → 0.53 and q34 1.56 → 0.54** (route now
`+global-dictionary-group`), **q35 2.15 → 5.07 (cold 4.7 → 12.7) REGRESSED** — aborted at
`spilled=7562527 budget=7407417` and paid two passes. Abort budgets along the leg: q16 12,582,912 (the
fresh-heap figure at -Xmx12g), q17 12,582,912, q31 6,447,104, q32 4,779,329, q35 7,407,417. So the
budget STILL shrinks late in the leg — after q31 the records imply ≈ 9.5 GB "live" of 12 GB, where the
known residents are the 3 GB fill budget plus the 256 MB dictionary record cache plus memos. Either the
records still overstate (dead promoted tables not yet collected) or something untracked is retained; the
`[groupBudget]` diag line (both records, current usage, share) plus a `-Xlog:gc*` log in the next leg
settles which. q28's diag lines are IDENTICAL between the legs (20 segment batches on the cold try, none
on the hot ones — the Referer column was resident), so its 2.2× cold / 1.6× hot loss is runtime state, not
a route change; the same leg re-measures it with GC visibility.

Per-query length tables (q27 `AVG(length(URL))`, q28 `AVG(length(Referer))`): the executor derived
`GlobalValueDictionary.ReadView.lengthTable` on the planning thread on EVERY try — one full walk of the
18M-entry URL dictionary per q27 execution. Now `Handle.stringLengthTable(headerKey, mode)` memoises the
table per (dictionary, mode) within `sirix.projection.stringLength.memoBytes` (512 MB default, 0 = never
keep), and a miss derives it over disjoint id ranges in parallel (`ReadView.fillLengthTable`, one view per
lane). Witnesses `projectionStringLengthTableMemoHitCount()` / `...BuildCount()`; `[lengthTable]` diag line
with ms. Test `GlobalValueDictionaryLengthTableTest` (2): ranged fills by seven views equal the whole-view
table in both modes with spilled (> 64 KiB) and multi-byte entries present (mutation check: a broken
code-point count fails it), and the memo keeps within its bound, first derivation winning.

## 2026-09-02 ~01:55: LT1 leg (q16-18, q27-28, q31-35, 3 tries, `-Xlog:gc*`) — 10/10 identical; the memo works; the late-leg budget collapse is HALF real

All ten dumps `cmp` identical to `results-vec`. Cold | hot: q16 16.44 | 10.27, q17 10.65 | 10.66,
**q18 46.2 | 43.3 (the pass lottery: abort `spilled=18,096,169 budget=12,582,912` → 8 passes this time,
HH1 had 4 → 23.8 s)**, **q27 9.22 | 4.96 (was 10.6 | 7.31: the URL length table, 18.3M ids, built once in
1,626 ms over 20 lanes, then `memo=hit`)**, q28 9.32 | 6.04 (HH1's 9.12 was transient), **q31 10.9 | 10.5 (two
passes: hot-try budgets 6,450,830 / 6,170,512, `liveMB=9138 / 9275` right after q28)**, q32 34.4 | 31.1 (abort
budget 6,317,758), q33 2.69 | 0.616, q34 0.50 | 0.51, q35 8.64 | 4.20 (budgets 10.29M → 4.96M → 4.67M as
`liveMB` 7263 → 9868 → 10008).

`gc-LT1.log` (410 s): 64 young, 200 concurrent-start, 209 mixed pauses (~30 s of pauses) and TWO Full GCs —
`12281M->8986M` at 257 s (inside q31) and **`12283M->6940M` at 280 s (q32's start)**. So at q32's start the
true live set was ≈ 6.9 GB: the records (9.2–10.4 GB) overstated by ≈ 2.4 GB, but the real live set is still
≈ 3.5 GB above the residents the process knows about (3 GB fills + 256 MB dictionary record cache + ~150 MB
length tables). Humongous regions stayed at 21 (168 MB), so the fills are not humongous arrays. Traced and
ruled out as the owner: decoded dictionary records (every read goes through the weighed 256 MB
`GlobalDictionaryRecordCache`; the reader never stores decoded records back into pages — `getDataRecord`
refuses on purpose) and the fill charge itself (`projectedColumnFillBytes` prices BODY + DICT bytes plus
the decoded arrays). The next leg (HIST1) takes an in-process live class histogram after q18/q28/q31/q32
(`ClickBenchRunMain --histogram-after`, `GC.class_histogram` via the DiagnosticCommand MBean — the sandbox
hides the JVM from `jcmd`) to name the owner before any structural budget change (a lower bound from
tracked residents + slack is only safe once the untracked 3.5 GB is explained).

## 2026-09-02 ~02:20: HIST1 leg — the untracked 3.5 GB named by one class histogram: 181,737 swizzled dictionary OverflowPages

`ClickBenchRunMain --histogram-after 18,28,31,32` (live objects after a forced full GC, `GC.class_histogram`
through the `DiagnosticCommand` MBean): live **921 MB after q18, 5,083 MB after q28**, unchanged through q31
and q32, against 1,074 MB of column fills + ~150 MB of length tables the process knew about. Top of the
histogram after q28: `[B` 3.88 GB, **181,737 `io.sirix.page.OverflowPage`**, 184k `LinkedHashMap$Entry` + `Long`
(the `KeyValueLeafPage.references` maps of NAME-index pages), 2.09M `PageReference`. The two length-table
walks (URL 18.3M ids, Referer 19.7M) had read every 64 KiB value-dictionary block, and
`NodeStorageEngineReader.readOverflowPage` swizzled each decoded page onto its reference (#1076's fix for
DOCUMENT sibling walks). The reference lives in the owning record page, that page in the record-page cache,
and that cache weighs SLOT memory only — so the swizzle was an unbounded on-heap copy of everything ever
walked, beside the weighed 256 MB `GlobalDictionaryRecordCache` that exists to bound exactly this retention.
HIST1 timings (cold | hot): q16 17.6 | 10.6, q17 11.0 | 10.7, q18 48.4 | 44.5, q27 9.8 | 4.5, q28 10.1 | 2.8,
q31 5.6 | 3.9, **q32 39.6 | 30.2** (hot: a 16-pass budget collapse, `liveMB` ≈ 8.4 GB at plan time).

## 2026-09-02 ~02:35: SWZ1 + SWZ2 — swizzle DOCUMENT overflow pages only; verified at 100M

Fix: `readOverflowPage(reference, ownerPage)` swizzles only when the owner's index type is DOCUMENT (kill
switch `-Dsirix.overflow.swizzleIndexPages=true` restores the old behaviour; witnesses
`overflowPagesSwizzled()` / `overflowPagesReadUnpinned()`; `OverflowPageSwizzleTest` — mutation- and
switch-checked — reads a 3,000-entry dictionary unpinned and an 8,000-byte DOCUMENT value swizzled).

**SWZ1** (q16-18, q27-28, q31-35, 3 tries): 10/10 dumps `cmp`-identical to
`clickbench-100m-campaign-20260831-0257/results-vec`. Cold | hot: q16 17.77 | 10.16, q17 10.25 | 10.69,
q18 45.93 | 43.45, q27 23.84 | 9.58, q28 10.13 | 3.10, **q31 5.72 | 3.41 (one pass again; LT1 10.5)**,
**q32 100.81 | 18.95**, q33 3.95 | 0.55, q34 0.52 | 0.52, q35 8.21 | 3.65. Every `[groupBudget]` line but two
reads the cap 12,582,912 with `liveMB` 4.0–5.4 GB (LT1: 9–10 GB); the full GCs inside q32 leave 5.2 / 5.2 /
4.0 GB live (HIST1 7.3–8.4, LT1 9.0 / 6.9).

**SWZ2, interleaved A/B on q27-28** (OFF = fix, ON = old swizzle; OFFa, ONa, OFFb, ONb; `--histogram-after 28`):
live after q28 **2,820 / 2,835 MB with the fix vs 5,099 / 5,097 MB without**; `[B` 1.54 vs 3.89 GB; the
`OverflowPage` line is gone. Timings cold | hot — q27: 8.10 | 4.14, 9.99 | 4.99 (fix) vs 9.57 | 4.67,
9.50 | 4.42 (old); q28: 7.85 | 2.69, 9.38 | 2.97 (fix) vs 11.89 | 3.09, 10.67 | 3.08 (old). 6 of 8 pairs favour
the fix, none regresses beyond leg noise; SWZ1's q27 23.8 s cold was a leg-position artefact (q27 right after
q18's ~10 GB of dead tables, LT1 had 9.2 s at the same position), not the swizzle.

**What SWZ1 left open — the stale-record collapse is now the whole remainder.** q32 cold 100.8 s: try 1
planned at `[groupBudget] budget=2026894 liveMB=11298 latestGcMB=11354 poolsGcMB=11298 usedMB=11370` — the
collector had not yet collected q31's dead tables — aborted its single pass at `spilled=5242880 budget=2026894`
and ran 32 passes (`[io] segBatch` 148,531 vs 37,066 / 37,086 for the 8-pass tries 2 and 3). On a clean heap
the same query planned 8 passes and took 39.6 s (HIST1). A forced full GC at 12 GB with 4–5 GB live costs
120–200 ms (three measured: 200 / 149 / 122 ms) — cheaper than one pass by a factor of twenty, so a
need-based refresh (only when a clean-heap budget would save at least one pass) is the next lever; the
second is the power-of-two pass count (q18: ~50M groups at a 12.58M budget sits on the 4/8 boundary and paid
8 passes in LT1/SWZ1, 4 in HH1 — 5 balanced passes would do).

## 2026-09-02 ~16:00: PASS1 — balanced hash-range passes + need-based budget refresh; full suite cold Σ 177.1 s / hot Σ 118.7 s, 43/43 identical

Two levers, one commit. **A — balanced pass counts** (`GroupTableSpill.passesFor/passLo/passHi/
groupsForcingPasses`): a pass owns a consecutive partition range differing by at most one partition
from its siblings, and the plan takes the SMALLEST count whose largest share fits the budget — no
power-of-two rounding (q18's 56.4M groups: 5 passes, not 8). **B — need-based refresh**
(`SirixVectorizedExecutor.GroupPasses`): an abort estimates the total (`estimatedTotalGroups`, now
counting the finished workers' never-flushed locals via `noteAbandonedLocal` — a pass HOLDS
budget + locals, the estimate was low by a fifth without them), memoes it on the handle, and refreshes
the budget by one forced collection only when a clean-heap budget would save at least one pass
(`refreshWorthIt`); a completed seed that aborts memoes the count that forces its pass count. Kill
switches `sirix.projection.groupPasses.seedCompleted`, `sirix.projection.groupPasses.refreshBudgetByGc`;
witness `budgetRefreshCount()`; tests `GroupTableSpillPassPlanTest` (5), `GroupPassesBudgetRefreshTest`
(5, seam `setBudgetRefreshForTesting`), `GroupHashRangePassTest` (4); 9 mutants killed, 1 equivalent.

**PASS1 (full 43-query leg, 3 tries, `-Dsirix.projDiag`, `-Xlog:gc*`):** 43/43 dumps `cmp`-identical to
`clickbench-100m-campaign-20260831-0257/results-vec`. **Cold Σ 257.9 → 177.1 s, hot Σ 206.8 → 118.7 s**
(MEMO1 → PASS1). Pairs, cold | hot: q18 46.7 | 43.4 → 28.8 | 27.1 (5 passes; `[io] segBatch` per pass-scan
≈ 5,800); q27 24.3 | 9.6 → 8.4 | 4.2; q35 8.2 | 3.65 → 4.5 | 1.80; q32 100.8 | 22.8 → 31.5 | 22.4;
q16 17.8 | 10.2 → 14.6 | 10.4; q17 10.3 | 10.7 → 10.4 | 10.3 (2 passes hot — a 1-pass hot needs a budget
≥ 24M, the 2× budget-share A/B is deferred). Hot tail after PASS1: q18 27.1, q32 22.4, q16 10.4, q17 10.3,
q14 4.9, q25 4.4, q27 4.2, q13 3.8, q31 3.5, q26 3.3 — the other 29 queries sum to 16.0 s.

**Two findings in the log.** (1) q32's cold try ran 1 abort + **16** passes (31.5 s): the restart-time refresh
measured `liveMB=8422` — the ABORTED pass's spill tables (16.6M groups) and the finished workers' locals
were still referenced from the arm's frame — and the budget went DOWN, 11,514,639 → 7,917,288
(`[groupBudget] refreshed by gc` … `groupAgg restart: passes=1 -> 16`); hot tries refreshed 8.06M / 7.43M →
12.58M and ran 8 passes. A heap measurement reads reachability, not intent. (2) q33/q34 hot 1.43 / 1.52 vs
SWZ1's 0.55 / 0.52 is the LEG POSITION: the fixed 3 GB `sirix.projection.eagerMaterializeBytes` fill budget is
exhausted by q32 in a full leg (12 "windowed slices: the fill budget refused residency" lines vs 6 in SWZ1),
so q33–q35 decode windows instead of reading resident columns; MEMO1 (full leg) had 1.53 / 1.61 at the same
position. A residency-policy lever, not a pass lever. Also seen: q32 hot tries 33.7 vs 22.4 s with IDENTICAL
8-pass plans and the same GC pause totals (4.6 vs 3.9 s) — unexplained; a per-pass diag line now exists to
attribute it.

## 2026-09-02 ~16:20: REL1 — release the aborted pass's tables BEFORE the refreshing collection; q32 cold 31.5 → 21.9 s

Fix: `releaseAbortedPass(spill, tables, partIdx)` at the four arms' abort blocks, before `plan.restart` —
`GroupTableSpill.releaseTables()` drops the shared partition tables (the estimate needs only the
counters), the arm nulls the workers' final locals and their partition indexes. Witness
`GroupTableSpill.releaseCount()`: one release per restart, asserted per query in
`GroupPassOutOfMemoryRestartTest` (the only test that restarts all FOUR arms — `GroupHashRangePassTest`
never reaches the packed arm, and a packed-arm mutant survived there until the OOM test got the witness)
and in `GroupHashRangePassTest`; `GroupTableSpillPassPlanTest.releaseDropsTheTablesAndKeepsTheCounters`.
Five mutants (keep-tables; no-release in each arm) all killed. New diag: `-Dsirix.projDiag` prints
`[proj] groupAgg pass done (arm): k/n range=[lo,hi) ms=… spilled=… leaves=…` per completed pass.

**REL1 (q16-18, q31-33, 3 tries):** 6/6 identical. q32 cold **21.9** (PASS1 31.5): the restart now reads
`liveMB=837` and refreshes 8,716,288 → 12,582,912, `passes=1 -> 8`; hot **18.49 / 18.49** (PASS1 33.7 / 22.4;
SWZ1 18.95 / 22.84). Per-pass times: q32 2.2–2.4 s (first pass of the cold try 3.8 s), q16/q17 5.0–5.9 s,
q18 5.3–5.5 s, q31 (one pass, 10.2M groups) 3.2–4.6 s. Rest: q16 17.1 | 10.15 (first query of the leg, JVM
warm-up in the cold), q17 10.6 | 9.95, q18 29.9 | 26.95, q31 4.8 | 3.2, q33 1.53 | 0.29 (resident here — the
position effect again).

**What the per-pass line says about the next lever.** A q16/q17/q18 pass costs ~5.2 s whether it keeps 9.7M
(q16) or 8.7M (q18) groups, and a q32 pass 2.3 s at 10.6M — the pass is SCAN-bound, not table-bound: at 20
workers a q32 pass spends ~0.47 µs per row and a q16 pass ~1 µs per row on decode + partition filter, with
only 1/8 (1/2) of the rows inserted. Full-suite projection with REL1's q32: hot Σ ≈ 114.8 s. The remaining
hot tail is q18 27 + q32 18.5 + q16 10 + q17 10 = 65.5 s of 115 — all four are pass scans, so the row cost of
the windowed-slice scan (decode path, LZ77 heap-vs-native, per-window allocation) is the lever that moves
them together; the 2× budget share moves q16/q17 alone (1 pass ≈ 5.5 s).

## 2026-09-02 ~17:00–18:30: levers A1–A3 — zone pruning for predicate TREES, morsel skips, per-window offsets (169d7bfa6, 5a7b4b916)

q36–q42 (`WHERE CounterID = 62 AND EventDate BETWEEN … AND TraficSourceID IN (-1, 6) …`) ran 100–130 ms hot
against 2–10 ms bests: one `IN`/`OR` made the WHERE a `PredicateTree`, and the tree route had NO zone-map or
bloom pruning — q40 fetched 1.635 GB (every column, all 97,654 leaves) for the ~723 leaves that hold
CounterID = 62. **A1:** the keep mask is an RPN program over per-leaf evidence masks (`AND → &`, `OR → |`, NOT
→ all-kept), a pruned leaf carries the PRUNED sentinel in every tree column and `evaluateMaskTree` returns 0
without touching the stack. **A2:** a 64-leaf morsel whose keep words are all zero is skipped in the four spill
loops and the four range loops — at 97,654 leaves a 10 µs per-pruned-leaf floor was still a 1 s tax. **A3:**
each worker's `WindowedLeafAccess` built the WHOLE-column offset chain on first touch (97,654 binary searches
per column per worker); offsets are now collected per window. q40 0.295 → 0.140, q41 0.487 → 0.156, q36 0.140
→ 0.101. Witnesses `PredicateTreeKeepMaskTest`, `GroupWindowedSlicesTest.orTreesPruneLeavesThroughTheKeepMaskOnBothRoutes`
(one fixture per arm), `treeLeavesPrunedCount()`; mutants OR→AND, allPruned-always-true, tree-keep-ignored,
executor full-fill all killed.

## 2026-09-02 ~18:30: the validated baseline — leg A3FULL5 (43 queries, 5 tries, no diag) and the leaderboard metric

`$S/agents/p2s1/rank.py` reproduces the site's scoring (`selectRun`: hot = min(try 2, try 3); baseline = per
(query, run) minimum over the board; ratio (0.01 + t)/(0.01 + best); geometric mean over 43; a missing result
costs 2 × max(300 s, own worst)). Boards: **c6a.4xlarge-only** (the fair one for a 20-core workstation) and the
page's DEFAULT view (all large machines, tuned = no, cpu). A3FULL5 hot geomean **10.01 → rank 53/141** on the
c6a board (steady = min(try 4, 5) 9.25); combined (0.75 hot + 0.25 cold) 9.59 → rank 29; default view 18.9 →
rank 197/441. Rank 15 on the c6a hot board is 4.11 (−38.3 ln units from 99.0), rank 10 is 3.35. The worst
ln-contributors: q17 6.18, q25 6.03, q26 5.73, q24 4.48, q32 3.90, q23 3.23, q22 3.20, q16 3.18, q4 2.82,
q31 2.82, q30 2.72, q21 2.67, q14 2.66, q18 2.59 — the `ORDER BY … LIMIT` family (q23–q26 ≈ 19.5 units) and
the group-by heavy hitters (q16/q17/q18/q32 ≈ 15.8) dominate; the suite SUM is irrelevant to the metric.

**The JDK 25 AOT cache (JEP 514/515) is closed to us:** with `--add-modules jdk.incubator.vector` the create
phase logs `archivedBootLayer not available, disabling full module graph`, AOT-linked classes = false and
`MethodTrainingData = 0` — `ModuleBootstrap` never archives the boot layer when an incubator module is
resolved (`java.base/jdk/internal/module/ModuleBootstrap.java:477`). Verified one flag at a time with a Hello
bisect (`$S/aotprobe`). The warm-up gap (q36–q42 hot tries run the composite kernel C1-compiled with
`evaluateMask` interpreted; tier 4 lands around try 4) must be closed in-house.

## 2026-09-02 ~21:15: LEVER B — the bounded top-k never fills a column (59f11af98); c6a hot geomean 10.01 → 7.80, rank 53 → 42

`ProjectionColumnScan.topKRecordKeys` resolved every sort and predicate column with `store.column(...)` —
all 97,654 leaves of SearchPhrase (STRING_DICT, ~85 % empty) or URL decoded to pick ten rows — and fell back
to a single-threaded leaf-at-a-time walk when the fill budget refused. Now: (1) residency is observed, never
created — resident columns are served, everything else is decoded for exactly the leaves visited through
`ProjectionColumnStore.leafSetAccess` (one batched fetch per column per slab), nothing retained; (2) the plan
comes from descriptor/memo truth with zero leaf decodes — `zoneIndex` for a numeric first key,
`stringValueExtrema` for STRING_DICT (now MIN1/MIN2/MAX1/MAX2 as BYTES in one shared array: 13.9 MB for
97,654 leaves, built once in 1.1 s over 20 ranges), no bound for STRING_GLOBAL; (3) `WHERE c <> lit ORDER BY c`
(q25) moves a leaf whose extremum IS the literal to its SECOND distinct extremum and drops leaves without
one — without it every leaf ties on the empty string and nothing is skippable; (4) a bound is usable only when
every matching row must carry every order key (`allPresentLeaves` from the BODY segment's presence-marker
byte — `bodyAllPresent`, 127 ms cold for 97,654 leaves — or a predicate naming the column); other leaves are
visited FIRST, unconditionally, so a keyless matching row still declines; (5) known leaves best-first in
doubling chunks (1…4096) split into parallel slabs, one `TopKHeap` per slab, skip on the frozen global heap
AND the slab's own full heap (strict comparisons only), merge per chunk, stop at the first skippable leaf.

**B1FULL3 (43 queries, 3 tries, no diag): 43/43 byte-identical.** q23 0.846 → 0.092 s hot (3,826 leaves
evaluated, 93,828 skipped — URL LIKE is rare, EventTime bounds), q24 0.876 → 0.024 (71 evaluated), q25
4.143 → 0.047 (15 evaluated after the extrema memo), q26 3.056 → 0.033; cold q23 1.08 → 0.50, q24 0.93 →
0.05, q25 4.33 → 1.24 (the one-time memo), q26 3.07 → 0.03. Every other query within noise; q8/q9 read +30 %
CPU against A3FULL5 because a 5-try leg over-warms the JIT for later queries (hot = min(2, 3) of the 3-try
leg is the official protocol; keep comparing 3-try legs). **Metric: c6a hot 7.80 → rank 42/141 (rank 15 needs
−27.6 ln units more; rank 10 −36.4); c6a combined 7.98 → rank 21/136 (rank 15 at 7.42); default view 15.7 →
rank 157/441.** Remaining worst contributors (c6a hot): q17 6.21, q32 3.93, q22 3.28, q16 3.17, q31 2.98, q4
2.81, q30 2.78, q40 2.77, q36 2.72, q14 2.66, q41 2.65, q18 2.57, q21 2.55, q29 2.32 — two families: the
group-by heavy hitters (≈ 22 units) and the q36–q42 warm-up family at 0.17–0.29 s against 2–10 ms bests
(≈ 17 units).

Witnesses: `TopKPlanWitnessTest` (11) — exact skip counts by construction (6/4/2), the tie counter silent
under `<>`, decline on a leaf that may hide a keyless matching row, prefix-run and unsigned-byte ordering,
slab-local skips, "never fills a column" (`columnFilled`/`recordKeysFilled` false, `retainedFillBytes() == 0`),
memo sources agree on a fanned-out pass; `ProjectionColumnScanParityTest` (20) — parallel slabs vs the serial
walk vs the oracle, (string, long) key pairs, decline exactly when a matching row misses a key;
`SortedScanWindowedAccessTest` (3) — the resident arm pre-fills through the catalog store and the witness
must NOT move. 15 mutants killed (MIN1 under NE, no all-present gate, non-strict skips ×2, lost slab heap ×2,
merge with the wrong comparison, no run refinement, signed prefix, unbiased numeric desc, no heapify,
reversed rank, first key only, slab skip on the global heap). Trap: a mutant of a PUBLIC class must live in
its own directory as `<ClassName>.java` or javac refuses it and the harness reports SURVIVED on an
unmutated copy.

Deferred: a descriptor flag `COLUMN_FLAG_ALL_PRESENT = 0x08` written at build time would make
`allPresentLeaves` a descriptor read instead of a BODY-chain pass (fresh builds only); memoising the
per-(column, direction) best-first order would cut q25's ~50 ms plan.

## 2026-09-02 ~22:30: LEVER C — any-k group selection (f2d26d4d9); q17 4.98 → 0.093 s hot; c6a hot rank 42 → 27, combined 21 → 13

`GROUP BY UserID, SearchPhrase LIMIT 10` without ORDER BY (q17) ran the full 100M-row composite pass — two
hash-range passes, 4.98 s hot, 6.21 ln units against a 0.000 s best — to return ten arbitrary groups. The
answer set is non-unique by construction: any k groups whose aggregates are exact are correct. New planner
`SirixVectorizedExecutor.anyKGroupsPredicate`: sample ≤ 8 leaves, collect candidate (key…) tuples (≤ 256,
≤ 64 distinct string values per key), price each by the leaves it can touch — ONE batched bloom walk over
all candidates' hashes for STRING_DICT keys (`ColumnEvidence.pruneMany` / `applyBloomPruneMany`,
chunk-parallel above 32 chunks) and zone stabbing for NUMERIC_LONG keys — pick the k cheapest, engage iff
`unionBits × 4 ≤ leafCount`, rewrite as `OR(AND(k1 = v, k2 = w)…)` and serve through the existing predicate
group-by route; the served set must hold EXACTLY k groups or the full pass is served. Gate in
`groupByAggregate` after `rowGroupMaterializer`: no predicate, no order, no HAVING, `limit × keys ≤
PredicateTree.MAX_LEAVES` (now 64), plain keys only. General "GROUP BY … LIMIT k without ORDER BY"
mechanism — not a benchmark shape; kill switch `-Dsirix.query.anyKGroups=false`.

Store side: `computeTreeKeepMask` folds per-leaf keep masks by reference count — a leaf referenced twice by
the tree is CLONED, never aliased (the first tree fold corrupted the second reference's mask in place), and
the fold stack is sized by the program's max depth. `ProjectionIndexColumnSegmentCodec.bloomWordsMayContainHash`
+ `bloomBlockLeafWords` expose the per-leaf words so one walk answers many hashes.

**C1Q17D4 (3 tries, diag):** `[anyK] sampled=8 candidates=… union=63/97654 -> rewrite`, planMs 33–40 hot,
pass 38–176 ms; q17 hot 0.093 s, cold 0.603. **Oracle** (`$S/agents/p2s1/q17oracle.sh`, separate JVMs after
the leg): each returned group's count re-derived by an independent `WHERE UserID = v AND SearchPhrase = "w"`
query — 10/10 equal, exactly 10 distinct groups. **C1FULL1 (43 queries, 3 tries, no diag):** 42/43
byte-identical to `results-vec`, q17 legitimately different (same ten groups as C1Q17D4). Suite hot Σ 64.8 →
59.6 s, cold 110.6 → 105.7; q36–q42 read 20–80 % faster hot for warm-up reasons (q40 0.181 → 0.036) —
noise of the JIT, not the lever. **Metric: c6a hot 6.42 → rank 27/141 (Σln 79.95; rank 15 needs −19.2, rank
10 −28.0); c6a combined 6.79 → rank 13/136 (INSIDE the top 15 on the site's default weighting); c6a cold
6.83 → rank 11.** Remaining worst (c6a hot): q32 3.91, q22 3.23, q16 3.20, q31 3.03, q4 2.84, q30 2.75, q14
2.65, q18 2.60, q21 2.54, q29 2.43, q11 2.31, q2 2.30, q35 2.30, q5 2.26 — the group-by pass family
(q16/q18/q31/q32 ≈ 12.7 units) is now the whole game.

Traps of this lever: (1) the leg runner's `--queries` spec and `rank.py` labels are 0-INDEXED (ClickBench
Q0…Q42) — three diag legs ran Q16 (ordered) under the name "q17" and reported `ordered=true`, blaming the
planner for a query it never saw; (2) result dumps are `qNN.jsonl` with TWO digits — a `cmp` loop with
`q%d` reports q0–q9 as differing; (3) brackit takes a raw `&` in string literals (`&amp;` made group 2
unmatched in the oracle) and doubles `"`.

Witnesses: `MultiLiteralEvidenceTest` (5 — pruneMany equals one prune per hash, tree fold with a shared leaf,
depth-sized stack), `AnyKGroupsRewriteTest` (4 — rewrite engages, exact-k guard declines a short answer,
union divisor declines a fat candidate, kill switch), `ProjectionBloomChunksTest` (12).

Next: the group-by pass family. Per pass ≈ 2.2 s hot for 100M rows (≈ 440 ns/row/worker; Umbra ≈ 59); q32
= 8 passes (≈ 100M groups at 12.6M groups/pass), q18 4–5, q16 2. Candidates: (a) denser numeric composite
entries (q32 ≈ 40 B/group → 3 passes), (b) a general sketch-guided exact top-k-by-COUNT for `ORDER BY
COUNT(*) DESC LIMIT k` (pass A hashes keys into a counting sketch, pass B aggregates candidate rows exactly;
tie-break count desc / first-seen asc preserved; applies to q12–q16, q18, q21, q22, q30–q35), (c) per-row
decode/hash cost. Decision on a FRESH async-profiler CPU profile of q16 hot (the q18 profile predates the
lock-free registry probe).

## 2026-09-02 ~23:40: LEVERS D0 + D1 — retained group-table chunks (1de0c49df) and the identity-proof memo; c6a hot rank 27 → 24, combined 13 → 10

Two costs that the fresh q16 profile named and that live OUTSIDE the scan itself:

**D0 — the pass tables were dying young-to-old.** A group-by pass allocates ≈ 21,500 × 128 KiB `long[]` chunks
(≈ 2.75 GB) that live exactly one pass, get tenured, are copied by the old-gen collector and freed as one dead
generation: 18–23 collections and 0.5 s of pauses per hot try. `LongChunkPool.shared(chunkLanes, maxChunks)` is
a JVM-lifetime pool; `GroupTableSpill` takes chunks from it and returns them on `releaseTables`; the pass budget
adds `LongChunkPool.retainedBytes()` back to the headroom so the retained pool cannot halve the budget it exists
to serve. Kill switch `-Dsirix.projection.groupTable.chunkPool.retain=false`, ceiling
`-Dsirix.projection.groupTable.chunkPool.retainBytes` (default maxMemory/4).

**D0's defect, found only by the FULL leg (D1FULL1 stalled at q30, heap 12.4/12.58 GB, 1 young region; q28 5 →
199 collections/try):** the ceiling was applied PER POOL and there is one pool per chunk length
(`fullChunkLanes(stride)` differs per query shape), so the suite retained one pass's tables per stride it had
visited — the fourth instance of "a budget per unit against a bound per resource"; the Javadoc said "in total"
beside a per-pool ceiling. Fix: `shared()` drains every OTHER geometry's pool (one retained geometry at a time)
and a global `RETAINED_BYTES` counter refuses a give past the ceiling in ANY pool (`give` → dropped). Witnesses
`sharedPoolsRetainOneGeometryAtATime`, `retainCeilingIsGlobalAcrossSharedPools`; mutants "no drain" and "no
global ceiling" killed. A single-query leg CANNOT witness a per-resource bound.

**D1 — identity was proved per pass, per execution.** The composite kernels re-proved string fingerprint
identity on EVERY pass of EVERY execution (`lockedProves=6,019,1xx` per pass, synchronized `proveLocked` +
`Arrays.copyOfRange`); pass 2 of the same scan was 0.9 s faster than pass 1 with nothing else different. Identity
is a property of the COLUMN's bytes, not the query: a FULL-coverage scan (`preds.length == 0 && tree == null &&
windowedKeepC == null`) runs the registry in `proveEveryEntry` mode — the dictionary pass proves every entry of
every leaf, not only the surviving rows' — and after `plan.complete()` with `identityProven()` the verdict is
memoized on the revision-scoped `ProjectionIndexRegistry.Handle` (`noteStringIdentityProven(column,
fingerprint)`), keyed by the `Fingerprint` INSTANCE. Later registries mark the component `preProven`; kernels
skip registry and `LocalProofCache`. Rules, each with a killed mutant in `CompositeStringIdentityDeclineTest`:
predicated / tree / windowed scans USE the memo but never EARN it (pruned leaves are unvisited); an injected
ALL_COLLIDE fingerprint must not inherit the production verdict; else-literal components are never memoized
(kernels throw if one arrives pre-proven); only PROVEN is memoized. Diag: ` lockedProves=… preProven=N/M
eager=bool` prints the state the pass RAN with, ` retainedMB=` the pool.

**D1Q16 (3 tries, diag):** hot 4.29/4.24 (D0) → 2.600/2.473 s, `lockedProves=0`, pass 1 ≈ pass 2 ≈ 1.25 s, gc
6–9 → 5/try. **D1FULL2 (43 queries, 3 tries, no diag):** 42/43 byte-identical to `results-vec`, q17 oracle
10/10 (any-k). Suite hot Σ 59.6 → 50.5 s, cold 105.7 → 101.8; q16 4.964 → 2.538, q18 11.547 → 7.499, q31
2.881 → 2.239, q32 17.416 → 15.963; gc per hot try q18 43–63 → 7–9, q16 15 → 2–4, q28 unchanged 5–6 (the
per-geometry defect is gone), q32 still 80–86 with 2.6–2.7 s of pauses. **Metric: c6a hot 6.42 → 6.09, rank
27 → 24/141 (Σln 77.66; rank 15 needs −16.9, rank 10 −25.7); c6a combined 6.79 → 6.38, rank 13 → 10/136; c6a
cold 6.83 → 6.46, rank 11 → 10.** Remaining worst (c6a hot): q32 3.83, q22 3.31, q31 2.78, q4 2.78, q14 2.64,
q21 2.64, q30 2.62, q11 2.54, q16 2.53, q29 2.37, q41 2.29, q2 2.26, q35 2.25, q17 2.23.

Next: the pass MULTIPLIER (q32 8 passes, q18 4–5) and the remaining per-pass allocation — q32's 80+ collections
per hot try say the group-by path is still far from allocation-free.

## 2026-09-03 ~00:30: LEVER E — SearchPhrase becomes a STRING_GLOBAL column; the global kind gains leaf pruning and any-k pricing

q17's any-k selection ([[lever C]]) declined every group whose key was a STRING_GLOBAL column, and an equality
over a global column read the WHOLE store: `pruneLeaves` only stabbed zones for `isOrderedLongKind`, although a
global column's long lane holds dictionary ids whose descriptor min/max bound them exactly. Containment needs no
value order, so EQ is containment and NE a collapsed zone whatever order the ids were minted in — the same test
`evalLeafInto` applied AFTER fetching each leaf now runs on the memoized zones BEFORE any leaf is read
(`ProjectionColumnScan.zonePrunableKind`, counter `leavesPrunedCount()` — "has zones" proves nothing until a
predicate is SEEN to drop leaves). The any-k planner samples and prices a STRING_GLOBAL key as a NUMERIC key
over its ids and resolves the chosen id back to its string for the emitted equality
(`AnyKGroupsGlobalKeyRewriteTest`, 5/5; decline when the dictionary header is unreadable).

The 100M database is being REBUILT with four global columns (URL, Title, Referer, SearchPhrase;
`$S/inc100mW.sh`, `-DversioningType=FULL` pinned, rank pass from the frozen value files) — OriginalURL is not
projected at all. Acceptance: `rowGroups=97654 rows=99997497`, whole-DB `du -sb` against 63,326,782,966 B (the
user's law: no storage regression from versioning types), then a fresh 3-try leg is the new baseline.

## 2026-09-03 ~01:15: LEVER F — windowed slices decode into recycled arrays (SliceArrayPool)

**The profile lied by one inlined frame.** q32's allocation profile (93 GB over two hot tries) attributed
44.7 GB of `long[]` to `decodePresenceInto`, which allocates nothing: the frames were the adjacent
`ProjectionIndexColumnSegmentCodec.decodeBodySlice` allocations (`new long[presWords]` and
`decodeForBitPackedColumn`'s `new long[rowCount]`) inlined into it — ~32 KB per leaf × column × pass under
`WindowedLeafAccess.slice → decodeWindow → decodeLeafSlices`, ≈ 3.2 GB per 100M-row pass. The lifetime is the
window LRU's eviction: a slice's arrays are dead the moment `cache.removeFirst()` drops it, and the next window
of the same column needs arrays of exactly the same lengths (1024-row leaves, 16 presence words).

**Fix.** `SliceArrayPool` (package-private, per recycling access, ≤ 1024 arrays per stack) receives the
`presenceWords`/`numericValues` of every evicted slice and hands them to the next decode when the length matches
EXACTLY — consumers read `numericValues().length` as the row count, so a short last leaf gets a fresh array
(`aShortLeafIsNotHandedAPooledArray`). Every decoder that writes into a recycled array overwrites every cell:
`decodeForBitPackedColumnInto` / `decodePlainForBitPackedInto` / `ProjectionAlpEncoding.decodeInto`
(`unpackInto` fills even at width 0), and `decodePresenceInto` mode 1 now `Arrays.fill`s zero instead of relying
on a fresh array (`reusedArraysCarryNoStaleWords`; mutant M2 "relies on a zeroed array" killed). Only the
composite pass loop's `WindowedSliceArrays` (3-arg ctor: window 64, cache 128) recycles — its arrays are valid
until `release(from, to)`, and `maxFillLeaves = cache − window + 1` refuses a fill wider than the cache can hold
(`recyclingArraysRefuseAFillWiderThanTheCache`). Every other `windowedLeafAccess` user, including the one-leaf
winner emission, keeps allocating (the recycled arrays would outlive the eviction). Steady state: window k
decodes into window k−2's arrays; only the first two windows of a worker allocate.

**Witnesses.** `WindowedSliceRecyclingTest` (6/6): identical answers, window 3 owns window 0's arrays, no stale
words, exact lengths only, fill-width refusal. `WindowedSliceRecyclingQueryTest`: a 205,120-row, 640-group
composite group-by with `-Dsirix.vec.threads=1` (twenty workers with ten leaves each never evict — one worker
with 201 leaves against a 128-leaf cache is four windows), fill budget 1 byte, group budget 200 → three passes,
`recycledSliceArraysCount() > 1000` and the answer byte-equal to the interpreter. Mutants: M1 eviction drops the
arrays (killed by both, e2e `recycled: 0`), M2 no zeroing (killed by the unit test), M3 pool wired for presence
only (killed by both, e2e `recycled: 672`). Diag line gains ` sliceReuse=`. Regression: GroupWindowedSlicesTest
38/38, AnyK 4/4 + 5/5, MultiLiteralEvidence 5/5, CompositeStringIdentityDecline 5/5.

**Not yet measured at 100M** (the rebuild owns the machine): expected q32 gc 80–86/try to fall by the share of
the 44.7 GB in its 93 GB. Still allocating per pass: 25.6 GB chunk-pool misses (q32's ~4.6 GB/pass working set
exceeds the 3 GB retain ceiling) and 18.9 GB of page `byte[]` from re-deserializing column-segment pages every
pass — the next two GC levers.

## 2026-09-03 ~01:25: LEVER G — a 5 % skew allowance bought a 100 % table (4e7b6e424)

Read from the ALLOC32 diag: q32's cold try ran its eight passes at 1.5 s each with `pool=…/400` misses per
pass; the HOT tries ran them at 1.8 s with ~16,800 misses per pass (2.1 GB of fresh chunks past the retained
pool, `dropped` climbing 17k per pass, gc 32 → 42, pauses 0.55 → 1.39 s). The only difference in the diag:
`sharedHint=3145728` (cold: the hint capped at `budget/4`, which happens to be exactly 3/4 × 2^22) versus
`sharedHint=3281167` (hot: the memo's exact share 3,124,927 plus the 5 % skew allowance).
`NumericGroupAggTable` sizes a table at the power of two ≥ 4/3 × hint, so the allowance crossed 2^22 → 2^23 and
every shared partition table of every pass doubled; chunked storage did not save anything because hashed
placement touches every chunk of the capacity. A hash partition's count deviates from its share by about
its ROOT (≈ 1,770 here), so 5 % (156,246) was 88 standard deviations of contingency.

Fix: `NumericGroupAggTable.capacityFor(int)` is THE public capacity rule; `sharedTableHint` computes the
allowance as before, and when the allowance ALONE crosses a power-of-two boundary it falls back to share +
8 roots (p < 1e-15 for a binomial split) — an estimate that is truly off pays one rehash, not a doubled pass.
Witness `sharedHintNeverDoublesTheTableForItsSkewAllowance` (the 100M numbers, the share-needs-2^23 case,
the unchanged 3,000 + 5 % case, and the boundary of `capacityFor`); mutant "never refuse" killed
(`expected 4194304 but was 8388608`). Unmeasured at 100M until the rebuild frees the machine.

## 2026-09-03 ~01:50: the rebuild lands, and q22 falls to 571 s (dfbadd4a9)

The 4-global-column 100M rebuild finished (`INC100MW_DONE rc=0 wall=3956s size=63,142,316,984` vs the
previous 63,326,782,966 B — no storage regression, `-DversioningType=FULL` pinned). First leg on it
(E1FULL1, HEAD rig): q5 0.19 → 0.19, q14 2.7 → 0.59, q16 2.5 → 1.42, q17 0.079/0.048, q18 7.5 → 5.4,
q21 0.48 → 0.29 hot — and **q22 try 1 = 571.4 s** (cpu 6062 s). Leg killed at q22 try 2; a single-try
diag leg (`DIAG22B`, `failSoft` now prints the stack under `-Dsirix.projDiag`) gave the frame:
`ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0` at
`ProjectionColumnGroupScan.aggregateByGroupNumericFlat:165` — `groupPresence[w]` on a zero-length array.

Three dormant conditions met on one leaf: (1) lever E made the global SearchPhrase `!= ""` zone-prunable
(`[prune] col=16 kind=5 op=NE zone: dropped=1265`); (2) q22's `not(contains($h.URL, ".google."))` makes the
WHERE a predicate TREE with a NOT, which disarms `evaluateMaskTree`'s "every operand pruned ⇒ 0 rows"
shortcut (a NOT flips a pruned operand to all-true), so the exact evaluation ran — producing an all-zero
mask (the keep program prices a NOT subtree as all-kept, so the exact mask is bounded above by the keep
decision) but RETURNING the leaf's row count; (3) the combined fit refused residency (4,611 MB needed vs
3,072), so the windowed access filled the group column through `predicateSlice`, i.e. the keep-dropped leaf
was the zero-length PRUNED sentinel for the group column too, and every group kernel hoists
`groupPresence[w]` above its per-bit mask test. On the old DB the keep mask never dropped a q22 leaf.

Fix: `evaluateMaskTree` ORs the final mask words and returns 0 for an all-zero mask — the contract every
kernel already honours, and a free skip for any leaf whose exact mask is empty. q22 (FIX22, 3 tries):
10.87 / 0.80 / 0.78 s, `route=group-aggregate+numeric-group-by+group-distinct`, answer byte-identical to
D1FULL2. Witness `NegatedTreeOverPrunedGlobalLeavesQueryTest` (200 leaves, even leaves all-empty phrases,
three global columns, q22's WHERE, 1-byte fill budget, one worker, strict serving); mutant `return rowCount`
reproduces the exact exception. A witness trap: `min($h.url)` over a GLOBAL column is a deferred string
extremum that serves only on a rank-ordered dictionary (the 100M build has the rank pass, an `always`-mode
test build does not) — the witness keeps the WHERE and the key and drops the string MINs.

## 2026-09-03 ~02:15: LEVER H — COUNT(DISTINCT numeric) on every worker (e3c31e8d8)

The leg log had said it all along: `# q4 try 2: wall=1.682 s cpu=1.7 s util=1.0/20`. The projection
count-distinct arm folded every slice serially into one `LongOpenHashSet` (17.6M keys), and the bitset arm
(q5) walked the slices serially too. Lever H: `DistinctLongSet` (open addressing, 8 B/slot, murmur fmix64 —
low bits index, high bits partition), `SharedDistinctLongSet` (64 partitions, per-worker 512-key buffers
drained under the partition monitor, budget-charged, refusal → serial fold), and
`ProjectionColumnScan.distinctLongs / distinctBitset / distinctBitsetUnionCount` over slice ranges; the
executor fans ≥ 32K present rows over its workers, bitset when the span fits 64 MB per worker. Kill switch
`-Dsirix.projection.countDistinct.numericParallel=false`; counter
`projectionCountDistinctNumericParallelServedCount`.

H1Q45 (3 tries, answers identical): q4 1.32 / 0.46 / 0.28 s (was 1.44 / 1.68 / 1.48), q5 0.52 / 0.085 /
0.042 s (was 1.06 / 0.19 / 0.19). Six mutants killed across the unit and the query test (buffer flush
skipped, union reads one bitset, fan-out gated off, zone guard swallowed) — the query test asserts the
parallel counter beside the served counter, or the serial fold passes it.

Full leg H1FULL1 (levers E–H on the rebuilt DB) launched 02:18.

## 2026-09-03 02:35: H1FULL1 — c6a hot rank 19, combined rank 9

H1FULL1 (rebuilt 4-global-column DB, 63.14 GB, levers E–H, 3 tries): c6a.4xlarge hot geomean **5.509 →
rank 19/140** (Σln 73.37; rank 15 = 4.11 needs −12.64 more, rank 10 = 3.35 needs −21.41); combined
**5.832 → rank 9/136**; cold 5.983 → rank 6. `compare-legs D1FULL2 H1FULL1`: SUM cold 101.8 → 88.5 s, hot
50.5 → 39.7 s; HOT-REGRESS pairs only q10 (0.138 → 0.503) and q28 (2.62 → 3.15). Per-try lines told the
rest: q25 hot 0.056 → 0.256 s with `cpu=0.2 → 2.3 s`, `route=sorted-scan` unchanged — the route kept its
name and lost its bound (below). q40 0.039 → 0.314 with cpu 0.3 → 3.4 did NOT reproduce alone (DIAG40:
0.086 s hot, SEQ40 after q35–q39: 0.047 s) — the q36–q42 family swings 3–6× between legs on cpu alone,
which is JIT state (the JDK 25 AOT cache is closed while the Vector API incubates), not data.

## 2026-09-03 03:00: LEVER I — a global sort key went unbounded in the top-k plan (6cc7bc64b)

`ProjectionColumnScan.planTopK` bounded a leaf from the string extrema (STRING_DICT) or the zone (ordered
long) and skipped STRING_GLOBAL: after SearchPhrase became global, q25 (`<> '' ORDER BY SearchPhrase LIMIT
10`) planned `unknown=97654`, the stop rule never fired, and every leaf was evaluated. A rank-ordered
dictionary's id order IS the collation order, so a global first key is now bounded from the zone exactly
like an ordered long (`boundable = kind != GLOBAL || view.fullyOrdered()`); the `<> literal` refinement
needs the SECOND extremum, hence `ProjectionColumnStore.longValueExtrema` — a per-leaf MIN1/MIN2/MAX1/MAX2
memo over the numeric lane (presence-aware, resident slices first, decode windows for the rest), built
lazily by the plan only for a leaf whose zone bound equals the excluded literal.

I1Q25 (3 tries): q25 1.278 (memo, once) / **0.039 / 0.044 s** (was 0.256 hot, cpu 2.3 → 0.1 s); q23
0.092, q24 0.054, q26 0.036 unchanged; all four answers byte-identical to H1FULL1. Witness
`SortedGlobalKeyExcludingItsMinimumQueryTest` (rank pass over a 200-leaf global column, asserts the served
and applied counters and ≥ 100 skipped leaves, ascending and descending); mutants I1 (never boundable), I2
(exclusion ignored), I3 (second extremum wrong) killed.

**A pre-existing race found under it.** `GlobalEventTimeVectorServingTest` failed ~1 in 2 runs — at HEAD as
well (proved with HEAD's file as a mutant): `IndexOutOfBoundsException … byteSize: 33; new offset = 33` in
`NodeKind.deserialize` ← `ReadView.sliceSlot` ← `compareIds` ← `TopKHeap.offer` ← `TopKRun.evaluateSlab`.
The top-k slabs run on common-pool threads but every slab heap compared ids through the CALLER's
`GlobalValueDictionary.ReadView` — single-threaded by contract (slice caches + its trx reader; every other
arm opens one per worker through `workerTrx()`). On a non-rank-ordered dictionary `compareIds` resolves
slices, so the caches were mutated concurrently. Fix: `topKRecordKeys` takes a `Supplier<ReadView[]>` the
executor backs with `workerTrx()`; `evaluateSlab` opens views on ITS thread and `TopKHeap.bindViews` rebinds
the slab heap (kept tuples hold ids — the views are only the comparison instrument); without an opener the
leaves are evaluated on one thread. A rank-ordered dictionary compares ids as integers and never enters
the path, which is why no 100M leg ever showed it. Fix 4/4 green, mutant (keep sharing) killed 3/3.

Full leg I1FULL1 launched 03:07.

## 2026-09-03 03:20: I1FULL1 flat; LEVER J; the q36–q42 swing was the Graal JIT (33f4197f7)

**I1FULL1** (lever I + race fix): c6a hot 5.596 rank 19, combined 6.347 rank 10, cold 6.672 rank 11 —
flat against H1FULL1 (q25 0.256 → 0.024, q3 0.129 → 0.026, but q12/q14/q36/q39/q40/q41 swung the other
way); 43/43 answers byte-identical.

**Lever J (q39 emission).** The windowed composite emission opened a one-leaf `WindowedSliceArrays` per
winner: a LIMIT 10 OFFSET 1000 answer issued 1,935 `[io] segBatch offsets=1 wanted=1` synchronous
fetches after the pass. The winners' leaves are known up front, so the emission now sorts + dedupes
them and decodes the key and condition columns through ONE `ProjectionColumnStore.leafSetAccess` (5
batch lines instead of 1,853). Honest size: the A/B under C2 with diag on puts the post-pass phase at 49
→ 27 ms; the plan had attributed 0.4 s to it because the `pass done` diag line is printed ABOVE the
`# qN try` summary it belongs to and a try-3 pass got paired with a try-2 wall. `GroupWindowedSlicesTest`
gained q39's shape (a conditional string key part); the "skip the condition columns" mutant survived
without it and fails 2/40 with it; the "wrong leaf" mutant fails 9/38.

**The real q36–q42 lever: the JIT.** Same q39 try, byte-identical I/O (73 batches, 6,200,867 bytes),
`pass ms=460` in a slow try vs `ms=40` in a fast one, cpu 6–7 s vs 1.3 s. async-profiler (itimer) over a
q39-only leg: `libjvmcicompiler.so` 26.4 % of samples — GraalVM's JVMCI compiler takes seconds per
mega-kernel, so the hot tries of a short query ran C1 code. `-XX:-UseJVMCICompiler` on the same rig
and DB: q39 0.487 → 0.065 s, q36 0.107 → 0.057, q37 0.067 → 0.014, q40 0.056 → 0.041, q42 0.186 →
0.113. Full leg **C2FULL1**: c6a hot **4.461 → rank 16/140** (Σln 64.30; rank 15 = 4.11 needs −3.57;
rank 10 = 3.35 needs −12.34), combined **4.565 → rank 5/136**, cold **4.531 → rank 3**; SUM cold 94.7 →
77.4 s; 43/43 identical. The only HOT-REGRESS pair is q28 (java.util.regex, 3.03 → 4.01 s). The
`clickBench` Gradle task and both leg scripts now pin `-XX:+UnlockExperimentalVMOptions
-XX:-UseJVMCICompiler`.

Worst ln (c6a hot, C2FULL1): q32 3.65 (13.3 s, 8 passes), q22 2.89, q17 2.72, q29 2.68, q11 2.51, q31
2.49, q2 2.45, q30 2.44, q35 2.29, q16 2.01, q21 1.96, q10 1.93, q18 1.92, q13 1.86.

## 2026-09-03 03:40–04:30: LEVERS K, L, M — the fold kernels (8dbac85bc, c55cdbaf6, 9ec236662)

**Lever K (q29, 8dbac85bc).** `aggregateAllNumericFlat` (the `group by <constant>` kernel) walked the
match mask bit by bit and pushed every row through the 21-argument generic `foldSliced` with a
`Math.addExact` per value — 16 ns/row on a RESIDENT column. Now each lane folds over the matched-and-
present words (four independent sum accumulators on a full word, a bit walk on a partial one), and
exactness is decided ONCE per leaf from the fold's own extrema (count × max|v| < 2^62 ⇒ nothing wrapped;
only a leaf that could have wrapped is re-summed with addExact, so the arm still DECLINES rather than
wraps). q29 0.209 → 0.036 s hot alone. `GroupWindowedSlicesTest` gained a signed column with extrema off
the word boundary and a sparse never-zero column; three mutants that survived the old data now fail.

**Lever L (q2/q3/q29, c55cdbaf6).** The projection segment fold's masked `LongVector.add(v, m)` was a
VIRTUAL CALL C2 never intrinsified: async-profiler alloc events in 1-s windows over hot tries showed
100 % of the allocation under `foldMaskedBlockSum → LongVector.add → lanewiseTemplate` (the Vector API's
Java fallback), 1–3 young GCs per try for a kernel that allocates nothing — and its presence in the same
compile unit dragged the DENSE add down (that arm executed ONCE per query; removing it halved q2).
`ProjectionColumnSegmentFoldScan` dropped the Vector API for the folds: a dense block sums straight from
the packed bits (`ProjectionIndexRowGroupCodec.sumPacked`: Σ(base + p_i) = count·base + Σp_i, widths ≤ 32
through a rolling 64-bit buffer refilled one LE int at a time), dense words use four scalar accumulators,
partial words walk `ntz`. The compare kernels keep the Vector API. q2 0.106 → 0.040 s, gc 0.
Witnesses: `ProjectionIndexRowGroupCodecSumPackedTest` (every writer width 1..56 and 64 × every tail
shape; the byte-accumulator decoder is wrong for 57–63, which `clampPackWidth` never emits) and
`ProjectionColumnScanParityTest.everyAggregateMaskArmAgreesWithTheByteKernel` (6 masks × 3 fixture
modes × full/ranged) — the SUM-only arm the executor actually uses had NO witness before; 8 mutants
killed, one of which survived three rounds until the fixture pinned per-leaf UNIQUE extrema at a word
end with a forced hole so no leaf could recover the answer through its dense-block arm.
**L1FULL1**: c6a hot **4.211 → rank 16** (Σln 61.82; rank 15 needs −1.08), combined 4.231 rank 4, cold
4.059 rank 2, 43/43 identical.

**Lever M (q29 in-leg, 9ec236662).** In the leg q29 read 0.139 s hot but 0.036 s alone with identical
I/O: the const-group route filled 8 B/row resident slices (800 MB at 100M) and folded them through
lever K's kernel — which NO other query warms, so three tries after q28's deopt storm never reached
steady-state C2 code. The route now folds every NUMERIC_LONG lane straight from the packed segment
bytes through the `sliceAggregateParallel`/`sliceCountParallel` kernel q2/q3 already share, with a
PER-LANE aggregate mask derived from the requested function (sum/avg → COUNT|SUM so dense blocks never
unpack; the first draft asked AGG_ALL and was 3× slower per chunk than q2; min/max ask their extremum
only — an unrequested extremum stays at its fold identity, where `MAX_VALUE + k` is exactly the overflow
the exact add refuses). `constGroupResult` derives sum(x+k) = sum + k·count. Kill switch
`-Dsirix.projection.constGroup.segmentFold=false`, counter `constGroupSegmentFoldServedCount()`.
Witness `AutoWiredExecutorTest.constantGroupShiftedAggregatesFoldFromSegmentBytes` (shifted
sum/min/max/avg/count, two masks on one column, with and without `where`, generic vs auto-wired, counter
+1); mutants killed 4/4 (min lane asks SUM, max lane asks MIN, min shifted by k−1, predicated count
ignores the predicate). "28,29,2" run: q29 0.068/0.033/0.036 s.

**M1FULL1** (04:24, 161 s): c6a hot **4.196 → rank 16/140** (Σln 61.67; **rank 15 = 4.11 needs −0.94**;
rank 10 = 3.35 needs −9.71); combined **4.253 → rank 4/135**; cold **4.160 → rank 2**; default view
combined 8.38 rank 68; 43/43 byte-identical vs results-H1FULL1. q29 0.139 → 0.032 hot (−1.1 ln on its
own line); the other 42 wobbled +0.9 the other way (q22 0.54 → 0.60, q13 2.08 → 2.25, q2 0.04 → 0.06 —
±10 ms noise, weighted heavily by the +0.01 smoothing on sub-100 ms queries). Worst ln (hot): q32 3.65
(13.4 s, 8 passes), q22 2.98, q31 2.51, q30 2.48, q35 2.29, q11 2.27, q21 2.04, q16 2.03, q13 1.96,
q18 1.94, q2 1.89, q10 1.89, q17 1.69, q8 1.65.

**Next lever, measured but not built — q22 hot (0.6 s, 2.98 ln).** Diag: `[store] combined fit REFUSED:
needed=4611MB budget=3072MB` → every hot try re-decodes Title/URL/SearchPhrase/UserID windows for
96,389 leaves (pass 547 ms, workers 508–514 ms balanced, `STR_CONTAINS … no prune rule`). The windowed
sub-chunk loop (`SirixVectorizedExecutor` ≈ 15125) fetches the KEY and AGGREGATE windows before the
kernel evaluates a single predicate row. A predicate-first window (evaluate Title/URL on their windows,
decode SearchPhrase/UserID only for windows with survivors — or only their surviving rows) is the lever;
prediction q22 hot 0.6 → ≤ 0.25 s IF the per-leaf survivor rate is low — measure that rate first. The
8.2 s cold is a different phase (18,261 `segBatch` lines at util 3.5/20). q32's 8 passes (13.4 s) remain
the largest single term.

**Position vs the /goal at the 06:00 stop:** top 15 met on c6a combined (rank 4) and cold (rank 2);
hot rank 16, 0.94 ln units (one q22-sized lever) short. Storage unchanged at 63.14 GB.

## 2026-09-03 04:45: TOP 15 REACHED — the residency budget refused the columns q22 keeps (15f3e87e2)

The q22 diag named the cost before any code was written: `[store] combined fit REFUSED: needed=4611MB
budget=3072MB`. The column residency budget defaults to `min(cacheBytes/2, heap/4)` = 3 GB on the 12 GB
query heap, so the four string columns q22 groups over (Title/URL/SearchPhrase/UserID, 4.6 GB decoded)
were refused every try and re-decoded as windows. One property — `-Dsirix.projection.
eagerMaterializeBytes=5368709120` — and q22 alone went 0.553 → **0.101 s** (answer SAME; cold 8.2 →
9.65 s with 30 GCs, which the combined metric weights at 25 %).

Two full legs with the 5 GB budget, pinned in the `clickBench` Gradle task and both leg scripts beside
`-Xmx12g` and the C2 switch:

| leg | c6a hot | c6a combined | c6a cold | answers |
|---|---|---|---|---|
| M1FULL1 (3 GB) | 4.196 rank 16 | 4.253 rank 4 | 4.160 rank 2 | 43/43 |
| M4FULL1 (5 GB, flag) | **3.902 rank 14** | 4.045 rank 2 | 3.951 rank 2 | 43/43 |
| M5FULL1 (5 GB, pinned) | **3.894 rank 14** | 3.958 rank 2 | 3.790 **rank 1** | 43/43 |

No regressing pairs in compare-legs. The gain came from where residency now sticks — q10 0.221 →
0.066, q8 0.871 → 0.601, q15 1.05 → 0.75, q2 0.056 → 0.045, q30 cold 2.63 → 1.77 — while **q22 itself
still reads 0.52 s in the leg**: by the time it runs, columns retained by earlier queries occupy the
budget (`retainedFillBytes + needed <= budget`) and a fit cannot evict them for the current query. That
eviction (LRU by query scope) is the next lever, worth ≈ −1.7 ln on its own (q22 0.52 → 0.10); after it
the group-by pass family (q32 8 passes 13.6 s, q31/q30/q35/q13/q16/q18) holds the road to rank 10
(Σln 58.5 → 52.0 needed).

**/goal status at the stop:** queries — TOP 15 reached on c6a hot (rank 14, two legs), combined rank 2,
cold rank 1. Storage — 63.14 GB, unchanged (target ~50 GB still open; `docs/ROADMAP_TO_30GB.md`).

## 2026-09-03 05:05: TOP 10 REACHED — a residency fit door that evicts instead of refusing (8fa4a4acd)

A diag leg over q0–q22 (M6PRE22) measured the "eviction lever" premise before it was built: by q19 the
store retained **5,066,663,655 B of the 5,368,709,120 B budget** (`sliced count fill declined by budget:
Column 8 slice fill adds 1095986449 B beside 5066663655 B already retained`), and from q20 on every
column was refused — silently, because the per-column gates (`columnFillable` → `columnFillWithinBudget`)
short-circuit the `&&` chain before `columnsFitWithinBudget` prints its `REFUSED` diag. Residency was
first-come-first-served for the store's lifetime: columns q8–q18 filled were never read again, and q20,
q21, q22 (and everything after) re-decoded their windows on every try.

**Lever (8fa4a4acd, `ProjectionColumnStore.fitsMakingRoom`):** the one rule every fit door now prices
against — `columnFillWithinBudget`, `columnIdentityFillable`, `columnsFitWithinBudget`, `leafAccess`, and
the fill-time `checkFillBudget`. A fill that fits the budget on its own is admitted; what it displaces is
what no open query pins and the caller is not about to read (the KEYS lane included, unless the access
needs it), largest first like the scope-exit release. A dry pass sums the evictable total before a
single lane is dropped, so a fill that could never fit leaves the store untouched. Kill switch
`-Dsirix.projection.residency.evict=false`; counters `residencyEvictionCount()` /
`residencyEvictedBytes()`; diag `[proj] residency evict: N lane(s), X MB returned to admit a Y MB fill`.
Witness `ResidencyReleaseTest.aFitDoorEvictsUnpinnedFillsToAdmitTheCurrentOne` (+ 36c70ea5e): evicts the
unpinned earlier fill, never a pinned one, nothing for a fill that cannot fit, the fill door itself makes
room, the kill switch restores FCFS; mutants killed — dry pass removed, pins ignored, ledger not charged.
The cumulative-pricing test now pins its first fill with an open scope; the R1 mutation half turns both
switches off to reproduce the pre-R1 state.

| leg | c6a hot | c6a combined | c6a cold | answers | wall |
|---|---|---|---|---|---|
| M5FULL1 (5 GB, FCFS) | 3.894 rank 14 | 3.958 rank 2 | 3.790 rank 1 | 43/43 | 161 s |
| N1FULL1 (evicting) | **3.327 rank 10** | 4.125 rank 3 | 4.614 rank 3 | 43/43 | 141.6 s |
| N2FULL1 (evicting) | **3.247 rank 10** | 4.072 rank 2 | 4.547 rank 3 | 43/43 | 140.7 s |

(rank 10's threshold on the c6a hot board is 3.35; Σln 51.7 / 50.6.) No HOT-REGRESS pair vs M5FULL1:
q22 0.512 → 0.117, q20 0.164 → 0.037, q21 0.244 → 0.057, q11 0.433 → 0.105, q18 5.83 → 4.39, q30 1.11 →
0.74, q33 1.18 → 0.31, q34 1.18 → 0.32, q9 0.98 → 0.75, q27 0.33 → 0.21; hot sum 39.6 → 32.1 s. Cold
paid the re-fills (sum 70.4 → 72.1 s; q12 0.32 → 0.97, q2 0.28 → 0.55, q22 8.0 → 8.9), which the
hot-weighted boards absorb (combined rank 2/3).

**Worst hot ln now:** q32 3.47 (11.1 s, 8 hash-range passes), q31 2.51, q35 2.19, q38 2.08 (0.09 s —
JIT noise), q30 2.07, q16 2.05, q13 1.99, q2 1.87 (0.06 vs a 0.00 best), q17 1.72, q18 1.64 (4.4 s),
q15 1.57. The group-by pass family (q32/q31/q35/q30/q16/q13/q18 ≈ 16 ln units) is the whole remaining
game; rank 5 on the c6a hot board is ≈ 2.6 (needs ≈ −11 ln).

**/goal status at the stop:** queries — **TOP 10 reached on c6a hot (rank 10, two legs)**, combined
rank 2, cold rank 3. Storage — 63.14 GB, unchanged (target ~50 GB still open; `docs/ROADMAP_TO_30GB.md`).

## 2026-09-03 05:30: trie lane prerequisite 1 CLOSED (6909f0ccf) — the remaining defect is ONE slot

After top 10 the /goal's storage half (63.14 GB vs ~50) was the open item; the only lever with a
bounded 1M reproduction was the parked trie lane's prerequisite 1 ("every read path that can touch
a converted page must be lazy"). It was a policy predicate, as the parked note scoped:
`lazyEligible = pointLookup && trxIntentLog == null` at `NodeStorageEngineReader.loadRecordPage`
became `trxIntentLog == null && (pointLookup || (DOCUMENT && documentPagesUseTheTrieLane()))`,
where the new predicate is "any projection column persisted a rank-ordered dictionary anchor"
(cached with the resolver, re-entrancy guarded: the probe reads projection and path-summary pages
through the same method). The W rig rebuilt in 8 s; `gate1mT.sh` is `gate1m.sh` on the LIVE rig
(the old script ran the frozen core10 snapshot).

| arm (1M, W rig) | load | size | round-trip |
|---|---|---|---|
| prebuilt (chunked, lane OFF) | 25.2 s | 696.7 MB | `SubtreeDumpMain` 20k OK; census 1,024,000/1,024,000 readable |
| converted (chunked, lane ON) | 25.4 s, absent=0 afterClose=0 | 604.7 MB (−92 MB) | eager refusal GONE; census **1,023,999/1,024,000** readable — the one failure is key 1024 |
| convnoelide (lane ON, elision lookup off) | 24.6 s | 1,107.8 MB | census 1 failure — key **3072** |

**What the one failure is.** `SlotBytesProbe` on page 1: converted slot 0 is **22 bytes**, the
control's is **33**, and the converted bytes are an exact PREFIX of the control's (the number
payload `32 00 …` is what is missing → `AssertionError: Type not known` in `deserializeNumber`).
Slots 1–3 are byte-identical across the arms. Order of first access does not matter
(`OrderProbe`: reading 1025/1724/1023 first changes nothing). Across 2,000 pages 1,359 have a
NUMBER at slot 0 and exactly one is truncated. In the no-elision arm the truncated slot moves to
page 3, and pages 0–6 there carry **no global tag at all** — so the truncation is a side effect of
the lane's ENCODER running (`probes=717925`), not of a page being converted, and it is NOT the
elision × lane collision. The earlier "11-byte truncation" that was retracted as a two-variable
artefact is real after all, on a single-variable arm: 33 − 22 = 11.

**Not the mixed-population theory.** The parked note attributed `Type not known` to lazy pages
beside eager neighbours; with every route lazy it still fires, on one deterministic slot. Both
old symptoms had ONE cause each: the refusal was the policy predicate (fixed), the assertion is
this write-side truncation (open).

**100M cost check.** N3FULL1/N4FULL1 with the predicate in the query JVM (lane off, so only the
probe runs): hot sum 32.2 → 32.3/32.4 s, c6a hot 3.456/3.408 (rank 11; N1/N2 were 3.327/3.247,
rank 10 — the difference is q2 0.038 → 0.063 s-level noise), combined rank 3, 43/43 identical.
Cold sum 71.1 → 77.3/74.4: N3 followed three 1M loads (page cache), N4 is +3 s with no load in
between — a small cold cost is possible (one HOT blob read + metadata parse per reader) and not
separated from noise. `TrieLaneReadSeamTest` 18/18.

**Next (the lane campaign, in order):** (1) find the slot-0 truncation in the encoder — it is
per-page, one page per load, position moves with layout; compare the staged slot length against
the region/body write for the FIRST slot of a page written while the lane is engaged; the 1M
reproduction is `gate1mT.sh converted` + `BadSlotCensus <db>/clickbench 0 1024000` (10 s);
(2) then prerequisite 2, the framing decomposition (`price-the-frame-a-lever-requires`);
(3) only then re-gate the lane's storage claim at 100M. Storage at 100M: 63.14 GB, unchanged.

### 05:35 — named next experiment for the slot-0 truncation (prediction stated, not run)

`TrieLaneWriteDictionaries.ThreadProbes.reader()` opens a **read-only trx lazily on the flush
thread's FIRST probe** — i.e. in the middle of serializing a page, on the same thread, once per
thread per load. That is the only "once per load" event on the write side, and it matches the
signature: one page per load, its position moving with flush layout, the page itself needing no
global tag (the probe runs before the tag decision), and slot 0 — the first record staged —
losing its tail. Hypothesis: opening the reader (or the first dictionary probe's page decode)
reuses a thread-local scratch that holds slot 0's staged bytes. Experiment: print the thread and
a serialization-scoped page key when `trx == null` in `reader()` (or move the open to
`bindConfigured` time, before any page is serialized) and re-run `gate1mT.sh converted` +
`BadSlotCensus 0 1024000`. Prediction: the printed page is the truncated one (1 in `converted`,
3 in `convnoelide`); with an eager open the census reads 1,024,000/1,024,000.

### 05:36 — the experiment ran; per-thread hypothesis FALSIFIED, the signature sharpened

`sirix.asyncFlush.parallelism=1` → still exactly ONE bad slot (page 1); `=8` → still exactly ONE
(page 5, slot 0, now an OBJECT_NAMED_STRING: `IllegalStateException: Corrupted fused string
payload flag 12 for node 5120`). So it is not "once per probing thread" but **once per LOAD**, and
the page it lands on moves with layout (1 / 3 / 5). Sharper: the damaged slot 0 is **always 22
bytes long** whatever its true length (33, 33, 36 in the control) and always an exact prefix — a
fixed length, not "minus 11". Candidates: a slot-0 length/offset taken from a stale or shared
field on one page per load (a lazily initialised static scratch or a once-per-load event on the
write side; the lazy reader open is per thread and is therefore NOT it). Reproduction: 
`gate1mT.sh convpar8` (27 s) + `BadSlotCensus 0 1024000` (10 s) + `SlotBytesProbe <db> 5 2`.
Arms `convpar1`/`convpar8` added to `gate1mT.sh`. Stopped at 06:00 Berlin per instruction.

### 05:36 — one more single-variable arm: the defect needs BOUND dictionaries

`laneonly` (flag on, NO prepass → `TrieLaneWriteDictionaries` never binds, no encode line):
census **0 / 1,024,000 bad**. With `prebuilt` (prepass, flag off) also clean, the truncation
needs the write dictionaries bound and probing. So the once-per-load event is inside the bound
lane's life: candidates are `bindConfigured`/`publishTrieLaneTags` (`ProjectionIndexBuilder`
1444/1454/1545 — one of them fires once, mid-load, when the path classes first become known) and
the first probe's dictionary decode. Next step, ~1 h: log `recordPageKey` of the page under
serialization on the thread at each of those events and match it to the damaged page (1/3/5).

### 2026-09-03 05:45: the slot-0 truncation is FOUND and FIXED — converted 1M arm reads 1,024,000/1,024,000

Correlation from the last diag run: the damaged page (3) was the one serialized on
`ForkJoinPool-1-worker-7`, the thread that issued the load's FIRST dictionary probe. Mechanism:
`PageKind` used ONE thread-local, `SLOT_DATALEN_SCRATCH`, both as the serializer's per-slot length
table (`serializeKeyValueLeafPage`, ~1845) and as the decoder's `inMemDataLengths` target
(~1115, stamped at ~9169). The trie-lane encoder probes the write dictionaries from inside the
region encode; the first probe of a load pulls a dictionary page from disk and deserializes it on
the serializing thread, overwriting the length table of the page under serialization — slot 0 is
written with the dictionary page's first-entry length (22 B). Later probes hit the page cache, so it
happens exactly once per load. Explains every observation: once per load, fixed 22 B, exact prefix
(heap intact, length wrong), staged slot intact, page moves with layout, absent whenever the
dictionaries are not bound (laneonly/prebuilt clean).

Fix: decode gets its own `SLOT_DATALEN_READ_SCRATCH`. Witness on the live W rig: `gate1mT.sh convpar8`
(chunked + lane + prepass, flush parallelism 8) then `BadSlotCensus 0 1024000` →
**bad=0 across all kinds** (OBJECT_NAMED_NUMBER 705,208, OBJECT_NAMED_STRING 309,129, OBJECT 9,661,
ARRAY 1, JSON_DOCUMENT 1); the same arm had exactly one bad slot on every earlier run. Database
604,657,907 B vs 696.7 MB control. Both trie-lane prerequisites for the read side are now closed;
the `sirix.trieLaneDiag`-guarded prints stay (off by default). Remaining before re-gating the −11 GB
claim at 100M: the framing decomposition (prerequisite 2) and a JUnit witness for the aliasing
(serialize with a probe that deserializes on the same thread).

**05:48 disk note for the 100M re-gate:** the scratch volume has 29 GB free (641 GB, 96 % used); the
baseline `db100m` is 61 GB and must stay as the query-leg reference. A converted 100M build needs
≈55–63 GB beside it → free or relocate space first (1M arms under `agents/p2s1/gate1mT/`, old
`results-*` dumps; never `hits.json.gz`/`hits-1m.json.gz`). Stopped 05:49 on the 06:00 instruction.
