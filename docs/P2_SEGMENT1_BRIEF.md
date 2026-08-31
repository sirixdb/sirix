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
