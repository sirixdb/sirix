# What stands between 69.6 GB and mid-table (~15 GB)

Written 2026-08-31 04:20, at the end of a night that took the 100M ClickBench database from 131.9 GB to
69.6 GB (−47.2 %) while making queries 1.50× faster cold and 1.69× hot with byte-identical answers. The
goal was the **middle of the ClickBench leaderboard**, and that is **not met**: the median `data_size`
across 132 published systems on `c6a.4xlarge` is **15.3 GB** (q1 14.8, q3 31.9), so mid-table means
roughly a further **−78 %**. This document says exactly where the remaining bytes are and what would have
to change, so the next attempt starts from arithmetic rather than from intuition.

## The measured budget

At 100M rows (`clickbench-100m-campaign-20260831-0257`, commit `f43a6a3a7`), by page class:

| class | bytes | share | what it is |
|---|---|---|---|
| KeyValueLeafPage | 49.94 GB | 71.9 % | the node trie's leaves |
| OverflowPage | 17.45 GB | 25.1 % | projection column segments, dictionary blocks, overlong records |
| HOTLeafPage | 1.93 GB | 2.8 % | the HOT index |
| **total** | **69.51 GB** | | **695 B/row** |

The leaf class decomposes (measured at 1M, per record; ×113.6 records per row):

| section | B/record | ≈ B/row | ≈ GB at 100M |
|---|---|---|---|
| **region: string** | 1.52 | 173 | **17.3** |
| region: objKeyNameKey | 0.53 | 60 | 6.0 |
| region: numberZoneMap | 0.38 | 43 | 4.3 |
| region: number | 0.36 | 41 | 4.1 |
| region: sketch + ordinal | 0.16 | 18 | 1.8 |
| encoded body (all sections) | 0.95 | 108 | 10.8 |
| header + bitmap | 0.16 | 18 | 1.8 |

## The two facts that decide everything

**1. The same values are stored twice.** The trie's string region (17.3 GB) and the projection's
dictionaries inside the overflow class (~17.4 GB) hold the same URL/Title/Referer/SearchPhrase bytes.
A resource-wide dictionary named by both would store them once — measured distinct set 7.75 GB raw,
about 2.7 GB written in the best shape actually built and weighed tonight (front-coded, no forward
index, in-record LZ77). See `P2_GLOBAL_DICTIONARY_DESIGN.md` §18: the gate for that failed at 27.4 B/row
against a 17.3 acceptance, but the failure was in the *acceptance*, not in the mechanism — the design
had priced the dictionary with the occurrence-weighted mean value length instead of the distinct-weighted
one. Corrected, the projection half is worth about −5.6 GB and the trie lane about −15 GB.

**2. Everything else is per-record overhead, and there are 106 records per row.** A ClickBench row is one
JSON object of 105 fields, stored as ~106 fused records. Every one pays a directory entry, a name-key,
an ordinal, zone-map participation and its share of the body. That is why 10.8 GB of body and 12 GB of
non-string regions exist at all: not because any single field is expensive, but because the row is
multiplied by 106 before the storage engine ever sees it. ClickHouse stores the same corpus as 105
columns of 100M values in 15.3 GB — 153 B/row — with no per-value metadata whatsoever.

## What each route is actually worth

| route | measured or derived | lands at | status |
|---|---|---|---|
| P2 projection half (global dictionary) | −5.6 GB measured | 64 GB | gate failed as specified; acceptance must be restated ~27 B/row |
| P2 trie lane (string region names ids) | −15 GB derived | ~49 GB | needs a per-transaction resolver; never built |
| overflow payload compression | −4.7 GB measured | −4.7 anywhere | built; **now ships ON by default** — the opt-in verdict recorded here was a small-scale one, see `PageKind.OVERFLOW_PAYLOAD_COMPRESSION_ENABLED` |
| L1 bigger leaves (2^17 slots) | −4.7 to −8.1 GB measured | | screened and ruled out as a duplication fix; still valid for framing/leaf count |
| **all of the above together** | | **~40–45 GB** | still 3× mid-table |

**No combination of the levers on the table reaches 15 GB.** That is the finding, and it is arithmetic,
not pessimism.

## What would reach it

The gap is structural: 106 records per row against ClickHouse's zero. Three candidates, in order of how
much they change:

1. **Fewer records per row.** Store a JSON object's scalar fields as ONE record with an internal layout,
   rather than one fused record per field. Per-record overhead then divides by ~100. This is the
   "one record per JSON object" data-model change parked earlier in `STORAGE_AND_SPEED_PLAN.md` §8 —
   it touches the node model, the cursor API, versioning granularity and every index, and it is the only
   candidate that addresses the multiplicand instead of the multiplicands' costs.
2. **Column-major leaves.** Keep one record per field logically, but store a leaf's records grouped by
   field with a single shared descriptor per field rather than per record — the PAX regions already do
   this for VALUES; the remaining per-record bytes are directory, name-key, ordinal and body framing.
   Less invasive than (1), attacks ~12 GB of the 22 GB of non-string leaf bytes.
3. **The dictionary work above (P2), corrected.** Worth ~20 GB combined and independent of (1) and (2);
   it should be done regardless, because it also removes the duplication that makes the fat columns
   expensive in both structures.

A realistic sequence is 3 → 2 → 1, with (3) alone taking the database to roughly 45–50 GB and each of
(2) and (1) needing its own campaign with a rebuild-and-leg gate. Mid-table is reachable only with (1).

## Constraints any attempt inherits

- **Query latency may not regress per query — and it must be measured at the target scale.** Tonight's
  overflow-compression lever saved 4.7 GB, made cold scans 1.24× faster, and shipped disabled because two
  queries lost 85 % of their hot time. That verdict was later overturned by the same measurement at 100M,
  where the lever is faster, not slower: a compression lever's query cost can change SIGN with scale, so a
  small-scale regression neither condemns nor a small-scale win acquits one
  (`PageKind.OVERFLOW_PAYLOAD_COMPRESSION_ENABLED` owns both numbers).
- **Versioning is the product.** Every byte here is versioned and reconstructible; a column store's
  numbers are not a like-for-like target, and a change that improves storage by weakening
  point-read, history or reconstruction cost is not a win.
- **Measure written bytes, never a ratio, and never extrapolate a compression ratio from two scales**
  (`P2_GLOBAL_DICTIONARY_DESIGN.md` §18.4 records both traps and how each was found).
