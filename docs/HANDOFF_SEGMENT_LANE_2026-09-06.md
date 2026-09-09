# Handoff: ClickBench top-10 on the segment lane (2026-09-06)

Written for the next agent (Codex) taking over branch `codex/clickbench-port-rebased-20260827` at
`be5e8232f`. It states the goal and its metric, where we stand, the one mechanism that costs us, the
lever queue ordered by what each is worth, the rig, the rulings in force, and the traps that cost
this campaign whole days. The rig lives in `bundles/sirix-query/bench/clickbench/rig/` (its README is
the operating manual); the design spec is `docs/SEGMENT_DICTIONARY_DESIGN.md`.

## 1. Goal and metric

**Top 10 on the ClickBench leaderboard for queries at 100M rows**, scored the way the site scores:
the c6a.4xlarge board, hot metric = min(try 2, try 3) per query, score = geometric mean over 43
queries of `(0.01 + ours) / (0.01 + best on the board)`. Rank 10 is a geomean of **3.35**, i.e.
**Σ ln ≤ 51.99** over 43 queries. `rig/rank.py` reproduces this exactly (verified against the
site's `index.html`); the board snapshot is from 2026-09-02.

Consequences that shape every decision:

- **Seconds are not the metric.** 686.8 s total was rank 81; 32.13 s was rank 10. A lever is worth
  its Δln, summed over the queries it touches. Score every leg before claiming progress.
- **The +0.01 s offset** on both sides means 0.05 s against a 0.000 s best still costs ln 6 ≈ 1.8 —
  SEG7T's q17 measures exactly that (0.037 s against a 0.000 s best, 1.55 ln). Measured on SEG7T,
  **20 of the 43 queries already answer in under 100 ms and still carry 22.44 of the 58.96 ln**;
  the 16 under 50 ms carry 15.93 ln between them. See §4 for the largest measured contributions;
  `python3 rank.py SEG7T` prints the fourteen largest C6A hot contributions.
- Only 3-try legs score, and `rank.py` additionally ranks only a curated leg whose provenance states
  the publication regime — `suite100m.sh 3` now collects a *steering* leg, which never ranks (§5).
  Never compare legs of different run shapes, and never a `-Dsirix.projDiag=true` run.

Standing secondary target: ~50 GB storage at 100M (met: 48 GB); long-term ≤ 30 GB
(`docs/ROADMAP_TO_30GB.md`).

## 2. Where we stand

**Update, 2026-09-09:** the committed `query-SEG7T.json` supersedes every snapshot below,
including SEG6T. The 2026-09-08 string-decode change is correctness-only and adds no accepted
performance result: SEG6T predates it, SEG7T's head is the first scored leg to contain it, and a
single leg attributes nothing to it, so its effect remains unverified pending measurement resolution;
[the string-decode report](CLICKBENCH_STRING_DECODE_2026-09-08.md) records that lane's delivery
constraints.

| leg | build | database | C6A hot geomean | rank / 140 | Σln |
|---|---|---|---|---|---|
| N1FULL1 (2026-09-03) | global dictionary + prepass | `db100m-ovf`, 49.70 GB — **deleted** | 3.327 | **10** | 51.69 |
| SEG3T (2026-09-06, measured) | segment lane | `clickbench-seg100m-20260905-2328`, 48 GB, 148 segments | 19.943 | 74 | 128.69 |
| SEG3TB (SEG3T with the served q21/q22/q28 spliced in) | segment lane + `a3aed07ec`…`417c62ead` | same | 9.516 | 52 | 96.88 |
| projection: SEG3TB + q17 (`86d839058`) + q27 (`be5e8232f`) | `be5e8232f` | same | ≈ 7.89 | ≈ 42 | ≈ 88.8 |
| SEG4T (2026-09-07, measured) | segment lane at the handover (`54b0a059b`) | same | 5.179 | 17 | 70.72 |
| SEG5T (2026-09-07, measured) | segment lane at `de2724c5c` | same | 4.714 | 16 | 66.68 |
| SEG6T (measured at or before 2026-09-07T23:24Z) | `8df0532d6`, attributed by the `sirix-cb-score-3` investigation report; absent from the leg artifact | `clickbench-seg100m-20260905-2328`, attributed by that report; absent from the leg artifact | 4.373 | 16 | 63.44 |
| **SEG7T (2026-09-09T10:55:40, measured)** | `465f20894`, recorded on the leg | campaign 100M database; the leg names no identifier | **3.940** | **15** | **58.96** |

**SEG7T is where we stand.** C6A hot geomean **3.939639**, **rank 15 of 140**, Σln **58.956827**,
total hot suite time 27.890 s. Rank 10 (Σln ≤ 51.99) needs ≈ **−6.97 ln** from here
(58.956827 − 51.99 = 6.966827). `python3 rank.py SEG7T SEG6T SEG5T SEG4T` reproduces those four rows
from the committed timings. §4 is drawn from this leg, and it is the only current standing
this document states.

SEG6T's provenance attribution comes from the `sirix-cb-score-3` investigation report, not from the
leg JSON: its `rig.regime` still accurately states which capture fields were absent. Its
collection-time bound above comes from the recording commit rather than the old template date.
SEG6T predates the string-decode source changes and therefore measures none of them. SEG5T remains
the historical `de2724c5c` snapshot at 66.676 ln.

Every record above SEG7T predates the 2026-09-08T03:58Z MSR cap change. Do not compare those
scores directly with capped steering legs. SEG7T is the first publication leg collected under the
cap, and unlike them it records its envelope: the pinned 50 W gate held, and one MMIO platform limit
moved 76 W → 45 W mid-run, which its `rig.regime` states. Even later legs need observed power and
thermal context: the calibration's MSR setting was 50/50 W, but its MMIO limits varied. The
[rig README](../bundles/sirix-query/bench/clickbench/rig/README.md) owns the protocol and links
the retained power audit.

The calibration's **1.345-ln** minimum detectable effect is an 80%-power estimate from
**ten A/A pairs**. It is neither the resolution of one pair nor a hardware floor transferable to
§4's historical observations. The **3.023-ln** range instead describes the twenty individual legs.
Neither figure qualifies a historical lever's single-pair delta. Firstmate owns that separate
requalification; retain repeated single-query diagnostics as mechanism evidence, with their stated
limits. A suite score change alone cannot attribute a gain to one of several intervening levers.

SEG5T is **4.041 ln** better than SEG4T, and the two levers §4 records as acted on account for
3.718 of that: q25 **−1.877 ln** and q28 **−1.840 ln**. SEG4T stays in the table as history — it is the leg §4
was drawn from until SEG7T superseded it, and the earlier arithmetic ≈ 88.8 projection it replaced
is above it.

SEG5T measured `de2724c5c`, which **predates the q16 and q35 work on this branch**, so it scores
neither q16 nor the q35 fold; its q35 row is the unrewritten query. SEG6T is the first leg past
that point — §4's q35 row says what SEG6T's own row does and does not attribute. This branch's
shared group-aggregation change lands on top of `aa4d81d54`, after SEG6T; SEG7T is the first leg
whose head carries it. One leg attributes no Δln to it, so it still claims no speedup of its own,
and its correctness and measurement standing are in
[shared aggregation](CLICKBENCH_GROUP_AGGREGATION_2026-09-08.md).

How we got here: on 2026-09-03 a global-dictionary build (`db100m-ovf`) scored rank 6 — but it
needed a value **prepass** over the corpus to build its dictionaries. The user ruled the prepass out
("it has to be generic"; DuckDB/ClickHouse/Umbra scope dictionaries to a block and do no prepass).
The **segment lane** replaced it: one sealed dictionary per segment of ~100k–1M rows, built
incrementally during the load, nothing global. It won storage (1M: 923 → 445 MB; 100M: 63 → 48 GB)
and initially lost the query goal (rank 81) because every string-keyed group/operand path
canonicalised the whole column. Fourteen commits since (`36406e43a`…`be5e8232f`) rebuilt that
path; the rest of this document is what is left.

## 3. The design in one paragraph

A projection column of kind `COLUMN_KIND_STRING_SEGMENT = 8` stores per row a packed **cell**
`(segment << 32) | id`, where `id` is the value's index in that segment's dictionary
(`GlobalValueDictionary` — the class name is legacy; there is one instance per segment, addressed
through a `headerKey` recorded in the NamePage directory). Leaves hold 1,024 rows and are cut at
segment boundaries; `SegmentBoundaries` closes a segment on a byte budget
(`sirix.segmentDict.budgetBytes`, 64 MB) or a leaf count (`sirix.segmentDict.maxLeaves`, 2^17
record pages). The 100M DB has 148 segments. Each dictionary is **sorted**, so within a segment
position order is collation order — which is why a total order over the column is a **merge of 148
sorted runs**, not a sort of 18.3M strings (`SegmentValueMerge`, `SegmentRunCursor`). Equal strings
in different segments have different cells; a group key or an aggregate operand therefore needs
**canonicalisation** to a column-wide id (`SegmentGroupCanonicaliser`). Its cost must be measured
per query: q16 spends more hot CPU on table/spill work, and q35 has no string canonicalisation (§4).

Key code (paths under `bundles/`):

| what | where |
|---|---|
| query planning + serving | `sirix-query/src/main/java/io/sirix/query/scan/SirixVectorizedExecutor.java` (29.7k lines) — `sealSegmentOperand` ≈ l. 19163/19206, `segmentLengthTables` ≈ 12199, group-agg plan rules ≈ 15340–15416, `GroupOrderPlan.resolve` ≈ 21083, `segmentWalksOnWorkers` ≈ 19406 |
| canonicalisation | `sirix-core/…/index/projection/SegmentGroupCanonicaliser.java` |
| merge of sorted runs | `SegmentValueMerge.java`, `SegmentRunCursor.java` |
| dictionary + read view (`readView`, `fillLengthTableByPosition`, `valueAsString`) | `GlobalValueDictionary.java` |
| segment cut policy / incremental sealing | `SegmentBoundaries.java`, `SegmentDictionaryLane.java` |
| leaf-level group scan (length tables, present cells) | `ProjectionColumnGroupScan.java` |
| handle + per-handle memos (`stringLengthTables`) | `ProjectionIndexRegistry.java` (`Handle`), `ProjectionIndexCatalog.java` |
| diagnostics | `-Dsirix.projDiag=true` prints `route=`, `[proj] groupAgg decline: …`, `[lengthTable] col= mode= segments= memoHits= built= ids= ms=`, `[topk-bounds] kind= positions= segments=`, `[proj] canonical lanes: sourceLongs= allocatedLongs= reusedEmptyLeaves= allocatedPresenceWords= reusedEmptyPresence= mapRanges=` (whole-column rewrites only; counters are column totals summed over the `mapRanges` parallel mapping ranges, but each range shares its own zero lane, so above one range the empty-lane reuse is per range rather than column-wide), `segment lane: …`; counters `projectionStringLengthTableBuildCount()`, `projectionStringLengthTableMemoHitCount()`, `segmentOperandSealCount()` on the executor |

Tests that pin the lane: `SegmentLengthLaneQueryTest`, `AnyKGroupsSegmentKeyRewriteTest`,
`AnyKGroupsGlobalKeyRewriteTest`, `GroupTopKDifferentialTest`, `SegmentOrderedLimitQueryTest`,
`ClickBenchQ21Q22SegmentRouteEvidenceTest`, `ClickBenchQ16Q35RouteEvidenceTest` and
`ClickBenchStringDecodeRouteEvidenceTest` (sirix-query); `SegmentLengthLaneGroupScanTest`,
`RankTableReadViewTest`, `SegmentBoundariesTest`, `SegmentCellRoundTripTest`,
`SegmentTopKBoundsTest`, `ValueDictionaryComparisonTest`,
`ProjectionBulkLoadFenceChunkBoundaryTest` (sirix-core).
The `…RouteEvidenceTest`s are the end-to-end witnesses that a route is actually taken, at a size CI
can afford: `Q21Q22` for the string extremum, `StringDecode` for the whole-column, predicated and
transformed group shapes, `Q16Q35` for the numeric composite keys.
`sirix-query`'s test JVM forwards `sirix.projDiag` (build.gradle ≈ l. 201), so a declined route in
a test is one `-Dsirix.projDiag=true` away from its reason.

## 4. The lever queue

**Ordering key: the ln the lever actually buys on the board, summed over the queries it touches.**
For one query, with `t` its current C6A hot seconds and `k` the achievable speedup on it:

```text
payoff = ln( (0.01 + t) / (0.01 + t / k) )
```

and the key is `Σ payoff` over the touched queries. That one number is the only thing that orders
§4a. `k` and the touched-query count `N` are its **inputs**, shown beside it; neither ranks anything
on its own.

**Why the exact form and not `N · ln(k)`.** The 0.01 s constant is the board's anti-gaming floor and
applies to both sides of every ratio, so a query already answering in tens of milliseconds cannot
return ln(k) however large `k` is. `Σ ln(k)` is only this expression's large-`t` limit: on q28's
6.039 s a 1.5× speedup pays 0.4046 ln against ln(1.5) = 0.4055, but the same 1.5× spread over §4c's
twenty sub-100 ms queries pays 5.639 ln, not the 8.109 that limit would promise. Effort pays on the
slow tail, and the key says so by construction instead of in a footnote. The exact form also vanishes as `k → 1`, which a
product like `k × N` does not — a lever with no achievable speedup must key at zero, not at `N`.

**Per-query ln contribution is NOT the ordering key.** A query's contribution is
ln((0.01 + ours) / (0.01 + board best)): it measures how far ahead the board leader is on that
query — a fact about *them* — not what a fix is worth, which is a fact about *us*. Ranking levers by
it already cost this campaign a day. The contribution stays visible as data in §4b, because it
sizes §2's remaining gap and bounds a query's payoff, but it must not order work.

**`k` is derived from a committed hot profile of the query the lever serves**, by Amdahl's law on
the share the lever actually *removes* — not the share it merely touches. A lever with no such
profile has no `k`, therefore no key, and cannot be ranked; it is listed unranked in §4c, and
profiling it is the work.

**Every value below is refreshed from SEG7T** — `python3 rank.py SEG7T`, `[C6A] hot` block, from the
committed `rig/legs/query-SEG7T.json`: geomean 3.939639, rank 15, Σln 58.956827. SEG4T used to fill
this table; it is three legs stale and superseded everywhere here. A single leg still cannot
attribute a delta to a mechanism, so the rescored rows in §4b report what a leg measured, not what a
commit caused.

**2026-09-09 — the aggregate family is deprioritized.** The generic shared
group-aggregate/numeric-group-by candidate is a
[MEASURED NEGATIVE](CAMPAIGN_PROGRESS.md#2026-09-09--measured-negative-shared-group-aggregation):
over 12 prespecified pairs its benefit on the requested q7–q18 and q28–q42 family is **+0.004791 ln,
nominal 95% interval [-0.512162, +0.521744]** — indistinguishable from zero. The mechanism is
reverted and the paired evidence retained. Read it against PR 1201, whose *per-cluster* dense group
index moved six of that same family in one leg (q31 3.013 → 0.908 s, q13 2.786 → 1.181, q32
6.743 → 4.236, q28 8.982 → 6.039, q18 3.914 → 2.972, q16 2.027 → 1.500): the per-cluster work moved
the family, and a generic path layered on top of it moved nothing measurable. The readily available
gain in this seam therefore appears already taken. That is a measured negative for one candidate,
not a proof the seam is exhausted — but it is why every aggregate-family lever ranks below the
non-aggregate ones here, and why none should be reopened without a fresh profile naming a cost the
per-cluster work did not already remove.

**2026-09-09, later — one such profile was taken, and the lever it named paid.** A 100M profile of
the seven high-cardinality `GROUP BY … ORDER BY count DESC LIMIT` queries at `b815d459d`
([`PROFILE.md`](../bundles/sirix-query/bench/clickbench/rig/evidence/hicard-groupby-20260909/PROFILE.md))
found q16/q18/q32 rescanning the input in 2/4/7 hash-range passes because the shared quarter share
capped their tables at 1.75 GiB of a 14 GiB heap. Planning a bounded top-k aggregate against three
quarters of the headroom instead (`GroupTableSpill.boundedGroupBudget`; arithmetic in
[`PASS_BUDGET.md`](../bundles/sirix-query/bench/clickbench/rig/evidence/hicard-groupby-20260909/PASS_BUDGET.md))
took them to 1/1/2 passes and measured **+0.409 ln on the seven-query family over 12 prespecified
pairs, 95% interval [+0.349, +0.469]**; the whole-suite score change (+0.290 ln) is **UNRESOLVED**,
its interval spanning zero. Removing five of q32's seven passes bought 11.8 % of its time, not
six-sevenths: the per-group table work survives every pass, and the profile puts it — lookup, merge
acquisition, spill/copy — as the largest shared cost left on q32/q18/q16/q31. That is the fresh
profile the note above demanded, so the composite group table lever is re-ranked on it as §4a
row 1 (N = 4; the ceiling, the one removable-share assumption and the payoff arithmetic are in the
row), which supersedes "every aggregate-family lever ranks below the non-aggregate ones". No
publication leg was collected, so SEG7T remains the scored basis of every row in §4b. Result,
remaining reference gap against DuckDB, the parked probing prototype and the disk-spill scope that
was NOT built:
[`RESULT.md`](../bundles/sirix-query/bench/clickbench/rig/evidence/hicard-groupby-20260909/RESULT.md).
The same work surfaced a pre-existing wrong-results defect — sparse `SUM` under
`order by … descending` places the all-missing groups first — which is documented and left
non-passing in
[`SUM_ORDERING_DEFECT.md`](../bundles/sirix-query/bench/clickbench/rig/evidence/hicard-groupby-20260909/SUM_ORDERING_DEFECT.md);
the 43-answer gate cannot see it. Fixing it is the lane's immediate next dispatch once the
bounded-allowance change lands — a wrong-results fix with its own review, before the probing
prototype in §4a row 1.

### 4a. Ranked levers

| # | key = Σ payoff (ln) | k (input) | N (input) | lever | profile the k comes from |
|---|---:|---:|---:|---|---|
| 1 | **0.721** (1.619 ceiling) | q32 1.28 (1.78 ceiling), q18 1.20 (1.50), q31 1.12 (1.27), q16 1.20 (1.50) | 4 | **Composite group table / spill path** — per-group table acquisition (lookup, merge acquisition, spill/copy), the largest shared cost the 2026-09-09 profile leaves on the four composite-key queries after the pass-count change; the parked probing prototype (`1703ebe2d` on `fm/sirix-cb-hicard-probing-prototype`, unmeasured) is the candidate, and its first step is a re-profile on the landed head. Aggregate family, ranked here on that profile — the fresh cost the §4 note demanded. The sparse `SUM` ordering fix (note above) is dispatched before it. | [`PROFILE.md`](../bundles/sirix-query/bench/clickbench/rig/evidence/hicard-groupby-20260909/PROFILE.md) at `b815d459d`, full-suite hot tries: lookup samples q32 **43.8 %**, q18 **33.5 %**, q31 **21.1 %**, q16 **33.5 %**. `k_max = 1 / (1 − share)` assumes the whole lookup share removable and is a ceiling: 1.78 / 1.50 / 1.27 / 1.50. **Working assumption: half the lookup share is removable** — the only recorded split (q32) puts 25.0 of its 43.8 points in worker-side acquisition, the part append-without-probing can skip, and 18.8 in the exact merge acquisition every record must still pay; half rounds the worker side down for the offsets [`RESULT.md`](../bundles/sirix-query/bench/clickbench/rig/evidence/hicard-groupby-20260909/RESULT.md) names (extra partial records, merge traffic, larger resident state). Working `k = 1 / (1 − share/2)`: 1.28 / 1.20 / 1.12 / 1.20. `t` is the candidate hot mean from RESULT.md — 3.615 / 2.412 / 0.913 / 1.427 s, the post-pass-count state the lever starts from, not SEG7T (no publication leg was collected). Payoff per query 0.2464 / 0.1825 / 0.1102 / 0.1819, Σ **0.721** (ceiling 0.5741 / 0.4059 / 0.2341 / 0.4045, Σ 1.619). The order of this table hinges on the assumption: at one third removable the key is 0.465, below row 2. **Provenance:** the lookup shares are from the `b815d459d` profile, which PREDATES the pass-count change; that change removed rescans, not per-group table work, so the shares are a reasonable first estimate — but the probing lever's first step is to re-profile on the landed head. q16's earlier capture (`identityMatches` 33.3 % self, table and spill frames 53.9 % inclusive) is in [Dependent numeric group keys](DEPENDENT_NUMERIC_GROUP_KEYS.md). |
| 2 | **0.514** | q22 1.52, q21 1.13 | 2 | **Reuse the existing predicate row mask in the numeric aggregate arm** instead of re-evaluating the mask tree there. Not an aggregate-family lever: what it removes is a repeated predicate evaluation, not group aggregation. Must cover NOT/missing semantics, tails, multiplicity and stable document-order ties, and keep the path for callers without masks. [Evidence and remaining target](SEGMENT_LIKE_GROUPS.md). | q22: of 12,137 hot CPU samples, 68.9 % is predicate-mask evaluation, split 34.46 % in `rowKeepMasks` and **34.42 % repeated in `aggregateByGroupNumericFlat`** — the repeated half is what the lever removes. q21: 12.0 % + **11.6 % repeated**. |

### 4b. Per-query evidence (SEG7T)

Rows are grouped by lever and ordered by their largest member's SEG7T contribution. **That order is
data, not priority** — §4a is the priority order. The 14 largest SEG7T contributions, in order, are
**q32, q25, q22, q33, q5, q34, q16, q10, q31, q2, q39, q17, q11, q28**.

Columns: C6A hot ln; ours s / board best s.

| q | ln | ours / best | query shape | lever |
|---|---|---|---|---|
| q32 / q31 | 2.502 / 1.888 | 4.236, 0.908 / 0.338, 0.129 | `(WatchID, ClientIP)` numeric composite, 100M groups | **PR 1201 has since scored both**: q31 3.013 → 0.908 s and q32 6.743 → 4.236 s, the largest single-leg movement in the family. Not a segment-lane problem. The 2026-09-07 hot profiles **split the two rather than confirming one shared cause**: q31 is dominated by windowed column reads/decode (55 % CPU), while the measured memory stalls in grouping are q32's — so the historical "memory-bound group hash table, ~27 % of suite CPU" reading is q32's, not q31's. Three probe levers already failed; the idea left is fewer probes (pre-aggregate per leaf / radix partition). Skipping non-owning-pass aggregate folds removes 599,984,982 q32 folds (work count); paired-profile CPU falls about 5 %. Neither is a scored delta-ln. Probes are unchanged. Adjacent duplicates are negligible. See [measurements and candidate falsifiers](NUMERIC_COMPOSITE_GROUP_MEASUREMENTS.md). **Both profile shares predate PR 1201** and were taken at 3.63 s and 7.34 s hot; at 0.908 s and 4.236 s they no longer describe these queries. The 2026-09-09 re-profile at `b815d459d` supersedes them and ranks both under §4a row 1 (q32 43.8 % lookup samples, q31 21.1 %). |
| q25 | 2.322 | 0.092 / 0.000 | `ORDER BY SearchPhrase LIMIT 10` | **Profiled, acted on (`0a22879f6`…`663557b11`) and since rescored by SEG5T: 0.611 s → 0.085 s hot, −1.877 ln; SEG7T reads 0.092 s.** The merge is not involved at all — `SegmentValueMerge`/`SegmentRunCursor` take zero samples, so the "column materialisation before the merge" guess above was wrong, and the route was already `sorted-scan`. The 0.6 s was the top-K plan: a segment cell means nothing outside its own segment, so `planTopK` declared the key unboundable, all 96,459 admitted leaves of 97,737 carried unknown bounds, none was skipped and 13,172,392 candidate rows were decoded for a ten-entry heap. `SegmentTopKBounds` now bounds a leaf by its **segment's** first/last collation position — advanced once past a directly excluded literal, at most two position reads per segment, no prepass — ranks those ≤ 148 cells once and orders and cuts through dictionary values. 100M diagnostic: evaluations 96,459 → 8,191, 88,268 leaves skipped, candidates 13,172,392 → 1,272,084, no restart. Whole-segment is the granularity ceiling without the merge, whose implementation is preserved **on the original machine only**, at commit `0350230d2b9eb1f28a905de7afc46715875e5cd0` — branch `fm/q25-segment-merge`, aliased as `fm/q25-segment-merge-preserved-0350230d` in the checkout that produced it. It is on **no remote** (`git branch -r --contains` is empty; GitHub answers 404), so a fresh clone cannot reach it and it must be fetched from that machine if it is ever wanted again. An ordering with **no** predicate on the sort key is deliberately refused (its all-present proof would be a whole-column BODY pass — the no-prepass ruling); the reasoning is in `SegmentTopKBounds.create`'s javadoc. SEG5T is that scored leg. |
| q22 | 2.299 | 0.299 / 0.021 | LIKE predicates + string `MIN` and distinct UserIDs | **Its duplicate-mask cost is §4a row 2, the ranked predicate-mask lever. Advertised MIN/verdict lever already spent:** verdict sharing (`921c3f811`) and `MIN` over canonical collation ranks already serve this query (since `a3aed07ec`). Hot 100M profile (initial diagnostic): **68.9% of CPU is duplicate predicate-mask evaluation; COUNT(DISTINCT) is 0.13%** — the `b00ed9e4` capture re-reads the same shape at 68.8% and 0.23%. Its hot min(tries 2,3) read 0.278 s in the earlier capture and 0.316 s at `b00ed9e4` — a raw range across two unscored diagnostics, **no timing claim in either direction**. Mask reuse is deferred as its own task; see [evidence and remaining target](SEGMENT_LIKE_GROUPS.md). |
| q33 / q34 | 2.092 / 2.032 | 2.152, 2.027 / 0.257 | `GROUP BY URL ORDER BY c LIMIT 10` on an 18.3M-distinct column | Aggregate **inside the merge**: counts per cell are known per segment; the merge emits canonical groups in order and a bounded top-K needs no hash table. Same lever serves q12 (1.404 ln, 0.686 / 0.161) and q5 `COUNT(DISTINCT SearchPhrase)` (2.047 ln, 0.656 / 0.076 — the 5th largest SEG7T contribution; the distinct count is the merge's output length). No hot profile share has ever been committed for this lever, so it is unranked in §4c. |
| q16 / q14 / q18 | 2.007 / 1.318 / 1.248 | 1.500, 0.711, 2.972 / 0.193, 0.183, 0.846 | composite keys `(UserID, SearchPhrase)` etc. | **q14 collapsed 2.32 ln below its projection and is no longer a lever**; q16 leads the group. **The canonicalisation-first lever is refuted as q16's dominant cost**: in the 10,597-sample hot 100M CPU capture, canonicalisation is 21.8 % (counting `SegmentGroupCanonicaliser` *and* its parallel `SegmentValueMerge` dictionary-merge workers — the canonicaliser class alone reads as a misleading 2.2 %), while table and spill frames are 53.9 % inclusive with `identityMatches` alone at 33.3 % self time. The earlier proposal — a cell is already a unique id **within** a segment, so aggregate per segment on the packed cell and merge the per-segment tables by canonical id — therefore targets the minority cost; investigate the table/spill path first. If segment-local preaggregation is built anyway, `a-segment-scoped-id-is-a-preaggregation-not-a-group` still binds: **all** partial groups must be canonicalised and merged **before** top-K pruning, because segment-local winners alone can lose the global winner. That profile is q16's alone; the 2026-09-09 re-profile at `b815d459d` adds q18 (33.5 % lookup samples) and q14 (13.4 %, string-merge dominated) and ranks the table/spill path as §4a row 1. No q16 speedup or new score is claimed. Evidence: [dependent numeric group keys](DEPENDENT_NUMERIC_GROUP_KEYS.md). |
| q10 / q11 / q13 | 1.896 / 1.533 / 1.317 | 0.223, 0.166, 1.181 / 0.025, 0.028, 0.309 | `COUNT(DISTINCT UserID)` grouped by MobilePhoneModel / `(MobilePhone, MobilePhoneModel)` / SearchPhrase | Served; the group key is a segment string and the aggregate is a distinct count per group. Under SEG7T only q10 (#8) and q11 (#13) remain in the top 14 — q13 has fallen to ≈ #23 after PR 1201 took it 2.786 → 1.181 s — so the distinct-count representation is worth a profile before any of it is redesigned; it has none today and therefore no ranked entry in §4a. q10 was **not** profiled in the q21/q22 follow-up; do not infer its bottleneck from those queries. |
| q39 | 1.759 | 0.170 / 0.021 | 5-column group with `CASE` on Referer/URL + tight `WHERE` | **Collapsed 2.16 ln below its projection (1.44 s → 0.157 s); SEG7T reads 0.170 s and 1.759 ln, the 11th largest contribution.** It is unprofiled, so it has no ranked entry in §4a. If it is ever picked up again: the predicate keeps few rows, so look at what runs before it. |
| q28 | 1.532 | 6.039 / 1.297 | `REGEXP_REPLACE(Referer, …)` group + `AVG(STRLEN(Referer))` + `MIN(Referer)` | **Landed** (`095be6eb3`): the regex now runs **once per distinct value per segment** (≤ dictionary size, not 100M rows), STRLEN comes from the length table (`be5e8232f`), and `MIN(Referer)` folds the merge's canonical ranks rather than comparing strings. Mechanism, retention bounds and profiling evidence: `docs/SEGMENT_TRANSFORM_GROUPS.md`. That evidence is a 100M **diagnostic** hot time of 60.728 s → 8.616 s, and SEG5T has since scored it: **57.9 s → 9.178 s hot, −1.840 ln**. PR 1201's dense group index moved it again, 8.982 → 6.039 s, which is the SEG7T figure in this row. |
| q36 / q37 / q38 | 1.308 / 1.299 / 1.327 | 0.064, 0.045, 0.039 / 0.010, 0.005, 0.003 | few-row predicates on URL/Title | Already predicate-first (`5026b239e`); all three now answer in 39–64 ms and have fallen out of the top tier (projected 3.08/2.62, SEG7T ≈ 1.31). The residue is fixed cost per query. The ln shown is what **matching the current board best** would recover — ≈ 1.3 ln per query, not 1.3 ln for the three together. It is not a zero-latency ceiling: rank.py's +0.01 s offset applies to *both* sides of the ratio, so driving our own time to 0 s would recover ≈ 2.00 / 1.70 / 1.59 ln. No profile attributes that residue to a shared fixed cost, so it is unranked in §4c rather than a lever. |
| q35 | 1.114 | 0.395 / 0.123 | `GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3 ORDER BY c DESC LIMIT 10` | **Rescored down to 1.114 ln by SEG7T, from SEG4T's 2.302** — and **not** a second copy of q31/q32: it has no string canonicalisation, its measured hot cost is redundant numeric key evaluation and group-table work, and whether it is memory-bound is **unresolved** (CPU stacks do not establish hardware stalls). Three derived key columns are pure integer offsets of the first, so the bounded same-column integer-offset rewrite (`87a5f04be`) groups on one numeric key and restores the translated keys only for winners: key components 4 → 1, exact identity lanes **5 → 0**, table stride **9 → 3** longs, leaf visits unchanged at **97,737**. Separately measured 100M diagnostic last-ten median **0.896 s → 0.3715 s**, against a **0.881 s** repeated parent baseline, with byte-identical ordered results. Those are unscored single-query medians and explicitly **not a scored Δln**: SEG7T's 0.395 s / 0.123 s and 1.114 ln are this row's current scored basis, superseding SEG4T's 1.319 s and 2.302 ln. SEG5T does rescore q35, at 0.912 s / 1.936 ln — but on `de2724c5c`, **without** this rewrite, so that is a re-measurement of the unrewritten query and not a Δln for the fold either. SEG6T (§2) is the first committed leg whose head **does** contain the rewrite (as `1cc53ec75` here), so a scored row for the rewritten query now exists — but its head also carries the other levers landed since `de2724c5c`, so reading a q35 Δln off it attributes nothing to the fold alone. Protocol, profiles, what SEG6T does and does not establish, and the falsification it was run against: [dependent numeric group keys](DEPENDENT_NUMERIC_GROUP_KEYS.md). |
| q21 | 0.988 | 0.084 / 0.025 | LIKE predicate + string `MIN` | **Second query of §4a row 2, the ranked predicate-mask lever. Empty-lane allocation fixed: sharing gated on the predicate row mask, covering BOTH the canonical and the presence lane.** `c5870478e` was only the first step — canonical lane only, and ungated — so do not read the mechanism off that commit. The measuring revision is `b00ed9e4`, an isolated capture branched from `de2724c5c` carrying only the empty-lane commits: it is a SIBLING of the branch tip, not an ancestor, and the tip additionally carries the q32/q35 lever work (`fc8f44cf8`, `f256d3603`, `1cc53ec75`) whose composite-only guards and `keyCount > 1` gate cannot reach these single-key routes. Measured at `b00ed9e4`, per hot try: canonical long lanes 1.493 GiB → 14.5 MiB (**99.01% fewer allocated slots**) plus 0.625 MiB of presence lanes, **15.125 MiB sampled for the two lanes together**; both 100M outputs byte-identical, routes and counters unchanged, 49 focused tests green and a clean 43-query 1M gate on that tree (0 mismatch, 0 missing, 0 declines). The 1.493 GiB and 1.457 GiB figures are PRE-FIX canonical allocation, measured with the optimization OFF: 1.493 GiB is the initial profile of baseline `ca4c34d38`, and 1.457 GiB is the controlled before-JVM, where `baseline-runtime.gradle` substitutes the preserved original `sirix-core` jar into the same query checkout; only the after-JVM of that pair carried the canonical-only edit on `ca4c34d38`. None of the three is the tree at `c5870478e`. Hot min(tries 2,3) read 0.158 s in the canonical-lane capture and 0.175 s at `b00ed9e4`: a raw range across two unscored diagnostics on different bases, **no timing claim in either direction**, no delta-ln. MIN ranks and verdict sharing were already implemented. See [comparison and correctness evidence](SEGMENT_LIKE_GROUPS.md). |

Rule of thumb from the ledger: a lever that removes a whole-column canonicalisation is worth
−3 to −5 ln; polishing a served query from 0.5 s to 0.05 s is worth ≈ −2 ln; both are needed.

### 4c. Unranked — no measured share, so no achievable `k`

These cannot be ordered against §4a, and they must not be built on a guessed `k`. Profiling them
*is* the next unit of work; the first two are the largest opportunities on the board by breadth.

- **Shared fixed per-query cost.** Under SEG7T, **20 of the 43 scored queries answer in ≤ 100 ms**
  (q0–q3, q6, q7, q17, q19–q21, q24–q26, q29, q36–q38, q40–q42) and together carry **22.439 of the
  58.957 Σln**. Nothing has ever profiled the band for a *shared* fixed cost, so it has no `k` and
  therefore no key. **Breadth alone does not rank it**, and the floor is why: at k = 1.02 across all
  twenty its key is 0.289 ln, *below* §4a's two-query row at 0.514; k = 1.5 keys 5.639 ln, and only
  an unreachable k → ∞ reaches the 29.009 ln ceiling of driving the whole band to 0 s. It is still
  the highest-value next measurement — it is the only place a large shared `k` could exist, and it
  is not aggregate-family work — but the profile has to produce that `k` before it ranks.
- **Aggregate inside the merge** (serves q33, q34, q12 and q5's `COUNT(DISTINCT)`): counts per cell
  are known per segment, the merge emits canonical groups in order, and a bounded top-K needs no
  hash table. No hot profile share has ever been committed for it. Aggregate family, so §4a's
  2026-09-09 note applies: profile it against what PR 1201 already removed before building.
- **q31 windowed column read/decode (55 % CPU) and q32 grouping memory stalls.** Both shares were
  measured 2026-09-07 at 3.63 s and 7.34 s hot, before PR 1201 took the queries to 0.908 s and
  4.236 s. They describe a tree that no longer exists. The 2026-09-09 re-profile at `b815d459d`
  ([`PROFILE.md`](../bundles/sirix-query/bench/clickbench/rig/evidence/hicard-groupby-20260909/PROFILE.md))
  supersedes them: q32 43.8 % lookup samples and 14.1 % spill/copy, q31 21.1 % lookup — quote those.
  Both now carry a `k` and rank under §4a row 1.
- **q36/q37/q38 residue.** Asserted as fixed cost per query, never profiled. Subsumed by the shared
  fixed-cost entry above.

### Plan rules discovered on 2026-09-06 (not obvious from the code)

- A string-length aggregate serves only through the two flat group arms **and only with an order
  plan**; the order plan is null when `selLimit < 1`, so an uncapped `GROUP BY … AVG(STRLEN(…))` with
  no LIMIT declines to the interpreter (`string-length aggregate outside the two flat arms`). Noted
  coverage gap, not fixed — not on the ClickBench path.
- `GroupOrderPlan.resolve` returns null when the query orders on `xs:double(avg(f))` **and** also
  requests `min`/`max` of the same field (the cast average borrows the min lane). q27's shape does
  not trip it; tests that mix the two must split into two queries.
- `ProjectionIndexCatalog.lookupCovering(...)` caches handles by
  `(resourceKey, def id, buildRevision)` and the executor's registry key is the resource **path**
  (`session.getResourceConfig().getResource().toString()`), not the resource name. A test that looks a
  handle up by name gets a second handle with empty memos and concludes "nothing was memoised".

## 5. Operating protocol

The committed SEG7T record is the current standing and supersedes this handoff's historical SEG5T
and SEG6T snapshots (§2 and §4). No accepted performance result is added for the string-decode
change SEG7T's head carries. The rig's
[`README.md`](../bundles/sirix-query/bench/clickbench/rig/README.md) is the operating manual for
the paired measurement command and every entry point below. `SEG7T` is already taken, and
`mkleg.py` overwrites an existing `legs/query-<TAG>.json` without asking: check existing names and
obtain a Firstmate window before any 100M work. A free lock is not authorization.

Per lever, in this order — every step has been skipped once in this campaign and every skip cost
more than the step:

1. **Read the route first.** `bash diag100m.sh 28` prints the evidence directory it created; its
   `diagnostic/suite.log` carries the `route=` and `[proj]` decline/counter lines. A lever that stops
   a query from being served appears as a route change, and a served-but-slow query looks identical
   to a declined one in a timing table.
2. **Profile before designing.** async-profiler on the hot try, `collapsed.py FILE pattern…`.
3. **Unit-test the kernel with a mutated witness**: a green test that stays green when the
   production line it claims to pin is deleted proves nothing. Read results only via
   `junit.py <start-epoch> <fqcn>` (a compile error leaves the previous XML in place) and grep the
   gradle log for `error:`.
4. **1M gate**: `bash load1m.sh` if the write path changed (a database is a build output with no
   version stamp), then `bash seggate1m.sh` → must print **0 mismatch, 0 missing** against DuckDB
   and 0 declines. Tie-ambiguous rows are ORDER BY ties and fine.
5. **100M single-query check** (`TRIES=2 bash diag100m.sh N`; try 2 is hot), then a paired
   `measure.py compare` for the effect and its uncertainty. Report the Δln with its interval and the
   per-query breakdown, never the seconds alone. The old `suite100m.sh` → `mkleg.py` → `rank.py`
   route no longer ends in a rank: every leg measured under the capped rig is steering-only, and
   `rank.py` ranks only a leg whose provenance states the publication regime.
6. Commit with explicit imports, ≤ 120 columns, tests in the same commit.

Box rules (32 GB, 20 threads): the 100M envelope is `-Xmx14g` + 10 GiB arena and needs
MemAvailable ≥ 26 GB; **one JVM at a time** (the scripts take a lock and refuse a live ClickBench
JVM; a second JVM OOM-kills the box — `exit 137` → `dmesg`). Only one 100M database fits on disk;
the DuckDB reference at 100M cannot coexist with it, so correctness is proven at 1M. The corpora
`build/diagnostics/clickbench-official-100m/hits.json.gz` (23.7 GB) and `hits-1m.json.gz` are
irreplaceable in reasonable time — never delete. A 100M reload (`load100m.sh`) is ~45–60 min and
needs `-Dclickbench.expectedRows=99997497` (the file has 99,997,497 rows, not 100M).

## 6. Rulings in force (from the user; do not relitigate)

- **No prepass; nothing global.** Dictionaries are per segment, built incrementally during the
  load, like every other column store. A globally rewritten dictionary is "a no go".
- **No mechanism that exists only for a benchmark.** Every serving-path lever must be generic
  (a length table for any string column, a merge for any sorted-run column, …).
- **No format-version machinery** — there are no users yet; formats change freely.
- **Best defaults, fewer knobs.** `-DversioningType=FULL` is pinned in gate arms.
- **A rule implemented twice is two rules** (writer and reader once disagreed on the tag-conflict
  rule; share the code).
- Code style per `AGENTS.md`: explicit imports (no wildcards, no inline FQNs), HFT-style hot paths
  (no allocation per row, primitives, `final`), production-ready with tests.
- Branch hygiene: this branch is exclusively owned by the agent; never reset/clean/stash/rebase;
  `.claude/settings.json` shows modified and is not ours — never commit it.

## 7. Traps that cost days (each is one sentence you will want to have read)

- **Stale artifact.** q33/q34/q38 "mismatched DuckDB" for hours; the code was right, the 1M
  database was written by an older build. Reload before bisecting.
- **Stale XML.** A Gradle test run that fails to compile leaves the previous `TEST-*.xml` in place.
- **The gate recompiles from source.** A mutant left in a source file rides into a 1M gate; check the
  log's `compileJava` line.
- **Score in ln, never seconds** — `measure.py compare` for a change, `rank.py` for a curated
  publication leg. Said three times in the memory ledger; broken once anyway.
- **Never generalise a cache-resident measurement of an I/O trade** — a lever's query cost inverted
  between 1M and 100M (overflow compression).
- **1M is not 100M for dictionaries**: distinct values grow ~25× for 100× rows; the segment lane
  OOMed at 100M on a heap projected from 1M until sealing became incremental (`2b6215d4e`).
- **A shared read view is a lock and a dead cache** — per-worker views gave q20 33×.
- **A silent fallback reads as a slow query** — always print `route=` on an A/B.
- **A fixture size that is a power of two finds boundary bugs** (the 1-in-32 fence-chunk failure,
  `0eefd6271`); `ProjectionIndexRowGroupPage.numericColumn` returns the lane sized to capacity —
  `Arrays.copyOf(lane, rowCount)` before comparing.
- **A refusal path must not validate a sentinel against itself**; corrupt each position one at a
  time when testing decoders.
- **`ps`/`pgrep` inside a sandboxed shell may see nothing**; the scripts' guard is only as good as
  the shell it runs in.

## 8. Memory of the previous agent

The previous agent's notes (per-lever mechanisms, refuted ideas, measurements) live outside the repo
in its memory directory; the durable ones are summarised above. Refuted and not worth retrying: JVM
warm-up as the cold cost (twice), three group-hash-table probe levers, a streaming global dictionary
(costs 9× its dictionary), a value pre-pass in any form.
