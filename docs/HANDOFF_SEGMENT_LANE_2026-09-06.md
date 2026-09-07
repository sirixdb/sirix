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
- **The +0.01 s offset** on both sides means 0.05 s against a 0.000 s best still costs ln 6 ≈ 1.8.
  The last ~10 ln of a top-10 run live in queries under 100 ms (q25, q36–q38, q39, q10, q21, q22).
- Only 3-try legs score (`suite100m.sh 3`). Never compare legs of different run shapes, and never a
  `-Dsirix.projDiag=true` run.

Standing secondary target: ~50 GB storage at 100M (met: 48 GB); long-term ≤ 30 GB
(`docs/ROADMAP_TO_30GB.md`).

## 2. Where we stand

| leg | build | database | C6A hot geomean | rank / 140 | Σln |
|---|---|---|---|---|---|
| N1FULL1 (2026-09-03) | global dictionary + prepass | `db100m-ovf`, 49.70 GB — **deleted** | 3.327 | **10** | 51.69 |
| SEG3T (2026-09-06, measured) | segment lane | `clickbench-seg100m-20260905-2328`, 48 GB, 148 segments | 19.943 | 74 | 128.69 |
| SEG3TB (SEG3T with the served q21/q22/q28 spliced in) | segment lane + `a3aed07ec`…`417c62ead` | same | 9.516 | 52 | 96.88 |
| projection: SEG3TB + q17 (`86d839058`) + q27 (`be5e8232f`) | `be5e8232f` | same | ≈ 7.89 | ≈ 42 | ≈ 88.8 |
| **SEG4T (2026-09-07, measured)** | segment lane at the handover (`54b0a059b`) | same | **5.179** | **17** | **70.72** |

SEG4T is the measured leg that replaced the arithmetic projection above, and it came in ahead of it.
Rank 10 (Σln ≤ 51.99) needs ≈ **−18.7 ln** from here. Its leg JSON is committed as
`rig/legs/query-SEG4T.json`, so `python3 rank.py SEG4T` reproduces this row — and §4's per-query
contributions — on any box.

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
**canonicalisation** to a column-wide id (`SegmentGroupCanonicaliser`), and every lever below is
about doing less of that, later, or not at all.

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
| diagnostics | `-Dsirix.projDiag=true` prints `route=`, `[proj] groupAgg decline: …`, `[lengthTable] col= mode= segments= memoHits= built= ids= ms=`, `[topk-bounds] kind= positions= segments=`, `segment lane: …`; counters `projectionStringLengthTableBuildCount()`, `projectionStringLengthTableMemoHitCount()`, `segmentOperandSealCount()` on the executor |

Tests that pin the lane: `SegmentLengthLaneQueryTest`, `AnyKGroupsSegmentKeyRewriteTest`,
`AnyKGroupsGlobalKeyRewriteTest`, `GroupTopKDifferentialTest` (sirix-query);
`SegmentLengthLaneGroupScanTest`, `RankTableReadViewTest`, `SegmentBoundariesTest`,
`SegmentCellRoundTripTest`, `SegmentTopKBoundsTest`, `ProjectionBulkLoadFenceChunkBoundaryTest`
(sirix-core).
`sirix-query`'s test JVM forwards `sirix.projDiag` (build.gradle ≈ l. 201), so a declined route in
a test is one `-Dsirix.projDiag=true` away from its reason.

## 4. The lever queue (C6A hot ln; ours s / board best s)

Every ln below is the **measured** SEG4T contribution — `python3 rank.py SEG4T`, `[C6A] hot` block,
from the committed `rig/legs/query-SEG4T.json`. The ≈ 88.8 projection that used to fill this table is
retired: SEG4T was a complete 43-query three-try leg, so no row here is arithmetic any more.

The reordering is material, not cosmetic. q31 and q32 rise into the top four; q35 enters at #8 having
never been in the queue at all; q14 and q39 — projected at 3.96 and 3.85 — measure 1.64 and 1.68 and
leave the queue entirely. The 14 largest measured contributions, in order, are **q25, q28, q31, q32,
q22, q21, q16, q35, q33, q34, q5, q13, q10, q11**. Rows below are grouped by lever and sit at their
largest member's ln.

Two cautions on reading the collapse. The projected seconds were often wrong in the same direction as
the projected ln (q28 measures 57.9 s, not the 115.7 s that stood here), so take both columns from
this table. And the improvements cannot be attributed to a mechanism by this leg alone — SEG3TB, the
leg the projection extrapolated from, was spliced rather than a whole-suite measurement at head.

| q | ln | ours / best | query shape | lever |
|---|---|---|---|---|
| q25 | 4.13 | 0.611 / 0.000 | `ORDER BY SearchPhrase LIMIT 10` | **Profiled and acted on (`0a22879f6`…`663557b11`); not yet rescored.** The merge is not involved at all — `SegmentValueMerge`/`SegmentRunCursor` take zero samples, so the "column materialisation before the merge" guess above was wrong, and the route was already `sorted-scan`. The 0.6 s was the top-K plan: a segment cell means nothing outside its own segment, so `planTopK` declared the key unboundable, all 96,459 admitted leaves of 97,737 carried unknown bounds, none was skipped and 13,172,392 candidate rows were decoded for a ten-entry heap. `SegmentTopKBounds` now bounds a leaf by its **segment's** first/last collation position — advanced once past a directly excluded literal, at most two position reads per segment, no prepass — ranks those ≤ 148 cells once and orders and cuts through dictionary values. 100M diagnostic: evaluations 96,459 → 8,191, 88,268 leaves skipped, candidates 13,172,392 → 1,272,084, no restart. Whole-segment is the granularity ceiling without the merge, whose implementation is preserved **on the original machine only**, at commit `0350230d2b9eb1f28a905de7afc46715875e5cd0` — branch `fm/q25-segment-merge`, aliased as `fm/q25-segment-merge-preserved-0350230d` in the checkout that produced it. It is on **no remote** (`git branch -r --contains` is empty; GitHub answers 404), so a fresh clone cannot reach it and it must be fetched from that machine if it is ever wanted again. An ordering with **no** predicate on the sort key is deliberately refused (its all-present proof would be a whole-column BODY pass — the no-prepass ruling); the reasoning is in `SegmentTopKBounds.create`'s javadoc. **Next: a scored leg** — no timing gain is claimed yet. |
| q28 | 3.79 | 57.9 / 1.297 | `REGEXP_REPLACE(Referer, …)` group + `AVG(STRLEN(Referer))` + `MIN(Referer)` | **Landed** (`095be6eb3`): the regex now runs **once per distinct value per segment** (≤ dictionary size, not 100M rows), STRLEN comes from the length table (`be5e8232f`), and `MIN(Referer)` folds the merge's canonical ranks rather than comparing strings. Mechanism, retention bounds and profiling evidence: `docs/SEGMENT_TRANSFORM_GROUPS.md`. That evidence is a 100M **diagnostic** hot time of 60.728 s → 8.616 s; **no scored suite leg was run, so the ln here still stands and no rank improvement is claimed.** |
| q31 / q32 | 3.27 / 3.05 | 3.63, 7.34 / 0.129, 0.338 | `(WatchID, ClientIP)` numeric composite, 100M groups | **Now #3 and #4** — q31 is the largest regression against the projection (+0.30 ln; q22's +0.12 is the only other one in this table). Not a segment-lane problem: the group hash table is memory-bound (~27 % of suite CPU historically); three probe levers already failed — the idea left is fewer probes (pre-aggregate per leaf / radix partition). |
| q22 / q21 | 2.84 / 2.40 | 0.52, 0.38 / 0.021, 0.025 | LIKE predicates + `MIN(URL)`/`MIN(Title)`, `COUNT(DISTINCT UserID)` | **q22 rose to #5, q21 to #6.** Served since `a3aed07ec`; the residue is the string-predicate verdict share (`921c3f811`) and `MIN` over strings — the min of a group is the smallest canonical id, no string compare needed. |
| q16 / q14 / q18 | 2.32 / 1.64 / 1.55 | 2.06, 0.98, 4.02 / 0.19, 0.18, 0.85 | composite keys `(UserID, SearchPhrase)` etc. | **q14 collapsed 2.32 ln below its projection and is no longer a lever**; q16 leads the group. Composite group keys with a segment-string component canonicalise the string column fully; a cell is already a unique id **within** a segment, so aggregate per segment on the packed cell and merge the per-segment tables by canonical id only for groups that survive. `a-segment-scoped-id-is-a-preaggregation-not-a-group` applies: merging cell keys is correct only where nothing is pruned, so top-K needs canonical ids **during** aggregation for the candidates. |
| q35 | 2.30 | 1.32 / 0.123 | `GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3 ORDER BY c DESC LIMIT 10` | **New at #8** — absent from the projected queue entirely. Same family as q31/q32: a wide numeric composite over a memory-bound group hash table, here with three derived key columns that are pure functions of the first. Whatever fixes the q31/q32 probe cost should be measured on this at the same time. |
| q33 / q34 | 2.21 / 2.18 | 2.42, 2.36 / 0.257 | `GROUP BY URL ORDER BY c LIMIT 10` on an 18.3M-distinct column | Aggregate **inside the merge**: counts per cell are known per segment; the merge emits canonical groups in order and a bounded top-K needs no hash table. Same lever serves q12 (1.51 ln, 0.77 / 0.161) and q5 `COUNT(DISTINCT SearchPhrase)` (2.14 ln, 0.72 / 0.076 — #11 measured; the distinct count is the merge's output length). |
| q13 / q10 / q11 | 2.04 / 1.95 / 1.86 | 2.44, 0.24, 0.24 / 0.309, 0.025, 0.028 | `COUNT(DISTINCT UserID)` grouped by SearchPhrase / MobilePhoneModel / `(MobilePhone, MobilePhoneModel)` | Served; the group key is a segment string and the aggregate is a distinct count per group. q10 came in 0.71 ln under its projection but the family is still three of the top 14, so the distinct-count representation is worth a profile before any of it is redesigned. |
| q39 | 1.68 | 0.157 / 0.021 | 5-column group with `CASE` on Referer/URL + tight `WHERE` | **Collapsed 2.16 ln below its projection (1.44 s → 0.157 s) and left the queue**; kept only so the next agent does not re-derive the old 3.85. If it is ever picked up again: the predicate keeps few rows, so look at what runs before it. |
| q36 / q37 / q38 | 1.32 / 1.32 / 1.31 | 0.065, 0.046, 0.038 / 0.010, 0.005, 0.003 | few-row predicates on URL/Title | Already predicate-first (`5026b239e`); all three now answer in 38–65 ms and have fallen out of the top tier (projected 3.08/2.62, measured ≈ 1.32). The residue is fixed cost per query. The ln shown is what **matching the current board best** would recover — ≈ 1.3 ln per query, not 1.3 ln for the three together. It is not a zero-latency ceiling: rank.py's +0.01 s offset applies to *both* sides of the ratio, so driving our own time to 0 s would recover ≈ 2.02 / 1.72 / 1.57 ln. |

Rule of thumb from the ledger: a lever that removes a whole-column canonicalisation is worth
−3 to −5 ln; polishing a served query from 0.5 s to 0.05 s is worth ≈ −2 ln; both are needed.

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

SEG4T (§2) is the first measured leg; the next one scores whatever lands after it. The commands are
"The one loop that matters" in the rig's
[`README.md`](../bundles/sirix-query/bench/clickbench/rig/README.md) — tag the next leg `SEG5T`,
since `SEG4T` is taken.

Per lever, in this order — every step has been skipped once in this campaign and every skip cost
more than the step:

1. **Read the route first.** `bash diag100m.sh 28` prints `route=` and every `[proj]` decline/
   counter line; a lever that stops a query from being served appears as a route change, and a
   served-but-slow query looks identical to a declined one in a timing table.
2. **Profile before designing.** async-profiler on the hot try, `collapsed.py FILE pattern…`.
3. **Unit-test the kernel with a mutated witness**: a green test that stays green when the
   production line it claims to pin is deleted proves nothing. Read results only via
   `junit.py <start-epoch> <fqcn>` (a compile error leaves the previous XML in place) and grep the
   gradle log for `error:`.
4. **1M gate**: `bash load1m.sh` if the write path changed (a database is a build output with no
   version stamp), then `bash seggate1m.sh` → must print **0 mismatch, 0 missing** against DuckDB
   and 0 declines. Tie-ambiguous rows are ORDER BY ties and fine.
5. **100M single-query check** (`TRIES=2 bash diag100m.sh N`; try 2 is hot), then the 3-try suite
   leg, `mkleg.py`, `rank.py`. Report the rank and the Δln, never the seconds alone.
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
- **Score with rank.py, never seconds.** Said three times in the memory ledger; broken once anyway.
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
