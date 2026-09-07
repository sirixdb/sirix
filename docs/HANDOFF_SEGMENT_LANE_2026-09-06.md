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
| **projection** SEG3TB + q17 (`86d839058`) + q27 (`be5e8232f`) | HEAD | same | ≈ 7.89 | **≈ 42** | ≈ 88.8 |

The projection is arithmetic on two measured single-query numbers (q17 0.055 s hot, q27 0.346 s
hot at 100M); **the first thing to do is replace it with a measured leg** (§5). Rank 10 then needs
≈ **−36.8 ln**.

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
| diagnostics | `-Dsirix.projDiag=true` prints `route=`, `[proj] groupAgg decline: …`, `[lengthTable] col= mode= segments= memoHits= built= ids= ms=`, `segment lane: …`; counters `projectionStringLengthTableBuildCount()`, `projectionStringLengthTableMemoHitCount()`, `segmentOperandSealCount()` on the executor |

Tests that pin the lane: `SegmentLengthLaneQueryTest`, `AnyKGroupsSegmentKeyRewriteTest`,
`AnyKGroupsGlobalKeyRewriteTest`, `GroupTopKDifferentialTest` (sirix-query);
`SegmentLengthLaneGroupScanTest`, `RankTableReadViewTest`, `SegmentBoundariesTest`,
`SegmentCellRoundTripTest`, `ProjectionBulkLoadFenceChunkBoundaryTest` (sirix-core).
`sirix-query`'s test JVM forwards `sirix.projDiag` (build.gradle ≈ l. 201), so a declined route in
a test is one `-Dsirix.projDiag=true` away from its reason.

## 4. The lever queue (C6A hot ln at the projected standing; ours s / board best s)

| q | ln | ours / best | query shape | lever |
|---|---|---|---|---|
| q28 | 4.48 | 115.7 / 1.297 | `REGEXP_REPLACE(Referer, …)` group + `AVG(STRLEN(Referer))` + `MIN(Referer)` | **Landed** (`09f9bf3b3`): the regex now runs **once per distinct value per segment** (≤ dictionary size, not 100M rows), STRLEN comes from the length table (`be5e8232f`), and `MIN(Referer)` folds the merge's canonical ranks rather than comparing strings. Mechanism, retention bounds and profiling evidence: `docs/SEGMENT_TRANSFORM_GROUPS.md`. That evidence is a 100M **diagnostic** hot time of 60.728 s → 8.616 s; **no scored suite leg was run, so the ln here still stands and no rank improvement is claimed.** |
| q25 | 4.18 | 0.645 / 0.000 | `ORDER BY SearchPhrase LIMIT 10` | The answer is the first 10 non-empty values of the merge. Should be a cursor over the first run positions of each segment — tens of microseconds. Find what the 0.6 s is (probably a column materialisation before the merge). |
| q16 / q14 / q18 | 3.97 / 3.96 / 2.72 | 10.7, 10.1, 13.0 / 0.19, 0.18, 0.85 | composite keys `(UserID, SearchPhrase)` etc. | Composite group keys with a segment-string component canonicalise the string column fully; a cell is already a unique id **within** a segment, so aggregate per segment on the packed cell and merge the per-segment tables by canonical id only for groups that survive. `a-segment-scoped-id-is-a-preaggregation-not-a-group` applies: merging cell keys is correct only where nothing is pruned, so top-K needs canonical ids **during** aggregation for the candidates. |
| q39 | 3.85 | 1.44 / 0.021 | 5-column group with `CASE` on Referer/URL + tight `WHERE` | Predicate keeps few rows; the remaining second is fixed cost — see what runs before the predicate. |
| q33 / q34 | 3.13 each | 6.1 / 0.257 | `GROUP BY URL ORDER BY c LIMIT 10` on an 18.3M-distinct column | Aggregate **inside the merge**: counts per cell are known per segment; the merge emits canonical groups in order and a bounded top-K needs no hash table. Same lever serves q12 (2.2 ln), q5 `COUNT(DISTINCT SearchPhrase)` (2.65 — the distinct count is the merge's output length). |
| q38 / q36 / q37 | 3.08 / 2.62 / — | 0.27, 0.27 / 0.003, 0.010 | few-row predicates on URL/Title | Already predicate-first (`5026b239e`); the residue is fixed cost per query — 100 ms is 2.4 ln here. Profile a hot try. |
| q32 / q31 | 3.08 / 2.97 | 7.5, 2.7 / 0.34, 0.13 | `(WatchID, ClientIP)` numeric composite, 100M groups | Not a segment-lane problem: the group hash table is memory-bound (~27 % of suite CPU historically); three probe levers already failed — the idea left is fewer probes (pre-aggregate per leaf / radix partition). |
| q22 / q21 / q10 | 2.72 / 2.68 / 2.66 | 0.46, 0.50, 0.49 / 0.02 | LIKE predicates + `MIN(URL)`, `COUNT(DISTINCT UserID)` | Served since `a3aed07ec`; the residue is the string-predicate verdict share (`921c3f811`) and `MIN` over strings — the min of a group is the smallest canonical id, no string compare needed. |

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

Do the first measured leg before anything else — the standing above is a projection:

```sh
cd bundles/sirix-query/bench/clickbench/rig
cat ../../../build/diagnostics/rig/current-100m-dir.txt   # the 100M DB the scripts use
bash suite100m.sh 3                                        # 43 queries × 3 tries, ~10 min
python3 mkleg.py SEG4T "$(cat ../../../build/diagnostics/rig/current-100m-dir.txt)/suite100m.log"
python3 rank.py SEG4T SEG3T N1FULL1                        # the [C6A] hot block is the target
```

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
