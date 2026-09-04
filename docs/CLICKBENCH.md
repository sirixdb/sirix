# ClickBench on SirixDB

[ClickBench](https://github.com/ClickHouse/ClickBench) is 43 analytical SQL queries over one wide,
denormalised web-analytics table (`hits`, 105 columns, ~100 M rows). This document describes the
port of that benchmark to SirixDB — a versioned JSON document store queried with JSONiq — what the
port measures, how to run it, and what it found.

Two things are worth saying up front, because they frame every number below:

* **SirixDB is not a column store.** ClickBench's reference systems persist columns; SirixDB
  persists a versioned tree in which every revision of every record stays queryable. The port exists
  to make that comparison measurable and honest, not flattering.
* **The port found four real engine defects, all of them wrong answers rather than slow ones**,
  all four fixed here. See [What the port found](#what-the-port-found).

## Layout

| path | what it is |
|---|---|
| `bundles/sirix-query/src/main/java/io/sirix/query/bench/clickbench/ClickBenchQueries.java` | the 43 queries, ported SQL → JSONiq, each with its original SQL alongside |
| `…/ClickBenchSchema.java` | the 105 columns, their types, and the database/resource names |
| `…/ClickBenchHitsGenerator.java` | deterministic synthetic `hits` generator, so the port runs without the 14 GB download |
| `…/ClickBenchSource.java` | opens a JSON-array file, a JSON-lines file (both optionally gzipped) or `generate:<rows>` |
| `…/ClickBenchLoadMain.java` | ingest + ClickBench's `Load time:` / `Data size:` lines + an encoding check |
| `…/ClickBenchRunMain.java` | the 43 queries under the ClickBench protocol; result dumps; results JSON |
| `…/ClickBenchGenerateMain.java` | writes the synthetic dataset to a file so a reference engine reads identical bytes |
| `…/ClickBenchProjection.java` | the projection index the 43 queries are served from — its columns are derived from the queries, not hand-listed |
| `…/ClickBenchMaintenanceMain.java`, `…/HftRuntimeEvidence.java` | a bounded ordinary-maintenance arm (>100 k dirty records in one commit, then cold reopen) and the HFT boundary evidence it emits |
| `…/ClickBenchParallelLoadProbe.java` | ingest-side probe: cursor, bulk-assembly and parallel-importer arms over the same rows, partitioned or single-resource, each run witnessed structurally |
| `…/ClickBenchParallelProjectionCostMain.java`, `…/ClickBenchPrimitiveIndexImportCostMain.java` | interleaved cost arms for index maintenance riding the parallel import — one-pass projection vs. post-pass build, and PATH/CAS/NAME on vs. off |
| `…/ClickBenchCompositeQueries.java`, `…/ClickBenchCompositeDifferentialMain.java` | the partition-decomposed formulation of the 43 queries, and the differential proving each composite answer equals the single-resource one |
| `bundles/sirix-query/bench/clickbench/` | the DuckDB reference side (`prepare-data.sh`, `duckdb_reference.py`, `compare-results.py`, `run-differential.sh`, `queries.sql`) and the fixed-heap HFT GC/safepoint and maintenance gates (`cold-rounds.sh`, `hft_*.py`) — protocol and thresholds in that directory's [`README.md`](../bundles/sirix-query/bench/clickbench/README.md) |
| `bundles/sirix-query/src/test/java/io/sirix/query/bench/clickbench/` | the CI gates: generator invariants, source/projection wiring, HFT runtime evidence, and a 43-query smoke test |

## Running it

Synthetic data, no download:

```bash
./gradlew :sirix-query:clickBenchLoad -Pclickbench.args="/tmp/cb-1m generate:1000000"
./gradlew :sirix-query:clickBench     -Pclickbench.args="/tmp/cb-1m --tries 3 --json /tmp/cb.json"
```

The real dataset (14 GB parquet, downloaded separately):

```bash
wget https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet
bundles/sirix-query/bench/clickbench/prepare-data.sh hits.parquet /data/hits.json      # all rows
bundles/sirix-query/bench/clickbench/prepare-data.sh hits.parquet /data/hits-10m.json 10000000
./gradlew :sirix-query:clickBenchLoad -Pclickbench.args="/data/cb-db /data/hits-10m.json"
./gradlew :sirix-query:clickBench     -Pclickbench.args="/data/cb-db --tries 3 --json results.json"
```

The correctness gate — the same 43 queries three ways over byte-identical records:

```bash
bundles/sirix-query/bench/clickbench/run-differential.sh 200000
```

### Import and decomposition harnesses

Four further entry points measure the *load* side rather than the query side; together they produce
the ingestion figures in [`BULK_IMPORT.md`](BULK_IMPORT.md). Each takes its positional arguments
through `-Pclickbench.args` and its switches through `-Pclickbench.jvmArgs`, exactly like the tasks
above.

```bash
# ingest arms over the same rows: cursor (default), bulk assembler, parallel importer
./gradlew :sirix-query:clickBenchParallelLoadProbe -Pclickbench.args="/tmp/cb-probe 1000000 1" \
    -Pclickbench.jvmArgs="-Dprobe.bulk=true -Dprobe.parallelImport=true -Dprobe.file=/data/hits.json"

# one-pass projection build vs. bare vs. bare-then-post-pass, interleaved, min of 3
./gradlew :sirix-query:clickBenchProjectionCost -Pclickbench.args="/data/hits-1m.json /var/tmp/cost 3" \
    -Pclickbench.jvmArgs="-Dsirix.projection.globalDict=never -Dcost.expectedRows=1000000"

# PATH/CAS/NAME maintenance on vs. off inside the same import, interleaved, min of 3
./gradlew :sirix-query:clickBenchPrimitiveIndexCost -Pclickbench.args="/data/hits-1m.json /var/tmp/idxcost 3"

# every query's partitioned formulation against the single-resource ground truth
./gradlew :sirix-query:clickBenchCompositeDifferential \
    -Pclickbench.args="/var/tmp <singleDb> hits <compositeDb> 4"
```

| task | arguments | switches |
|---|---|---|
| `clickBenchParallelLoadProbe` | `<dbDir> <totalRows> <partitions> [maxConcurrency]` | arm selection: `probe.bulk` (bulk assembler instead of the Gson cursor), `probe.parallelImport` (chunked coordinator/worker pipeline); corpus: `probe.file` (single partition only), `probe.ndjson` + `probe.rowLimit`, or `probe.writeFile` to emit the generated rows and exit; post-pass per-partition projection: `probe.projection`, `probe.projectionParallel`. Resource shape: `storageType`, `versioningType`, `buildPathSummary`, `sirix.autoCommit.nodes`, `sirix.offheap.bytes`, `clickbench.seed` |
| `clickBenchProjectionCost` | `<corpus.json> <workDir> [reps]` | `cost.expectedRows` (the expected-row-count hint both projection arms build with; default `-1`, unknown); the published arms also set `sirix.projection.globalDict=never` |
| `clickBenchPrimitiveIndexCost` | `<corpus.json> <workDir> [reps]` | none |
| `clickBenchCompositeDifferential` | `<location> <singleDb> <singleResource> <compositeDb> <partitions> [--timings-only\|--union]` | `sirix.offheap.bytes` (default 6 GiB — the arena and the comparison heap share the box) |

Both databases the differential opens must hold the *same* rows, so generate them from one seed —
`generate:<rows>:<seed>` through `clickBenchLoad`, `-Dclickbench.seed` through the probe, whose
partition slices are byte-identical disjoint ranges of the single-resource corpus. `--union` swaps
the composite arm for the original query text over the partitioned database's logical union
resource.

## The JSON encoding

ClickBench's rules forbid changing the data; they do not prescribe how a system types it, and every
entry maps the columns onto its own type system (MongoDB imports BSON int64s and dates,
Elasticsearch maps `long` and `date`, Druid keeps three of the four timestamps as strings). The
JSON encoding here is:

* one object per hit, **all 105 columns always present**, in `create.sql` order;
* the whole file is **one JSON array** of those objects. The loader also accepts newline-delimited
  JSON — the shape of the official `hits.json.gz`. File sources default to the general chunked
  parallel importer: a streaming delimiter adapter frames LDJSON as an array, while an
  allocation-stable byte filter exposes quoted signed `BIGINT` values as exact numeric tokens and
  changes only the timestamp separator from a space to `T`. Neither adapter buffers a record or
  writes a converted source file. `-Dclickbench.parallelImport=false` retains the Jackson cursor;
* integers as JSON numbers, **exact int64, never quoted** — `UserID` and the two hashes are 18-digit
  values that Q19/Q40/Q41 filter on by literal;
* text as JSON strings, with `NULL` coalesced to `""` (ClickBench's own missing marker);
* `EventDate` as `"YYYY-MM-DD"`, the three timestamps as `"YYYY-MM-DDTHH:MM:SS"`.

`ClickBenchSchema.java` is the authoritative definition of this encoding; where prose and code
disagree, the code wins.

The date choice is load-bearing rather than cosmetic. JSON has no date type, and ISO-8601 orders
lexicographically, so `ORDER BY EventTime` and the `EventDate BETWEEN …` predicates stay plain string
comparisons, `extract(minute FROM EventTime)` is `xs:integer(substring($t, 15, 2))`, and
`DATE_TRUNC('minute', EventTime)` is `substring($t, 1, 16)`.

`ClickBenchLoadMain` validates the first source record **before opening the target store**, then
checks the first stored record after ingest. This is not ceremony: ClickHouse's `JSONEachRow` quotes
64-bit integers by default, so the official `hits.json.gz` carries values such as
`"UserID":"435090932899640449"`. With the default `clickbench.normalizeSource=true`, the streaming
parser exposes that token as an exact signed long. Strict mode rejects the quoted token before the
target can be replaced. Without either guard it would shred as a string node, and several queries
would quietly return wrong results while the rest still looked plausible.

## Translating the SQL

Most of the translation is mechanical. These are the places where it is not, each forced by the
engine rather than chosen:

| SQL | JSONiq | why |
|---|---|---|
| `LIMIT n OFFSET k` | `fn:subsequence(expr, k+1, n)` | in JSONiq mode `[...]` is an array index, not a positional predicate, so `[position() le 10]` does not exist. SirixDB's top-k pushdown also keys on exactly this shape (two integer literals) |
| `HAVING` | a `where` clause **after** `group by` | XQuery 3.0's free clause ordering; brackit compiles it to a Select on top of the GroupBy, which is what HAVING is |
| `COUNT(DISTINCT x)` | `count(distinct-values($g.x))` | the post-group path form; the equivalent nested-`for` spelling used to be answered with the global fold (defect 3 below, now fixed) |
| `AVG(x)` | `xs:double(avg(…))` | brackit's `fn:avg` over integers returns `xs:integer` when the quotient is exact and `xs:decimal` otherwise; SQL `AVG` is a double |
| `LIKE '%x%'` | `contains(…, "x")` | — |
| `REGEXP_REPLACE(r, '^https?://(?:www\.)?([^/]+)/.*$', '\1')` | `replace($r, '^https?://(www\.)?([^/]+)/.*$', '$2')` | brackit translates XSD regex to Java and rejects **every** `(?…)` construct, so the non-capturing group becomes a capturing one and the replacement renumbers. The pattern must sit in a **single-quoted** literal: double-quoted literals take JSON escape rules, where `\.` is not legal |
| `CASE WHEN … THEN … ELSE … END` in a group key | a `let` with `if … then … else …` before the `group by` | a grouping spec can only name a variable |

Where a second formulation is legitimately equivalent it is kept as a variant (`--variant N`), and
the smoke test requires all variants of a query to agree. Q2 (three aggregates) and Q29 (ninety
shifted sums) each have a multi-pass and a single-pass form.

## What the port found

Running 43 unfamiliar query shapes over a 105-column document turned up three wrong-answer defects
that the existing suites did not cover. All three were found by differential testing, none by a
crash.

**1. `sum`/`avg` over 64-bit integers wrapped silently.** *(fixed)* Q3 is `AVG(UserID)`. The
vectorized aggregate accumulated into a `long`; a column of 1e18-scale ids overflows that after a few
dozen rows. At 200 k rows SirixDB answered `1.67921e13` where the true average is `5.71681e17` — the
reported sum was exactly the true sum modulo 2^64. `xs:integer` is arbitrary precision and brackit's
own `AbstractNumeric#addLong` escalates to `BigDecimal` on overflow; the kernel now detects overflow
(exactly in the scalar lanes, by a magnitude bound for the SIMD page kernel) and redoes the fold
through the exact accumulator. The same wrap existed a second time in the path-summary statistics
(`PathStats.sum`), which serve `sum`/`avg` directly in the default configuration; that accumulator
is 128 bits wide and the serving verdict is derived from the exact total, so a column whose TRUE sum
leaves `long` declines and falls back to the scan (see `docs/DISK_FORMAT.md`, §2 "PathStats
trailer"). Regression test: `VectorizedAggregateExactnessTest`.

**2. `min`/`max` over a string column failed instead of answering.** *(fixed)* Q6 is
`MIN(EventDate), MAX(EventDate)`; Q21/Q22 take `MIN(URL)`/`MIN(Title)`. The numeric kernels
contribute nothing for a string column, and that branch was terminal — it threw
`BIDY0300 … string/boolean/null aggregation is not supported`, and the caller
(`VectorizedGroupByExpr#requireSupported`) turns a decline into an error rather than a fallback, so
there was nothing to fall back to. String extrema are well defined (`fn:min`/`fn:max` order
`xs:string` by codepoint) and the interpreter answers them, so the kernel now answers them too, via
the typed group-key kernel it already trusts for `count(distinct …)`. `sum`/`avg` over strings stay
loud, because there the interpreter's own semantics are an error.

**3. An aggregate over a nested `for` on a grouped variable folded the whole input.** *(fixed in
SirixDB; the root cause is upstream in brackit)* In the default configuration,

```
for $h in $hits[] let $k := $h.RegionID group by $k
return {"k": $k, "s": sum(for $x in $h return $x.ResolutionWidth)}
```

returned the **global** sum for every group (verified: every group reported 4998, the sum over all
records, where the correct per-group values were 3634 and 1364), and the `count(distinct-values(…))`
form of the same shape reported the global distinct count. `count($h)` — which needs no field, so no
inner pipeline — stayed correct, which is what made the wrong ones hard to notice.

A `group by` rebinds every non-grouping variable to the sequence of *that group's* values. Brackit's
detection walker resolves a scan's source by following the variable to its binding clause, and that
walk does not stop at the grouping, so the inner pipeline's source resolved back through `$h` to the
whole document — and the source gate, whose entire job is to prove a scan reads the resource it
claims, was handed a `SourceRef` that named the right document for the wrong extent.

The fix is upstream, in brackit's `VectorizedGroupByDetection`: a second pass over the annotated AST
withdraws any claim whose scan source is a variable an enclosing `group by` has rebound, replacing it
with `SourceRef.unknown()` — which every compile-time gate already fails closed on, so the query
falls back to the generic pipeline that reads the grouped binding. Written as a separate pass rather
than another parameter threaded through `resolveSourceRef`, so the resolution itself and the
200-line `tryAnnotate` stay untouched. Merged as
[brackit#117](https://github.com/sirixdb/brackit/pull/117).

SirixDB carried a backend-side guard for the same shape while that was in flight
(`RegroupedSourceGuardStage`). It was removed once the published snapshot carried the upstream fix
and the differential passed with the stage disabled — one fix in one place beats two. Regression
test: `TypedGroupByDifferentialTest#aggregateOverANestedForOnAGroupedVariableStaysPerGroup`, which
runs sum/avg/min/max/count/count-distinct and a two-key sparse-field case through both pipelines and
so catches a regression from either side.

A fifth, cosmetic difference is worth recording: brackit's serializer writes a bare `Atomic` with
`toString()` but quotes an `Atomic` that also implements `JsonItem`. A kernel that answered
`min(EventDate)` with a plain `Str` therefore serialized `2013-07-02` where the interpreter
serialized `"2013-07-02"` — same value, different bytes. `ComputedStrJsonItem` exists so computed
string results are indistinguishable from read ones.

## Correctness: the three-way differential

`run-differential.sh` runs the 43 queries three ways over byte-identical records and diffs every
result:

1. SirixDB, default configuration (analytical fast paths on);
2. SirixDB with `-Dsirix.query.autoVectorize=false` (the generic interpreter);
3. DuckDB over the same JSON file.

Leg 1 vs 2 catches a fast path claiming a shape it cannot serve; leg 1 vs 3 catches a mistranslation
of the SQL. A harness detail that matters more than it looks: the runner must *not* install a
vectorized executor when the kill switch is off, or leg 2 measures the fast path twice and the
comparison proves nothing. That bug hid defect 1 for a full differential cycle.

The differential now proves that separation per query. Its fast leg uses
`--require-vectorized-serving`: each fully serialized query must move at least one outcome-level
serving counter, and the runner prints the exact route delta. Attempts, catalogue lookups, declines,
and page reads do not count. Its interpreter leg uses `--require-generic-serving` and fails if any
of those counters moves. For campaigns claiming the AUTO resource-wide dictionary, set
`REQUIRE_AUTO_GLOBAL_DICT=1`; this forces `sirix.projection.globalDict=auto` and rejects a missing or
zero `globalDictColumns` completion banner before query execution.

Result at 200 000 rows, after the fixes:

| leg | match | tie-ambiguous (of which unverifiable) | mismatch |
|---|---:|---:|---:|
| fast path vs interpreter | 42 | 1 (1) | **0** |
| SirixDB vs DuckDB | 33 | 10 (5) | **0** |

The split between the first two columns moves by a query or two between runs over identical data,
because DuckDB's own choice among tied rows is not stable across runs; the mismatch column is what
the gate asserts on.

"Tie-ambiguous" is not a euphemism for "different". Roughly half of ClickBench's queries are
`ORDER BY <aggregate> DESC LIMIT 10` over data with many equal counts, so which of the tied rows
survives the cut-off is engine-defined — DuckDB itself returns different rows at different thread
counts. `compare-results.py` only accepts a difference when the differing rows are all tied at the
window boundary on the query's own sort key, and calls everything else a mismatch. Five of the ten
are additionally marked **unverifiable** and counted separately in the summary, because nothing about
them was actually checked: Q17 has no `ORDER BY` at all, and Q31/Q32/Q38/Q39 return a window that
lies entirely inside one tied group (at this scale their `PageViews` plateau is all 1s), so two
completely disjoint answers are both legal. The script also fails loud — exit 2 — on a `queries.sql`
that is not 43 statements or a column roster that has drifted from `ClickBenchSchema.java`, because
either would quietly weaken every verdict.

## Measured

See [Numbers](#numbers) below. Scale caveat first, because it is the honest headline: the per-query
table here is at **1 M rows**, not ClickBench's 100 M. The 100 M build *has* since been run — see
[At 100 M rows](#at-100-m-rows) for its figures, its protocol and what it costs to repeat — but it
needs the real corpus and ~50 GB of free disk per database, so the 1 M table stays the one a reader
can reproduce.

Do not extrapolate one to the other. At 1 M the load below takes ~63 s and occupies 1.36 GB, which
would put 100 M at ~136 GB; the measured 100 M database is **49.70 GB**, i.e. the naive ×100 is
2.7× too high. The two are different configurations, not one workload at two sizes: the table below
is synthetic data on the bare row path, the 100 M build is the real corpus on the
global-dictionary and trie-lane route.

Other things to keep in mind when reading the numbers:

* the data is the **synthetic** generator, not the real `hits` corpus. Its distributions are shaped
  to make every query non-degenerate (35 % of rows carry `CounterID = 62`, 15 % of URLs contain
  `google`, the literals Q19/Q40/Q41 filter on are planted), but string lengths and cardinalities are
  not the real ones;
* no page-cache dropping between tries, so try 1 here is not ClickBench's "true cold" — a submission
  would have to carry the `lukewarm-cold-run` tag or drop caches per query;
* a fresh `SirixVectorizedExecutor` is installed per try, so the executor's `(source, predicate)`
  memo never serves a timed run. Without that, tries 2 and 3 report a hash lookup;
* the resource is loaded **with** a path summary and a projection index over the 25 columns the 43
  queries touch (`-Dclickbench.projection`, on by default, and the summary is forced with it because
  the projection builder resolves its field paths through it). Building the index is charged to
  `Load time`, the way DuckDB's own per-column structures are charged to its ingest. Pass
  `-Dclickbench.projection=false` for a row-path A/B. See
  [What the projection actually serves](#what-the-projection-actually-serves);
* the index is built **by** the shred rather than after it: the definition is catalogued on the empty
  resource and the shred's own change notifications feed the projection builder, so the corpus is
  walked once. At 1 M rows that is `Load time` 95.8 s against 121.7 s for shred-then-build (71.5 s +
  50.2 s), and the saving grows with the corpus because the second pass it removes is a full walk.
  The ClickBench loader now rejects `-Dclickbench.projection.incremental=false`; it never repairs a
  failed load by walking the completed corpus. Explicit projection DDL remains available for an
  already-loaded non-benchmark resource, but it is not a ClickBench ingestion mode;

### Numbers

_1 M rows, 20-core Linux (32 GB RAM, NVMe), GraalVM, 3 tries per query, synthetic data, no
projection index. "cold" is try 1 with the OS page cache warm from the previous query, "hot" is
min(try 2, try 3). Load: 62.7 s for 1 000 000 records; data size 1 357 109 623 bytes._

| q | shape | cold (s) | hot (s) |
|---:|---|---:|---:|
| 0 | COUNT(*) | 0.102 | 0.001 |
| 1 | COUNT(*) WHERE AdvEngineID <> 0 | 5.754 | 1.381 |
| 2 | SUM + COUNT + AVG | 1.057 | 0.078 |
| 3 | AVG(UserID) | 0.515 | 0.302 |
| 4 | COUNT(DISTINCT UserID) | 0.860 | 0.552 |
| 5 | COUNT(DISTINCT SearchPhrase) | 1.237 | 0.353 |
| 6 | MIN/MAX(EventDate) | 0.828 | 0.689 |
| 7 | GROUP BY AdvEngineID | 1.537 | 1.326 |
| 8 | RegionID -> COUNT(DISTINCT UserID) | 1.874 | 1.703 |
| 9 | RegionID -> four aggregates | 1.588 | 1.552 |
| 10 | MobilePhoneModel -> uniq users | 1.359 | 1.306 |
| 11 | MobilePhone + Model -> uniq users | 1.386 | 1.376 |
| 12 | SearchPhrase -> count | 1.430 | 1.412 |
| 13 | SearchPhrase -> uniq users | 1.581 | 1.579 |
| 14 | SearchEngineID + SearchPhrase | 1.648 | 1.606 |
| 15 | GROUP BY UserID | 1.136 | 1.108 |
| 16 | UserID + SearchPhrase | 2.590 | 2.587 |
| 17 | UserID + SearchPhrase, no ORDER BY | 1.119 | 0.852 |
| 18 | UserID + minute + SearchPhrase | 3.147 | 3.165 |
| 19 | WHERE UserID = <literal> | 0.576 | 0.528 |
| 20 | COUNT WHERE URL LIKE '%google%' | 0.754 | 0.737 |
| 21 | SearchPhrase -> MIN(URL) | 0.964 | 0.960 |
| 22 | Title LIKE / URL NOT LIKE | 1.503 | 1.502 |
| 23 | SELECT * ORDER BY EventTime | 0.953 | 0.894 |
| 24 | SearchPhrase ORDER BY EventTime | 1.497 | 1.486 |
| 25 | SearchPhrase ORDER BY SearchPhrase | 1.407 | 1.405 |
| 26 | ORDER BY EventTime, SearchPhrase | 1.468 | 1.470 |
| 27 | CounterID -> AVG(STRLEN(URL)), HAVING | 1.357 | 1.403 |
| 28 | REGEXP_REPLACE(Referer), HAVING | 2.336 | 2.222 |
| 29 | 90 shifted SUMs | 8.927 | 9.427 |
| 30 | SearchEngineID + ClientIP | 1.744 | 1.717 |
| 31 | WatchID + ClientIP (filtered) | 1.522 | 1.510 |
| 32 | WatchID + ClientIP | 1.105 | 1.161 |
| 33 | GROUP BY URL | 1.247 | 1.220 |
| 34 | GROUP BY 1, URL | 1.272 | 1.243 |
| 35 | ClientIP arithmetic keys | 1.862 | 1.810 |
| 36 | July window -> URL page views | 4.054 | 3.658 |
| 37 | July window -> Title page views | 3.043 | 3.026 |
| 38 | July window -> URL, OFFSET 1000 | 3.526 | 3.517 |
| 39 | five keys incl. CASE, OFFSET 1000 | 3.057 | 3.115 |
| 40 | URLHash + EventDate, OFFSET 100 | 3.206 | 3.130 |
| 41 | window size, OFFSET 10000 | 3.189 | 3.181 |
| 42 | DATE_TRUNC minute, OFFSET 1000 | 2.960 | 2.932 |
| | **total** | **84.3** | **76.2** |

## At 100 M rows

The table above is 1 M. The 100 M build has since been run as an A/B — two *whole* databases, not
one database re-measured — and this section records it, because until now those figures lived only
in commit messages and a session transcript.

**None of the 100 M figures below are reproducible on a host without the disk for them.** What a
smaller host *can* check is [What a smaller host can reproduce](#what-a-smaller-host-can-reproduce),
and that is a real subset — the storage direction and the answer identity — rather than a
consolation prize. The leaderboard rank is not in it.

### Prerequisites

* **~50 GB free per database**, and two of them if you want to keep the previous build alongside the
  new one, which an A/B of a storage lever requires;
* **the official 100 M corpus** as `hits.json.gz` (23.7 GB). The synthetic generator does not stand
  in here: the levers being measured are string- and timestamp-shaped, and the generator's string
  lengths and cardinalities are not the real ones;
* **the pre-pass value files.** `-Dsirix.import.prepassRunner` runs `ClickBenchLoadPrepassHook`
  against the still-empty resource, which commits the resource-wide rank-ordered dictionaries named
  by `-Dsirix.projection.globalDict.prepassValues=<Column>=<file>,…` and publishes their anchors, so
  the load binds them at its first streaming epoch instead of running the election. Without them the
  build is a different route and its bytes are not comparable to the ones below;
* **`absent=0 afterClose=0`** on the load's `[trie-lane]` line. A non-zero count on either means the
  lane converted fewer pages than it claims — `TrieLaneWriteDictionaries` puts it plainly: *"an arm
  with a non-zero absent count has under-converted its pages, so its size is not this lever's
  size"*. Both arms below read zero. A run that does not is not a measurement, and its storage
  number must not be quoted.

The full launch protocol — the GO/NO-GO memory diagnostic, the RSS and `df` watchdogs, and the
SIGTERM-only kill discipline that preserves the shutdown-hook counter lines — is
[`CLICKBENCH_100M_RESUMPTION_PLAN.md` §6](CLICKBENCH_100M_RESUMPTION_PLAN.md).

### Storage

_Two 100 M builds differing only in overflow compression (`-Dsirix.page.overflow.compress`, flipped
to on by default) and the temporal lane (`-Dsirix.page.temporalLane`). Page-class rows are the
writer-path `StorageProfile` figures (`-Dsirix.storage.profile=true`)._

| | before | after | delta |
|---|---:|---:|---|
| database on disk | 52,488,784,824 B | 49,703,766,971 B | **−2.79 GB** |
| `OverflowPage` | 10,862,700,646 B | 8,078,960,087 B | ratio **0.744** |
| `KeyValueLeafPage` | 39,444,149,969 B | 39,444,812,649 B | **+0.66 MB** |

The whole of the win is the overflow class. The `KeyValueLeafPage` row is the temporal lane, and it
is a *non*-result recorded as one — see [the caveats](#two-caveats-that-travel-with-these-numbers).

### Queries

_Same two databases, same 43-query protocol._

| | before | after |
|---|---:|---:|
| C6A hot geomean | 3.329 (rank 10) | **3.159 (rank 6)** |
| paired per-query | — | **−2.244 ln**, 23 faster to 11 slower |
| answers | — | **43/43 byte-identical** |

Separately, and on one database rather than two, the stride-aware group budget was measured with
`-Dsirix.projDiag=true` against its kill switch `-Dsirix.projection.groupTable.strideBudget=false`:
**19 → 16 grouped passes** over a leg, −1.555 and −1.131 ln over two leg pairs.

Geomeans are only comparable *within* a build pair. Two separately measured noise floors bound what
counts as a result at all: repeating one build moves the C6A hot geomean by about ±0.08, and nothing
under roughly 0.3 ln survives a repeated leg.

The storage and query figures above are also carried, in the same form, by the javadoc on
`PageKind.OVERFLOW_PAYLOAD_COMPRESSION_ENABLED` — the flag they justify — so the default and its
evidence cannot drift apart unnoticed.

### Two caveats that travel with these numbers

1. **The overflow ratio is scale-dependent, and the query cost inverts outright.** The compression
   ratio is 0.560 at 1 M against 0.744 at 100 M, and the sign of the *query* effect flips: the same
   flag costs **+22.4 % hot at 1 M** and is **faster at 100 M**. A cache-resident working set makes
   every decode pure added CPU buying no I/O back, while an I/O-bound scan gets the unread bytes
   back with interest. Do not generalise a small-scale measurement of this flag — its sign is wrong
   there. Both numbers are in `PageKind`'s javadoc for the same reason.
2. **The temporal lane is worth ~0 on the route that ships.** It projects to about −1.06 GB at
   100 M from its 1 M measurement, and measured **+0.66 MB** on the trie-lane + pre-pass route
   above. Why it does not fire there is open; it is kept because it is correct, tested and cheap,
   and it is **off by default**. See
   [`ROADMAP_TO_30GB.md`](ROADMAP_TO_30GB.md), *"The temporal sub-lever, MEASURED"*.

### What a smaller host can reproduce

At 200 k–1 M rows the shipped tasks reproduce the *direction* of the storage result and the answer
identity exactly. Two whole-database loads that differ only in the kill switch:

```bash
./gradlew :sirix-query:clickBenchLoad -Pclickbench.args="/tmp/cb-on  generate:200000:42" \
    -Pclickbench.jvmArgs="-Dsirix.storage.profile=true"
./gradlew :sirix-query:clickBenchLoad -Pclickbench.args="/tmp/cb-off generate:200000:42" \
    -Pclickbench.jvmArgs="-Dsirix.storage.profile=true -Dsirix.page.overflow.compress=false"
```

| 200 k rows, seed 42 | off (kill switch) | on (default) | |
|---|---:|---:|---|
| ClickBench `Data size:` | 352,783,201 | 285,674,336 | −19.02 % |
| `OverflowPage` bytes | 183,081,357 | 118,588,830 | ratio **0.648** |
| `OverflowPage` writes | 112,892 | 112,892 | unchanged |

Read those two rows differently. The `Data size:` delta lands on an exact 64 MiB boundary because
`sirix.data` grows in 64 MiB extents, and the totals themselves drift by a couple of bytes between
runs of the *same* arm (two independent runs of the compressed arm gave 285,674,336 and
285,674,338), so quote it as a percentage and not as a byte count. The `StorageProfile`
`OverflowPage` row is the un-quantised writer-path figure and reproduced *exactly* across both runs
— it is the one that carries the ratio. Then `--dump` both databases and `diff -r` the two
directories: all 43 `q*.jsonl` come back byte-equal.

The group budget's one-sided safety property is observable on real shapes too — `-Dsirix.projDiag`
prints one `[groupBudget]` line per grouped plan, and over a 43-query leg 13 of 28 grouped plans are
charged less than the old flat 128 B/group (48/64/96/112 B at strides 3/4/6/7) while all 15 plans at
stride ≥ 8 keep **exactly** the old 128 B and the identical 12,582,912-byte budget. No plan is ever
charged more, so no shape can plan more passes than before.

What a small host cannot show, and what this document therefore does not claim it shows: at 200 k
rows the compressed database is about **5.6 % slower** over the 43-query sum — the same sign as the
+22.4 % at 1 M, and the reason caveat 1 exists. The inversion at 100 M, and the rank that follows
from it, are the author's measurements alone.

## Against DuckDB

Same 1M rows, same box, both engines materialising results, three tries each:

| | SirixDB (row paths) | DuckDB | ratio |
|---|---:|---:|---:|
| cold total, 43 queries | 84.6 s | 0.37 s | 227x |
| hot total, 43 queries | 75.6 s | 0.35 s | 215x |

Best case is Q0 `COUNT(*)` at 3x (metadata on both sides); worst is Q1
`COUNT(*) WHERE AdvEngineID <> 0` at 2317x — 1.418 s against 0.6 ms, i.e. SirixDB walking one field
of 1M records at ~700k rows/s against DuckDB scanning one compressed column. This is the honest floor
for the row paths, and it is three orders of magnitude away from
`docs/COMPARISON_DUCKDB.md`'s 1.1-2.5x, which is measured **with** a projection index over a
five-column dataset.

## What the projection actually serves

Until recently the answer was **nothing**: the harness never created a projection index, so every
number ever published here measured the row path with

```
# served: predicateCounts=0 groupAggregates=0 numericGroupBys=0
```

for all 43 queries. (An ad-hoc 25-column projection had been A/B'd at 100k rows and reported 6 of 43
servable shapes, but that projection was built by hand and never by the benchmark.) The loader now
builds one as part of the load — 25 columns, derived from the query text rather than hand-listed, so
a query edit that reaches for a new column widens the projection instead of silently declining.

Measured at 1 M rows: the index costs **51.6 s to build** and takes data size from 1.357 to 1.557 GB
(+15%).

### Serving, and what each step bought

| state | served, of 43 |
|---|---|
| no projection (every earlier run) | 0 |
| projection + numeric group-by kernels + widened detection | 3 |
| \+ NE in the mask algebra | 6 |
| \+ string NE | 7 |
| \+ top-K group selection, flat partitioned tables, string ordering + contains predicates, grouped COUNT(DISTINCT), composite + transformed + constant group keys (2026-08-16) | 29 of 43 at ≤ 0.35 s hot |
| \+ string sorted scans + return-field, deferred grouped string MIN/MAX, `fn:not` in the mask algebra, conditional (CASE WHEN) keys, HAVING, `string-length` operands, ORDER BY `xs:double(avg)`, regex keys, 128-bit-sum aggregates, predicate scans (2026-08-16, later) | **every query serves; 43-query hot total 2.06 s** |

The 2026-08-16 kernel campaign took the 43-query hot total from **69.0 s to 2.06 s** (DuckDB:
0.351 s on the same box — 197× down to 5.9×). The ordered-group-by cap (`fn:subsequence` as the sole
consumer) reaches the kernel, which heap-selects the first K groups of the stable order instead
of sorting and materializing them all; group accumulators live in flat open-addressed tables merged
partition-parallel; the string arm groups by the 64-bit FNV hash of the value bytes and decodes
strings for the K winners only. Every served query is byte-identical to the interpreter.

The COLD regimes were then attacked separately (all three matter: cold one-shot, cold cache, warm).
The finding that reframed the work: SirixDB's cold cost was CPU, not I/O — the first query to fall
back to the row path deserialized every record page (~5.8 s at 1 M rows, identical with a cold and
a warm OS cache), while loading the projection itself costs ~0.3-0.5 s. Three declines triggered
that hydration and all three now serve: `avg(UserID)` (long-sum overflow → a 128-bit-sum kernel;
the interpreter merely promotes), `min/max(EventDate)` (the dict route now probes BEFORE the
type-discovery row scan), and Q19's point lookup (a predicate-scan route: mask → record keys in
document order → per-record materialization of just the matches). Fresh-process per-query
latencies after: 0.14-2.3 s with an fadvise-evicted cache, 0.13-1.9 s warm — the residual is the
JVM/JIT floor plus projection materialization, not data I/O (DuckDB: 0.02-0.10 s / 0.01-0.07 s;
its AOT-compiled binary has no such floor, and ours is blocked by oracle/graal#14255).

Q7 (`AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC`) is the clearest single case:
warm **1.318 s -> 0.032 s**, a 41x improvement, once NE stopped forcing it onto the row path.

Correctness is checked by running the same corpus with `-Dsirix.query.autoVectorize=false` and
diffing all 43 result sets: **42 of 43 byte-identical**. The exception is Q17, which is
`GROUP BY UserID, SearchPhrase LIMIT 10` with no `ORDER BY` — the two outputs are a permutation of
the same ten rows because group emission order is implementation-defined for that shape. The current
group-aggregate detector does recognize this order-free form and serves Q17 from the projection; the
permutation is not evidence of a Brackit-only fallback.

### What still declines, and why

* **String NE is unrepresentable** — `PredicateNode.StrEq` carries no operator, and `Not(StrEq)` is
  not equivalent: over a record MISSING the field the interpreter yields the empty sequence, which a
  filter reads as false, whereas a negated equality reads as true. This blocks the 14 `<> ''`
  queries and needs a new node variant. Numeric NE already works.
* **No string ordering comparison** — the leaves are NumCmp/FpCmp/DecCmp/StrEq/ArrayContains/BoolRef,
  so `EventDate >= '2013-07-01'` cannot be expressed and the whole July window (Q36-Q42) can never
  reach a vectorized route.
* **`contains()` is not a predicate leaf** (Q20-Q23), computed group keys decline (Q18, Q27, Q28,
  Q35, Q39, Q42), `COUNT(DISTINCT …)` has no kernel (Q8, Q9), and string min/max never routes to the
  projection (Q6, Q21, Q22).

The group-by side is no longer the constraint: numeric group keys have kernels now, and the
detection stage accepts the post-group-`let` + `order by` shape every analytical query is written in.
What is left is predicate vocabulary.

(One correction while we were in there: the "persisting wider projections trips a HOT-storage
chunk-split limitation" note in `COMPARISON_DUCKDB.md` is stale — it cites a `KNOWN_LIMITATIONS.md`
entry that no longer exists, and the widening guard test passes today.)

## The native image does not run this workload

A GraalVM native image of the runner **segfaults deterministically on Q2**, so there is no AOT number
here and no PGO number either. The diagnosis, since it is not what it first looks like:

* it is **not** PGO — a plain `-O3` image crashes identically, and PGO itself works end to end
  (instrument -> profile -> optimise, 188 MB -> 56.5 MB, `PGO: user-provided`) on Oracle GraalVM
  25.3.4.1-dev from the EA-builds channel;
* it is **not** a race (single-threaded crashes), **not** accumulated state (Q2 crashes in a fresh
  process while Q1 alone succeeds), **not** build-time initialisation
  (`--initialize-at-run-time` over the sirix packages changes nothing), and **not** alignment (the
  faulting instruction is `vmovdqu`, the unaligned form);
* the faulting load is `ByteVector.fromMemorySegment` in
  `ObjectKeyNameKeyRegion.findMatchingSlots`, reached from `parallelAggregate`. Instrumenting the
  fault site shows AOT seeing a segment **identical to the JVM's** — `native=true`, `size=1563`,
  `okCount=1012`, `dictIdsOff=551`, `lanes=32`, `lastRead=1562 < 1563`, live scope — and faulting
  anyway. The generated `vmovdqu (%rdi,%rbx,1)` uses the compressed-references **heap base** as
  `%rdi` and **segment-object address + 551** as `%rbx`, i.e. it addresses a native segment as if it
  were heap-backed; `551` is exactly `dictIdsOff`;
* forcing the scalar tail (`-Dsirix.pax.scalarOnly`, a throwaway patch) makes the same binary
  complete the query, and removing the flag crashes it again.

So the API use is legal and HotSpot runs it correctly: this is a native-image codegen bug. No
standalone reproducer yet — monomorphic, `ofAddress`+`reinterpret`, heap/native polymorphic, and
`Arena.ofAuto`+slice with the exact sizes all pass under AOT — so the trigger needs sirix's
compilation context. Reproduction is one clone, one `nativeCompile`, and
`cb-plain <db> --queries 2 --tries 1`, in about 90 s.

## Known gaps

* **Scale.** 1 M is the reproducible table; 100 M has been run on the author's host and is recorded
  in [At 100 M rows](#at-100-m-rows), but repeating it needs the real corpus and ~50 GB per database.
* **No AOT/PGO number** — see the native-image section; that one is blocked on a GraalVM bug
  (oracle/graal#14255), not on us.
* ~~Only 6 of 43 queries are projection-served.~~ As of the end of the 2026-08-16 campaign every
  query serves from the projection (group aggregates, sorted/predicate scans, scalar aggregates);
  no ClickBench query touches the row path anymore.
* **Q29** (ninety `SUM(ResolutionWidth + k)`): the DEFAULT variant is the one-pass form
  (`let $g := 1, $w0 …, $w89 … group by $g`); variant 1 is the ninety independent sums. (An earlier
  revision of this bullet said the reverse — reading it, one would "fix" Q29 by switching to the
  slower variant.) The one-pass form is now served by the constant-key group-aggregate route: every
  key being a literal-bound let means the grouping partitions nothing, so one scalar pass folds
  `count/sum/min/max` of `ResolutionWidth` and the ninety shifted sums are answered algebraically
  (`sum(f+k) = sum(f) + k·presentCount(f)`). Measured hot: 9.21 s → 0.054 s at 1 M rows,
  byte-identical to the interpreter.
* **Q41** returns the empty result below roughly 100 M rows even on real data — its `OFFSET 10000`
  needs more than 10 000 distinct `(WindowClientWidth, WindowClientHeight)` pairs inside a single
  `URLHash` slice.
* The port measures the **row-oriented** query paths. Nothing here is tuned; the point of this first
  pass was to make all 43 shapes run and prove the answers right.
