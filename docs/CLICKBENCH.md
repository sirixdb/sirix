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
| `bundles/sirix-query/bench/clickbench/` | the DuckDB reference side: `prepare-data.sh`, `duckdb_reference.py`, `compare-results.py`, `run-differential.sh`, `queries.sql` |
| `bundles/sirix-query/src/test/java/io/sirix/query/bench/clickbench/` | the CI gates: generator invariants and a 43-query smoke test |

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

## The JSON encoding

ClickBench's rules forbid changing the data; they do not prescribe how a system types it, and every
entry maps the columns onto its own type system (MongoDB imports BSON int64s and dates,
Elasticsearch maps `long` and `date`, Druid keeps three of the four timestamps as strings). The
JSON encoding here is:

* one object per hit, **all 105 columns always present**, in `create.sql` order;
* integers as JSON numbers, **exact int64, never quoted** — `UserID` and the two hashes are 18-digit
  values that Q19/Q40/Q41 filter on by literal;
* text as JSON strings, with `NULL` coalesced to `""` (ClickBench's own missing marker);
* `EventDate` as `"YYYY-MM-DD"`, the three timestamps as `"YYYY-MM-DDTHH:MM:SS"`.

The date choice is load-bearing rather than cosmetic. JSON has no date type, and ISO-8601 orders
lexicographically, so `ORDER BY EventTime` and the `EventDate BETWEEN …` predicates stay plain string
comparisons, `extract(minute FROM EventTime)` is `xs:integer(substring($t, 15, 2))`, and
`DATE_TRUNC('minute', EventTime)` is `substring($t, 1, 16)`.

`ClickBenchLoadMain` re-reads the first record after ingest and **fails the load** if a 64-bit id
arrived as a string or a timestamp did not. This is not ceremony: ClickHouse's `JSONEachRow` quotes
64-bit integers by default, so the official `hits.json.gz` may well carry `"UserID":"435090932899640449"`.
That shreds as a string node, and then exactly three queries quietly return nothing while the other
forty look perfectly plausible.

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
(`PathStats.sum`), which serve `sum`/`avg` directly in the default configuration; there the fix
follows that file's existing doctrine and marks the statistic untrusted so the query falls back to
the scan. Regression test: `VectorizedAggregateExactnessTest`.

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

See [Numbers](#numbers) below. Scale caveat first, because it is the honest headline: the runs here
are at **1 M rows**, not ClickBench's 100 M. At 1 M rows the load takes ~63 s and occupies 1.36 GB,
which extrapolates to ~1.7 h and ~136 GB at 100 M — feasible, but not something this port has
executed yet. A 100 M run needs the real `hits.parquet` and a box with the disk to hold both the
JSON intermediate (~230 GB) and the database.

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
  [What the projection actually serves](#what-the-projection-actually-serves).

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
| \+ NE in the mask algebra | **6** |

Q7 (`AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC`) is the clearest single case:
warm **1.318 s -> 0.032 s**, a 41x improvement, once NE stopped forcing it onto the row path.

Correctness is checked by running the same corpus with `-Dsirix.query.autoVectorize=false` and
diffing all 43 result sets: **42 of 43 byte-identical**. The exception is Q17, which is
`GROUP BY UserID, SearchPhrase LIMIT 10` with no `ORDER BY` — the two outputs are a permutation of
the same ten rows, it is served by brackit's own `VectorizedGroupByExpr` rather than by a projection
route, and group emission order is implementation-defined for that shape.

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

* **Scale.** 1 M measured; 100 M not yet run.
* **No AOT/PGO number** — see the native-image section; that one is blocked on a GraalVM bug
  (oracle/graal#14255), not on us.
* **Only 6 of 43 queries are projection-served.** The remaining gap is predicate vocabulary, not
  group-by machinery — see [What the projection actually serves](#what-the-projection-actually-serves).
* **Q29** (ninety `SUM(ResolutionWidth + k)`) dominates the total in the default variant because each
  sum is its own pass over the column. Variant 1 computes all ninety in one pass.
* **Q41** returns the empty result below roughly 100 M rows even on real data — its `OFFSET 10000`
  needs more than 10 000 distinct `(WindowClientWidth, WindowClientHeight)` pairs inside a single
  `URLHash` slice.
* The port measures the **row-oriented** query paths. Nothing here is tuned; the point of this first
  pass was to make all 43 shapes run and prove the answers right.
