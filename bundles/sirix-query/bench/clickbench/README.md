# ClickBench for SirixDB — the DuckDB reference side

[ClickBench](https://github.com/ClickHouse/ClickBench) ported to SirixDB. This directory holds the
**reference engine** the SirixDB answers are diffed against, plus the data-preparation path for the
real dataset. The SirixDB side lives in
`bundles/sirix-query/src/main/java/io/sirix/query/bench/clickbench/`.

| file                  | what it does                                                                             |
|-----------------------|------------------------------------------------------------------------------------------|
| `queries.sql`         | the 43 canonical queries, byte-identical to ClickBench's `duckdb/queries.sql`, one per line |
| `prepare-data.sh`     | official `hits.parquet` → the JSON encoding SirixDB ingests                               |
| `duckdb_reference.py` | builds a DuckDB `hits` table from the parquet **or** from that same JSON, runs the 43 queries, dumps canonical results |
| `compare-results.py`  | diffs SirixDB's result dump against DuckDB's, telling a legitimate ORDER BY tie from a wrong answer |
| `run-differential.sh` | the correctness gate: drives all of the above (SirixDB fast path vs SirixDB interpreter vs DuckDB) |
| `cold-rounds.sh`      | the **performance** gate: evicted, cool-gated, interleaved cold rounds — the protocol that produced the published cold figures (see "Measuring" below) |

Requirements: the `duckdb` CLI (for `prepare-data.sh`) and the `duckdb` Python module (for
`duckdb_reference.py`). `compare-results.py` uses the standard library only. Verified against
DuckDB **1.5.2 (Variegata)**.

---

## The JSON encoding contract

Everything here — and `ClickBenchSchema.java` on the Java side — agrees on exactly this:

* **one JSON object per hit, all 105 columns always present, in `create.sql` column order**;
* the whole file is **ONE JSON ARRAY** of those objects, `[{...},\n{...}]` (SirixDB shreds a JSON
  array with gson's streaming `JsonReader`; the loader also accepts newline-delimited JSON — the
  shape of the official `hits.json.gz` — and frames it into an array on the fly);
* `SMALLINT`/`INTEGER`/`BIGINT` → JSON numbers, exact int64, never quoted, never floats;
* `TEXT`/`VARCHAR`/`CHAR` → JSON strings; `NULL` is coalesced to `""`, which is what ClickBench
  itself uses as the "missing" marker, so the file never contains a JSON `null`;
* `EventDate` → `"YYYY-MM-DD"`;
* `EventTime`, `ClientEventTime`, `LocalEventTime` → `"YYYY-MM-DDTHH:MM:SS"` (ISO-8601, `T`
  separator, second resolution, no timezone).

JSON has no date type, and ISO-8601 strings order lexicographically. That is the whole point of the
choice: `ORDER BY EventTime` and the `EventDate` range predicates stay plain string comparisons in
JSONiq, and `DATE_TRUNC('minute', t)` is `substring(t, 1, 16)`.

### int64 exactness — verified, not assumed

DuckDB's JSON writer (yyjson) serialises `BIGINT` through an integer path, not through a double:

```console
$ duckdb -c "COPY (SELECT 435090932899640449::BIGINT AS UserID,
                          9223372036854775807::BIGINT AS maxi,
                          (-9223372036854775807-1)::BIGINT AS mini)
             TO 'p.json' (FORMAT JSON, ARRAY true);"
$ cat p.json
[
	{"UserID":435090932899640449,"maxi":9223372036854775807,"mini":-9223372036854775808}
]
```

All 18 digits survive and both int64 extremes round-trip, so **no cast or quoting workaround is
applied** — the integers go out as bare JSON numbers. This matters: Q19 filters on
`UserID = 435090932899640449` and Q40/Q41 on `3594120000172545465` / `2868770270353813622`. A double
detour would have produced silently wrong answers rather than a load error.

### The official parquet does not store timestamps as timestamps

In `datasets.clickhouse.com/hits_compatible/athena/hits.parquet`, `EventTime`, `ClientEventTime` and
`LocalEventTime` are `INTEGER` unix-seconds and `EventDate` is `INTEGER` days-since-epoch — which is
why ClickBench's own `duckdb/load` applies `epoch_ms(EventTime*1000)` and `make_date(EventDate)`.
Both `prepare-data.sh` and `duckdb_reference.py` inspect the actual parquet column types and apply
the right conversion, so they work on the official file, on a re-exported typed parquet, and on one
whose temporal columns are already ISO strings. The three routes were checked to produce
byte-identical JSON.

---

## End to end with the real dataset

```bash
cd bundles/sirix-query/bench/clickbench

# 1. fetch the dataset (~15 GB, 100M rows). prepare-data.sh prints this exact
#    command and exits non-zero if the file is not there.
wget --continue --progress=dot:giga \
     -O hits.parquet https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet

# 2. parquet -> the JSON encoding above. Add a row count to work on a prefix;
#    the subset is the first N rows in physical file order (parquet
#    file_row_number), so it is reproducible across runs and thread counts.
./prepare-data.sh hits.parquet hits.json            # everything
./prepare-data.sh hits.parquet hits-10m.json 10000000

# 3. SirixDB: load, then run and dump results
java -cp <sirix-query classpath> io.sirix.query.bench.clickbench.ClickBenchLoadMain \
     /var/tmp/sirix-clickbench hits.json
java -cp <sirix-query classpath> io.sirix.query.bench.clickbench.ClickBenchRunMain \
     /var/tmp/sirix-clickbench --tries 3 --dump results-sirix

# 4. DuckDB, over the SAME JSON file, so the differential compares identical data
./duckdb_reference.py --source hits.json --format json \
    --db /var/tmp/hits.duckdb --out results-duckdb --tries 3

# 5. diff
./compare-results.py results-sirix results-duckdb
```

For the *performance* number, point DuckDB at the parquet instead — that is how ClickBench measures
it, and it avoids charging DuckDB for a JSON parse SirixDB does not pay at query time:

```bash
./duckdb_reference.py --source hits.parquet --format parquet \
    --db /var/tmp/hits.duckdb --out results-duckdb --tries 3
```

`prepare-data.sh` is idempotent: it writes to a temporary file, renames it into place atomically and
records `(source, size, mtime, rows)` in a `<output>.meta` sidecar. Re-running with the same inputs
is a no-op; `FORCE=1` overrides. `CHECK_NULLS=0` skips the audit pass that proves none of the 77
`NOT NULL` numeric/temporal columns is NULL (that audit costs one extra scan, which is worth
skipping only on the full 100M-row file).

Note the size: the full dataset in this JSON encoding is roughly **200 GB** (~2.1 kB per hit, all
105 keys spelled out on every object). Prefer a prefix (`prepare-data.sh hits.parquet hits.json N`)
unless you really need the whole thing.

## End to end with the synthetic generator (no download)

The Java side can generate the dataset offline, and `duckdb_reference.py` reads the very same file:

```bash
CP=<sirix-query classpath>

# 1. generate 1M hits (seed 42) in the JSON encoding
java -cp "$CP" io.sirix.query.bench.clickbench.ClickBenchGenerateMain hits-1m.json 1000000 42

# 2. SirixDB
java -cp "$CP" io.sirix.query.bench.clickbench.ClickBenchLoadMain /var/tmp/sirix-cb hits-1m.json
java -cp "$CP" io.sirix.query.bench.clickbench.ClickBenchRunMain  /var/tmp/sirix-cb \
     --tries 3 --dump results-sirix

# 3. DuckDB over the same file, then diff
./duckdb_reference.py --source hits-1m.json --format json --db :memory: --out results-duckdb
./compare-results.py results-sirix results-duckdb
```

(`ClickBenchLoadMain` also accepts `generate:<rows>[:seed]` directly, but then SirixDB and DuckDB
would each generate their own copy — write the file once and feed both from it.)

---

## `duckdb_reference.py`

```
--source <path>            hits.parquet, or the JSON array file SirixDB ingested
--format parquet|json
--rows N                   load only the first N rows (0 = all)
--db <path|:memory:>       :memory: by default; a file path is deleted first so a
                           stale table can never answer the queries
--out <dir>                default results-duckdb
--tries 3                  the ClickBench protocol
--queries <file>           default: queries.sql next to the script
--threads N                0 = DuckDB default
--column-spec <file>       JSON from ClickBenchSchema.duckdbColumnSpecJson();
                           cross-checked against the embedded type table
--only 0,7,42              run a subset (debugging)
```

* The `hits` table is created from the **verbatim `duckdb/create.sql` DDL**, so its column types are
  identical whichever source is used (checked: `DESCRIBE hits` matches `create.sql` on all 105
  columns including nullability).
* On the JSON path the columns are read with an **explicit `columns=` map** so DuckDB's sampler
  cannot guess `BIGINT` as `DOUBLE`; the four temporal columns arrive as ISO strings, are read as
  `VARCHAR` and `CAST` on insert.
* Every try calls `fetchall()` **inside** the timed region — DuckDB's streaming result would
  otherwise let the timer stop before the work is done.
* Outputs: `qNN.jsonl` per query (`NN` = 0-based ClickBench index, matching
  `ClickBenchQueries.byIndex(NN)`), `clickbench-results.txt` (the `Load time:` / `Data size:` /
  43 × `[t1, t2, t3],` block), and `summary.json`.
* A failing query is reported, written as `[null, null, null],` and makes the exit status non-zero;
  the other 42 still run.

Result canonicalisation, identical on both engines: integers keep every digit, floats are rounded to
**6 significant digits** (`%.6g`), NULL is `null`, timestamps/dates are the ISO-8601 strings above.
`ClickBenchRunMain.canonicalCell` does the same thing on the Java side.

## `compare-results.py`

```
./compare-results.py <sirix-results-dir> <duckdb-results-dir> [--queries queries.sql]
                     [--max-diff-rows N]
```

Rows are compared as a **multiset** — SQL `GROUP BY` has no inherent order, so line order carries no
information — with integers exact, floats to a relative 1e-9, strings exact, `null` equal only to
`null`. `int` vs `float` is compared numerically: JSON has one number type, so `1666` vs `1666.0` is
a serialisation artefact, not a wrong answer.

Whenever the ORDER BY key is resolvable, both sides are additionally checked to be **actually
ordered by it** — a multiset comparison alone would not notice an engine that returns the right rows
in the wrong order.

### The tie ambiguity, handled explicitly

`ORDER BY <agg> DESC LIMIT n [OFFSET k]` does not define a total order. Rows that tie on the sort key
may come back in any order, so two correct engines can keep different members of a tied group at the
window boundary. That is reported as **TIE-AMBIGUOUS**, not MISMATCH — but only when all of:

1. both results have the same number of rows;
2. the query has a `LIMIT` (read from `queries.sql`, so the judgement comes from the SQL and is not
   guessed from the data);
3. both sides carry the same sort key at the ambiguous boundaries;
4. **every** differing row, on both sides, has a sort key equal to one of those boundary keys.

The ambiguous boundaries are the **last** kept row always, plus the **first** kept row when the query
has an `OFFSET` — a window that starts part-way down can be cut through a tied group at its top edge
too, whereas with `OFFSET 0` the top of the window is the global maximum and is not ambiguous.

The sort key is taken from the SQL: the `ORDER BY` terms are matched against the `SELECT` list by
alias, by expression text, or by ordinal. Over the 43 queries that lands as:

* **29** resolve to exact output column indices (q07–q16, q18, q21, q22, q25, q27, q28, q30–q42);
* **10** have neither `ORDER BY` nor `LIMIT` (q00–q06, q19, q20, q29) — the full result is returned,
  so an exact multiset match is required and any difference is a MISMATCH;
* the rest cannot be decided from the result files alone, and are called out rather than hidden:

| query | why it cannot be decided from the result files | verdict |
|-------|------------------------------------------------|---------|
| Q17 `GROUP BY UserID, SearchPhrase LIMIT 10` | no `ORDER BY` at all — which ten groups come back is entirely engine-defined | TIE-AMBIGUOUS, marked `UNVERIFIABLE` |
| Q24, Q26 `ORDER BY EventTime ... LIMIT 10` | `EventTime` is not in the `SELECT` list, so the sort key is not observable in the output | TIE-AMBIGUOUS, marked `UNVERIFIABLE` |
| a window lying **entirely inside one tied group** (at synthetic scale the `PageViews` plateau of Q38–Q41 is all 1s) | every returned row is at a boundary, so two completely disjoint answers both pass the tie test — legal, but nothing was actually checked | TIE-AMBIGUOUS, marked `UNVERIFIABLE` |
| Q23 `SELECT *` | resolved: a star's `ORDER BY` terms are matched positionally against the 105 `hits` columns (`EventTime` is output index 4), so its ordering IS checked | as usual |

`UNVERIFIABLE` verdicts are counted in the summary line (`12 tie-ambiguous (5 unverifiable)`) and
listed by query, so a run can never be mistaken for fully checked.

The script fails loud rather than degrading quietly: a `queries.sql` that does not resolve to exactly
43 statements, a missing queries file, or a `ClickBenchSchema.java` whose column roster has drifted
from the one embedded here all abort with exit 2 — each of them would otherwise silently weaken the
comparison.

Exit status: `0` all MATCH/TIE-AMBIGUOUS, `1` at least one real MISMATCH, `2` no mismatches but at
least one MISSING result file or an unusable setup.

---

## Measuring: the cold protocol

`run-differential.sh` proves the answers. `cold-rounds.sh` produces the numbers, under the protocol
that every figure in [`docs/BENCHMARK_CAMPAIGNS.md`](../../../../docs/BENCHMARK_CAMPAIGNS.md) §4
obeys:

```bash
# one arm, JVM (correctness-grade timing; the published figures are ahead-of-time)
./cold-rounds.sh /var/tmp/sirix-clickbench

# two ahead-of-time images, interleaved A B A B, four rounds each
./cold-rounds.sh /var/tmp/sirix-clickbench \
    --arm base=/var/tmp/bin/cb-lm4 --arm new=/var/tmp/bin/cb-lm5 --rounds 4
```

It evicts the page cache before every round (`../common/evict.py`, `posix_fadvise DONTNEED`), waits
for the CPU package to fall below 55 °C, runs each arm in a **fresh process**, and reports the best
and median suite time per arm against the DuckDB reference (0.520 s cold / 0.351 s hot on the
campaign box; override with `--duckdb-cold` / `--duckdb-hot`).

**The published ClickBench numbers, for reference:** cold suite **0.986 s** best of 4 rounds (median
1.050) vs DuckDB 0.520 s — **1.90×**; hot suite **0.600–0.615 s** vs 0.351 s — **1.71–1.75×**. Both
from a GraalVM native image over a 1 M-row synthetic corpus with a 25-column projection index; the
answers are byte-identical to SirixDB's own generic interpreter, which is itself differentially
verified against DuckDB.

### The discipline these numbers depend on

Each rule below was paid for with a wrong conclusion; the JSONBench kit's
[README](../jsonbench/README.md#4-measurement-discipline) states them at length and they apply
identically here.

- **Interleave arms in one build.** Old, new, old, new — never two blocks. This laptop drops to one
  seventh of its clock at 99 °C, and block measurement once faked a 1.7× regression convincingly
  enough that code was reverted over it. `cold-rounds.sh` interleaves by construction; a 40 W power
  cap plus the cool gate keeps arms comparable.
- **Min-of-N for everything, including internal phase timers.** A single-sample phase timer once
  mis-attributed a change by 2.7× — reported +73 ms where the truth was −17.7 ms.
- **Cold means an evicted cache *and* a fresh process.** `posix_fadvise(DONTNEED)` needs no root and
  evicts exactly the files under test, unlike `drop_caches`, which needs root and drops the binary
  under test and the other engine's files too — making interleaved arms depend on their order.
  `evict.py --verify` reports residency before and after (via `mincore(2)`) so a run can prove it was
  cold. Note that fadvise cannot evict *dirty* pages, so `evict.py` calls `sync(2)` first; without
  that the first "cold" run after a load silently measures a warm cache.
- **Prove the route with counters, not with timing.** The runner prints `# served: …`. A route can
  decline silently and a differential still passes vacuously, because both legs then ran the same
  pipeline. And check that the counter you are reasoning about is actually in the printed line —
  "route gap" was twice diagnosed from a `0/0/0` that simply omitted it.
- **Isolated runs do not transfer to suite context.** A query measured alone pays fills and catalog
  work that, in the suite, an earlier query already paid. Attribution runs must reproduce the regime
  they explain.
- **Sweep selectivity when testing a predicate route.** A wrong-answer bug hid behind a common
  literal for a whole session; the error scaled with rarity, not with corpus size. Test
  common / mid / rare / no-match literals.
- **`--queries` here is ZERO-based** (`--queries 18` is the docs' Q19), while the JSONBench runner's
  is one-based. Mixing them up costs a run.

---

## Two real cross-engine semantics to know about

Neither is worked around; both would show up as a MISMATCH and both are the JSONiq side's call.

1. **`STRLEN` counts bytes, `string-length` counts code points.** Q27 (`AVG(STRLEN(URL))`) and Q28
   (`AVG(STRLEN(Referer))`) therefore agree only while the URLs are ASCII:

   ```console
   $ duckdb -c "SELECT strlen('ünïcode') AS bytes, length('ünïcode') AS chars;"
   bytes = 9,  chars = 7
   ```

2. **Q42's group key renders differently.** `DATE_TRUNC('minute', EventTime)` is a `TIMESTAMP` in
   DuckDB and canonicalises to `"2013-07-15T03:46:00"`, while the JSONiq port's
   `substring($h.EventTime, 1, 16)` produces `"2013-07-15T03:46"`. Appending `":00"` on the JSONiq
   side fixes it and changes nothing else — a constant suffix preserves the lexicographic order the
   query's `ORDER BY` relies on:

   ```
   let $m := substring($h.EventTime, 1, 16) || ":00"
   ```

   The comparison tool does catch this: truncating one side's Q42 key to 16 characters is reported as
   `MISMATCH … the boundary sort keys themselves differ`.

---

## How this was verified

No network access to `datasets.clickhouse.com` in the build sandbox, so the tooling was verified
against a locally generated stand-in with the **identical 105-column schema**, in two flavours (real
`TIMESTAMP`/`DATE`, and the official file's `INTEGER` epoch encoding):

* `prepare-data.sh` on both flavours produced **byte-identical** JSON: 20 000 objects, 105 keys each,
  same key order in every object, zero JSON nulls, zero floats, `435090932899640449` exact,
  `"2013-07-14T00:00:00"` / `"2013-07-14"`, and quote/backslash/tab/newline/non-ASCII payloads
  escaped and round-tripped.
* `duckdb_reference.py` ran **all 43 queries** with no errors on both the parquet and the JSON path;
  the two loads produced the same table (identical md5 over every row) and `DESCRIBE hits` matched
  `create.sql` exactly.
* At 20 000 rows Q27/Q28 (`HAVING COUNT(*) > 100000`) and Q41 (`OFFSET 10000`) are structurally
  empty; a 250 000-row run was added where **all 43 return rows**.
* `compare-results.py`: an identical copy gives 43 × MATCH (exit 0). A perturbed copy is caught in
  every shape tried — a changed scalar, a changed non-boundary row, a dropped row, a 1e-6 relative
  float drift, a NULLed value, a Q42 key truncated to 16 characters, a reversed or swapped ordering
  (exit 1) and a deleted file (exit 2) — while a 1e-12 relative float drift and a swap of two rows
  *tied at the cut-off* correctly stay MATCH / TIE-AMBIGUOUS.
* Running the two load paths against each other on the same data is itself a live tie test. The two
  loads hold provably identical rows (same md5 over the whole table) yet 23 of the 43 result files
  differ byte for byte, because DuckDB's parallel scan picks a different winner among rows tied at
  the cut-off. The tool reports **0 MISMATCH** — ~18 MATCH and ~25 TIE-AMBIGUOUS, the split moving
  from run to run exactly as the ties do.
