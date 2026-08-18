# JSONBench on SirixDB

The [JSONBench](https://github.com/ClickHouse/JSONBench) Bluesky workload — 1M firehose events,
five queries — ported to JSONiq over a SirixDB JSON resource, with a differential against the
ClickHouse reference answers.

Unlike ClickBench, this corpus is genuinely semi-structured: only `did`, `time_us` and `kind` are
present on every event, and the `commit` object (with `operation`, `collection` and a `record`
whose shape varies by collection) exists only on `kind = 'commit'` events. In the 1M file that is
994,672 commit events and 5,328 identity events; there are no `account` events.

## Layout

| What | Where |
| --- | --- |
| Schema, encoding contract | `io.sirix.query.bench.jsonbench.JsonBenchSchema` |
| Loader | `io.sirix.query.bench.jsonbench.JsonBenchLoadMain` |
| Projection index (5 columns) | `io.sirix.query.bench.jsonbench.JsonBenchProjection` |
| The 5 queries | `io.sirix.query.bench.jsonbench.JsonBenchQueries` |
| Runner | `io.sirix.query.bench.jsonbench.JsonBenchRunMain` |
| Differential | `bench/jsonbench/compare-results.py` |

The loader reuses `ClickBenchSource.open(...)` verbatim. That class is ClickBench-*named* but
format-generic: it fabricates the enclosing JSON array around a JSON-lines stream on the fly, which
is what the shredder consumes and what keeps the 480 MB uncompressed corpus from being buffered
whole.

## Running it

```bash
export TMPDIR=/tmp/claude-1000
export JAVA_HOME=~/.sdkman/candidates/java/25.0.3-graal

# Load (shred + projection). ~28 s shred + ~16 s projection on a 20-core laptop.
./gradlew :sirix-query:jsonBenchLoad \
    -Pjsonbench.args="/tmp/claude-1000/jb/db-1m /tmp/claude-1000/jb/data/file_0001.json.gz"

# Run the five queries and dump the answers.
./gradlew :sirix-query:jsonBench \
    -Pjsonbench.args="/tmp/claude-1000/jb/db-1m --tries 3 --dump /tmp/claude-1000/jb/dump-sirix-1"

# Differential against the ClickHouse reference.
python3 bundles/sirix-query/bench/jsonbench/compare-results.py \
    --dump /tmp/claude-1000/jb/dump-sirix-1 --ref /tmp/claude-1000/jb/ch-ref
```

Useful flags: `--queries 1,3-5` to select a subset, `--build-projection` to add the projection to an
already-loaded corpus, `--query-file F` to run one hand-written body against the same binding (it
prints the `# served:` counters too, which is the point of the hatch), and
`-Pjsonbench.jvmArgs="-Dsirix.query.autoVectorize=false"` to force the generic pipeline for a
correctness A/B.

## The reference side

ClickHouse 26.7.3.19 (`/tmp/claude-1000/jb/clickhouse-common-static-26.7.3.19/usr/bin/clickhouse`),
table `bluesky` under `/tmp/claude-1000/jb/ch-data`, loaded per `jsonbench/clickhouse/ddl.sql` —
the whole event in one `JSON` column named `data`, with `kind`, `commit.operation`,
`commit.collection` typed `LowCardinality(String)`, `did` as `String` and `time_us` as `UInt64`.
Reference answers live in `/tmp/claude-1000/jb/ch-ref/qN.tsv`.

### Q3 and Q4 are regenerated in UTC — read this before trusting them

ClickHouse's `toHour(fromUnixTimestamp64Micro(...))` and its `DateTime64` *formatting* both read the
session timezone. The references shipped with the rig were produced on a Europe/Berlin box, so Q3
answered hour **17** and Q4 printed `2024-11-21 17:25:49.000167`. Our Q3 is deliberately pure
integer arithmetic (`(time_us idiv 3600000000) mod 24`), which is timezone-free and equals the UTC
hour, and our Q4 emits raw microseconds.

Both references were therefore regenerated with `SETTINGS session_timezone='UTC'`, giving hour
**16** and `2024-11-21 16:25:49.000167`. The originals are kept beside them as `q3.berlin.tsv` and
`q4.berlin.tsv` so the discrepancy stays visible rather than looking like a fixed bug. To redo it:

```bash
CH=/tmp/claude-1000/jb/clickhouse-common-static-26.7.3.19/usr/bin/clickhouse
$CH local --path /tmp/claude-1000/jb/ch-data --query "<q3 from queries.sql> SETTINGS session_timezone='UTC' FORMAT TSV" \
    > /tmp/claude-1000/jb/ch-ref/q3.tsv
```

This also matters for the comparator: it parses Q4's printed timestamp back to microseconds, and
that parse is only well defined because the reference is UTC.

## Semantics that had to be matched deliberately

Each of these was measured against ClickHouse, not assumed.

**Absent paths print as the empty string, not NULL.** Because the reference schema types
`data.commit.collection` as `LowCardinality(String)`, an event with no `commit` object reads as
`''`. Q1's reference therefore has a row with an empty first column and count 5328. A JSONiq deref
of an absent path yields something that serializes as `null`, so Q1 wraps its group key in
`fn:string(...)`: `string(())` is `""`, exactly ClickHouse's substitution. (This has a cost — see
"Serving status" below.)

**Q5's span truncates each end to milliseconds before subtracting.** ClickHouse's
`date_diff('milliseconds', a, b)` counts unit boundaries crossed, so it is
`(max idiv 1000) - (min idiv 1000)`, not `(max - min) idiv 1000`. The two genuinely differ on this
corpus: for the top actor (min `…582101`, max `…589060`) they give 813007 and 813006 respectively,
and the reference says 813007.

**Neither LIMIT-3 boundary ties.** Exactly one actor holds Q4's third-smallest first-post
timestamp, and Q5's third span (811404) is well clear of the fourth (811016), so the differential
can require exact answers instead of tolerating an arbitrary tie-completion. Q1-Q3's ordering values
are all distinct on the 1M corpus as well. The comparator still handles ties correctly should a
different corpus produce them: it requires the *sequence of ordering keys* to match exactly and
compares rows as a multiset only *within* a run of equal keys, which is precisely the freedom SQL
leaves and no more.

## The projection index and the ambiguity guard

`JsonBenchProjection` declares five columns: `/[]/kind`, `/[]/did`, `/[]/time_us`,
`/[]/commit/collection` and `/[]/commit/operation` — the same five fields the ClickHouse schema
types explicitly.

Creating it originally failed:

```
Projected field name 'did' is ambiguous: it also occurs at a different path under the record set.
```

That is true of the corpus — `did` occurs at six further paths below `commit.record` (2285 events
carry `commit.record.facets[].features[].did`) and `collection` recurs at
`commit.record.skyfeedBuilder.blocks[].collection` — but the guard it tripped was protecting
against a hazard that no longer exists. Column lookup now matches a column by its declared path
*relative to the record root* (`ProjectionIndexRegistry.Handle#columnOf` compares the chains the
catalog derives from the definition), so a query dereferencing `$e.commit.record.did` produces the
token `commit/record/did` and simply finds no column. `CreateProjectionIndex#assertUnambiguousFieldNames`
was narrowed to match: it now checks only declarations that are *not relativizable* against the
declared root, which are the only ones whose lookup still degrades to bare-name matching. Two
declared fields still may not share a trailing name — that rule is about the projection's identity,
not its lookup, and is unchanged.

Verified on the real corpus: `count(... where exists($e.commit.record.did) ...)` returns **74**, the
true number of events carrying that nested field, while the top-level `did` exists on all 1,000,000.
The nested deref is not answered from the projected column.

## Serving status

**All five queries are served from the projection index** — one `groupAggregates` increment each, so
a three-try run reports `groupAggregates=15`. The answers are byte-identical to the generic
pipeline's (`-Dsirix.query.autoVectorize=false`) and both match the ClickHouse reference 5/5.

Three shapes had to be added to get there; each one had declined the WHOLE pipeline, so the
measurements below are per-query, not incremental:

| Query | What declined it | Hot: served vs generic |
| --- | --- | --- |
| Q1 | `string($e.commit.collection)` as the group key — the detection stage did not look through `fn:string` | 0.127 s vs 0.652 s (**5.1x**) |
| Q2 | the nested-deref `where` (`$e.commit.operation = "create"`) had no representable predicate | 0.553 s vs 1.234 s (**2.2x**) |
| Q3 | the computed second key `($e.time_us idiv 3600000000) mod 24`, plus the same nested `where` | 1.118 s vs 1.017 s (1.1x SLOWER) |
| Q4 | the nested `where` | 1.131 s vs 0.622 s (1.8x SLOWER) |
| Q5 | the nested `where` (the three-let span composition already served) | 1.097 s vs 0.617 s (1.8x SLOWER) |

Hot Σ is 4.03 s served against 4.14 s generic: **serving all five is a correctness result, not yet a
speed result.** The two that lose are Q4 and Q5, and they lose for the reason the ClickBench campaign
already recorded — they group ~1M rows by `did`, whose cardinality is essentially the row count, and
the string-dict group kernel is slower there than Brackit's own group-by. Q1 and Q2 group by
`commit.collection` (15 distinct values) and win by 5.1x and 2.2x. The residual is the kernel at
extreme group cardinality, not the query vocabulary.

### Diagnosing a decline

A declined pipeline used to be silent, indistinguishable from "no fast path exists". Run any query
with `-Pjsonbench.jvmArgs="-Dsirix.projDiag=true"` and the detection stage prints one line per
declined FLWOR naming the first shape element that failed:

```
[groupagg-decline] let: unmodelable pre-group binding: FunctionCall[string]
[groupagg-decline] where: selection is representable by neither Brackit's predicate tree nor a chain predicate
[groupagg-decline] pipe: no ForBind at the chain head
```

The third line is the harness's own `let $events := jn:doc(...) return (...)` wrapper, which is not a
group-by pipeline and is expected on every run. The same flag also enables the executor's and the
catalog's projection diagnostics, so one switch reports the whole route.

## Operational trap

Several agents in this session share one `GRADLE_USER_HOME=/tmp/claude-1000/gradle-home`. When two
gradle invocations overlap, the second dies after exactly 60 s with

```
Timeout waiting to lock journal cache (/tmp/claude-1000/gradle-home/caches/journal-1).
It is currently in use by another process.
```

This is lock contention, not a build error — retry. Every script here retries rather than reporting
a failure.
