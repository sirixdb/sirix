# JSONBench on SirixDB — the reproduction kit

[JSONBench](https://github.com/ClickHouse/JSONBench) is ClickHouse's JSON-analytics benchmark: real
Bluesky firehose events — deeply nested, variably shaped JSON — and five queries over event kind,
collection, user id and microsecond timestamps. This directory is everything needed to re-derive the
JSONBench numbers in [`docs/BENCHMARK_CAMPAIGNS.md`](../../../../docs/BENCHMARK_CAMPAIGNS.md) §5 on
your own machine: fetch the corpus, clean it, build the ClickHouse reference, load SirixDB, measure
both under one protocol, and check that the answers agree.

Unlike ClickBench, this corpus is genuinely semi-structured: only `did`, `time_us` and `kind` are
present on every event, and the `commit` object (with `operation`, `collection` and a `record` whose
shape varies by collection) exists only on `kind = 'commit'` events. In the 1 M file that is 994,672
commit events and 5,328 identity events; there are no `account` events.

---

## 1. Quickstart — one screen per tier

```bash
export JAVA_HOME=/path/to/graalvm-25            # GraalVM 25 (see Prerequisites)
KIT=bundles/sirix-query/bench/jsonbench
W=/var/tmp/jsonbench                            # anything with enough free space
CH=/path/to/clickhouse                          # the 26.7.3.19 static binary

# 1. corpus  (135 MB at 1m, 13 GB at 100m; resumable, idempotent)
$KIT/download-data.sh 1m $W/data

# 2. canonical cleaned corpus, shared by BOTH engines
$KIT/clean-corpus.py $W/data 1m                 # -> $W/data/bluesky-1m-clean.ndjson

# 3. ClickHouse reference: table, UTC reference answers, cold/hot baseline
$KIT/clickhouse-setup.sh 1m $W/data/bluesky-1m-clean.ndjson $CH $W

# 4. SirixDB: load if absent, then evicted cool-gated rounds + the differential
$KIT/run-benchmark.sh 1m $W/db-1m $W/ch-ref-1m --data $W/data/bluesky-1m-clean.ndjson

# 5. the ahead-of-time binary the published numbers use (three-step PGO, ~45 min)
$KIT/pgo-native.sh $W/db-1m --tier 1m --out $W/pgo
$KIT/run-benchmark.sh 1m $W/db-1m $W/ch-ref-1m --bin $W/pgo/jb
```

Substitute `10m` or `100m` for `1m` throughout. Step 4 on the JVM is a valid correctness run and a
rough timing; **only step 5 reproduces the published figures** — they are all ahead-of-time compiled.

Disk and time, per tier (measured on the campaign box, 20 threads, NVMe):

| tier | gz corpus | cleaned NDJSON | SirixDB | ClickHouse | sirix load | CH load |
| --- | --- | --- | --- | --- | --- | --- |
| 1m | 135 MB | 480 MB | 340 MB | 100 MB | 43 s | 11 s |
| 10m | 1.4 GB | 4.9 GB | 3.4 GB | 2.2 GB | ~6 min | ~4 min |
| 100m | 13 GB | 48 GB | 31 GB | 27 GB | ~43 min | ~26 min |

At 100m budget ~120 GB of free space and an afternoon. (ClickHouse's compression degrades badly with
scale — 100 MB at 1 M becomes 27 GB at 100 M, 12× its 10 M size — while SirixDB's global value
dictionary keeps its growth linear, which is why the two end up at near footprint parity.)

---

## 2. Prerequisites

| what | version used | note |
| --- | --- | --- |
| JDK | GraalVM 25 (`25.0.3-graal` via SDKMAN) | `JAVA_HOME` must point at it; the Vector API and FFM code need it |
| GraalVM for PGO | `graalvm-25.3.4.1-dev` EA build | GA toolchains crash the register allocator while instrumenting jline's FFM upcall stub; the kit already excludes jline from the instrumented image, so a GA toolchain works too |
| ClickHouse | **26.7.3.19**, static build | `clickhouse-common-static-26.7.3.19` from the GitHub releases of `ClickHouse/ClickHouse`, or `curl https://clickhouse.com/ \| sh`. Earlier versions lack the `JSON` type's v3 serialization |
| python3 | 3.11+ | standard library only |
| wget, gzip | any | corpus fetch and integrity check |
| lm-sensors | optional | without it the cool gate warns and continues (see §4) |

The kit runs the repo's `./gradlew` by default. On a box where `~/.gradle` is not writable, point it
at a distribution launcher instead:

```bash
export GRADLE=/path/to/gradle-9.7.0/bin/gradle
export GRADLE_USER_HOME=/var/tmp/gradle-home
export GRADLE_FLAGS="--offline --no-daemon"
```

---

## 3. What the kit contains

| file | what it does |
| --- | --- |
| `download-data.sh <tier> <dir>` | fetch `file_NNNN.json.gz` from the ClickHouse public bucket; resumable, and idempotent *offline* (an existing file is verified with `gzip -t`, not re-downloaded) |
| `clean-corpus.py <dir> <tier>` | concatenate the tier's files into one NDJSON, dropping unparseable lines and printing their line numbers |
| `clickhouse-setup.sh <tier> <corpus> <chbin> [workdir]` | DDL + load + UTC reference answers + the cold/hot ClickHouse baseline |
| `run-benchmark.sh <tier> <db> <ref>` | SirixDB: load if absent, N evicted cool-gated rounds, differential, scoreboard |
| `pgo-native.sh <db>` | the three-step instrument → collect → optimise native build |
| `queries.sql` | the five upstream ClickHouse queries, verbatim |
| `compare-results.py` | the differential: SirixDB's JSONL dump vs ClickHouse's TSV |
| `../common/evict.py` | the page-cache evictor (`--verify` proves it worked) |
| `../common/bench-common.sh` | cool gate, eviction, failure reporting, shared by both kits |

The SirixDB side itself lives in
`bundles/sirix-query/src/main/java/io/sirix/query/bench/jsonbench/`: `JsonBenchSchema` (encoding
contract), `JsonBenchLoadMain` (loader), `JsonBenchProjection` (the 5-column projection index),
`JsonBenchQueries` (the five queries in the exact FLWOR shape detection recognises) and
`JsonBenchRunMain` (the runner). The loader reuses `ClickBenchSource.open(...)` verbatim — that class
is ClickBench-*named* but format-generic: it fabricates the enclosing JSON array around a JSON-lines
stream on the fly, which is what keeps a 48 GB corpus from being buffered whole.

---

## 4. Measurement discipline

Every rule here was paid for with a wrong conclusion.

**Interleave arms; never measure them in blocks.** This laptop drops to **one seventh of its clock at
99 °C**. Measuring A as a block and B as a block once produced a clean, consistent and entirely
fictitious 1.7× regression, and code was reverted over it. `cold-rounds.sh` in the ClickBench kit
interleaves `A B A B`; `run-benchmark.sh` runs one arm, so compare two binaries by alternating whole
invocations, not by running four rounds of each.

**Cool-gate every timed run.** The gate waits until the CPU package is below 55 °C (`COOL_MAX_C`).
Without `lm-sensors` it warns once and continues — the kit stays usable, but on a thermally
unconstrained box treat single-arm comparisons as unreliable. A 40 W power cap on top of the gate is
what made the campaign's numbers repeatable to ~1 %.

**Min-of-N, including internal phase timers.** A single-sample phase timer on this box once
mis-attributed a change by 2.7× — it reported +73 ms where the truth was −17.7 ms. The kit reports
the minimum across rounds and the median beside it.

**Cold means evicted cache *and* a fresh process.** `common/evict.py` calls
`posix_fadvise(DONTNEED)` over every file of the target directory. That needs no root, and it evicts
exactly the files under test — unlike `echo 3 > /proc/sys/vm/drop_caches`, which needs root and drops
everything including the binary under test and the other engine's files, making interleaved arms
depend on the order they ran in. For the files being measured the two are equivalent; `evict.py
--verify` proves it per run by reporting page-cache residency before and after, measured with
`mincore(2)`. The script calls `sync(2)` first, because **fadvise cannot evict dirty pages** — without
that, the first "cold" run after a load silently measures a warm cache. Set
`BENCH_EVICT_FLAGS=--verify` to make every round in a measurement prove its own coldness:

```
  /var/tmp/jsonbench/db-1m: 6 files, 321.5 MiB on disk, cached 321.5 MiB -> 0.0 MiB
evicted 6 files, 321.5 MiB on disk; page cache 321.5 MiB -> 0.0 MiB (freed 321.5 MiB)
```

A non-zero residual is reported as a warning: another process still maps those pages, and that run is
not fully cold.

**Prove the route with counters, not with timing.** The runner prints `# served: …`. A route can
decline silently and the differential still passes — vacuously, because both legs then ran the same
pipeline. Check the counters. Two diagnosis cycles were also lost to reading `# served: 0/0/0` as "the
route is dead" when the print simply did not include that counter: check that the counter you are
reasoning about is in the line at all. `-Dsirix.projDiag=true` prints *why* a query declined.

**Isolated runs do not transfer to suite context.** A query measured alone pays fills and catalog work
that in the suite an earlier query already paid, and vice versa. Attribution runs must reproduce the
regime they explain.

**At 100 M, `ps` RSS is meaningless.** It counts resident *mapped database pages*: 23 GB of RSS was
2 GB of anonymous memory. Read the anonymous figure from `free` instead. (The one genuine kernel OOM
in the campaign was confirmed from `dmesg`, which reports `anon-rss` explicitly.)

**A CPU profile is the wrong instrument for a wall-clock cold question.** It over-weights a 20-thread
fold and hid a single-core 404 ms serial phase at 0.4 % of samples. Use wall-clock phase stamps to
find *where* the time is, and a CPU profile only to name *what* that phase is doing.

---

## 5. The per-tier configuration

`run-benchmark.sh` bakes this in; it is spelled out here so a deviation is visible.

| tier | loader flags | runner flags |
| --- | --- | --- |
| 1m | `-Xmx12g -Xms10g` | none (image defaults) |
| 10m | `-Xmx12g -Xms10g` | none (image defaults) |
| 100m | `-Xmx16g -Xms14g -Dsirix.offheap.bytes=8589934592` | `-Xmx14g -Xms12g -Dsirix.offheap.bytes=8589934592 -Dsirix.projection.promoteMaxBytes=0` |

* **`-Xms` is two gigabytes below `-Xmx` throughout.** A native image grows and zeroes its heap during
  try 1; at 100 M that is worth 120 ms min-of-2 and 250 ms mean. Configuration, not code. (The
  campaign's 1 M and 10 M native runs passed no heap flags at all and used the image's own default;
  the convention is applied at those tiers only for consistency and changes nothing measurable.)
* **`-Dsirix.offheap.bytes` bounds the off-heap arena.** Both benchmark runners initialise the
  allocator *before* opening the database, so the property takes effect. Any other path that opens a
  database without that early init inherits the size persisted in `dbsetting.obj` (16 GiB by default)
  and silently ignores the flag — the knob looks dead there, and a whole ledger of "offheap 8g"
  numbers was once recorded from runs that actually used a 16 GiB arena.
* **`-Dsirix.projection.promoteMaxBytes=0` is a workaround, not a tuning.** It disables the
  byte-kernel promotion at 100 M, where the promotion tries to materialise ~30 GB of row-group
  payloads and OOMs a 14 GB heap (open defect, task #36). **Remove it once #36 is fixed.** With
  promotion enabled, smaller tiers legitimately route Q4/Q5 to the byte kernel — which is why their
  `groupDense` counter reads zero and the 100 M runs' reads two.

---

## 6. Results being reproduced

Ahead-of-time (GraalVM native image) binaries with a **freshly collected** profile, cool-gated,
evicted page cache, min of two rounds, `--tries 3`. ClickHouse measured on the same machine, the same
cleaned corpus, and the same eviction protocol.

| tier | SirixDB cold | ClickHouse cold | SirixDB hot | ClickHouse hot |
| --- | --- | --- | --- | --- |
| 1 M | **0.098 s** | 0.187 s | **0.056 s** | 0.145 s |
| 10 M | **0.563 s** | 0.643 s | **0.180 s** | 0.484 s |
| 100 M | **2.61–2.82 s** | 4.16 s | **1.74–1.84 s** | 3.50 s |

Per query at 100 M (SirixDB cold/hot vs ClickHouse cold/hot, seconds): Q1 0.35/0.12 vs 0.11/0.11 ·
Q2 0.59/0.31 vs 2.18/1.92 · Q3 0.44/0.11 vs 0.80/0.53 · Q4 0.68/0.61 vs 0.49/0.43 ·
Q5 0.55/0.57 vs 0.58/0.52. Q2 is `uniqExact` over ~40 M distinct users and is **exact**, not
approximate, on both sides.

All tiers: **5/5 byte-equivalent answers** against the ClickHouse reference, matching answers from
SirixDB's own generic interpreter (`-Dsirix.query.autoVectorize=false`), and the ClickBench 43-query
suite byte-identical as a regression canary.

The 100 M figures use the config in §5 (`promoteMaxBytes=0`, 8 GiB arena, `-Xmx14g -Xms12g`). The
10 M and 1 M figures use the image defaults. Each binary is measured **on the format it was built
for**: the projection format changed several times during the campaign, and an older binary against a
newer database silently falls back to the row path (minutes, not milliseconds). Cross-format A/B in
one run is impossible; compare recorded numbers per format.

---

## 7. Traps, one line each

Ordered by how much time each one cost.

1. **A stale PGO profile makes the native binary under-read its own engine.** After landing any
   hot-path change, re-run all three steps of `pgo-native.sh` — a 40 % JVM win once showed as zero
   natively for this reason alone.
2. **Collect the profile at the tier you will measure**; a 1 M profile drives a 100 M binary into
   default AOT treatment on exactly the paths that matter at scale.
3. **`--queries` is ONE-based in the JSONBench runner and ZERO-based in the ClickBench runner.** Every
   "QN-only" label taken as zero-based was off by one for a whole session.
4. **`--tries N` is try 1 cold plus N−1 hot tries**; the hot figure is the best of tries 2..N.
   `--tries 1` gives a cold number and no hot number.
5. **Binaries are format-tied.** Never A/B two binaries built for different projection formats.
6. **Pipeline stages must gate on exit codes.** A 100 M projection build was OOM-killed, the pipeline
   stamped itself done, and every query silently ran the generic row path for a day. "31 GB on disk"
   is not "loaded" — check for the projection under `<db>/<name>/resources/<res>/indexes/`.
7. **Spilling queries need `-Djava.io.tmpdir=<writable>`**; without it the spill dies with a swallowed
   cause (`bit:BIDY0300`) naming nothing.
8. **The runner can swallow a query's failure cause** (open observability defect, task #34) — read the
   tail of the run log, which `run-benchmark.sh` prints on failure.
9. **Long detached runs get reaped on this rig.** Run timed arms as foreground chunks.
10. **Two gradle invocations sharing one `GRADLE_USER_HOME` deadlock for exactly 60 s** ("Timeout
    waiting to lock journal cache"). That is contention, not a build failure — retry.
11. **The published dataset is corrupt** at 10 M and above; see §8.
12. **ClickHouse's `toHour` and DateTime64 formatting are session-timezone dependent**; the references
    must be generated under UTC. `clickhouse-setup.sh` does this — see §8.
13. **A served route is not a fast route.** Counters prove routing; only phase-level attribution proves
    the route does the right work. A top-k pruner that never pruned and a filter that walked the whole
    document per row both passed every counter check.
14. **At 100 M, cold is CPU, not I/O.** A warm-page-cache control moved the evicted cold suite by 4 %.
    Every readahead lever is capped there; do not re-litigate it.
15. **Fold timings swing.** Hot Q2 once ranged 0.127–0.886 s across identical runs (GC from per-group
    hash sets). Min-of-many is mandatory for any fold A/B.

---

## 8. The two dataset semantics you cannot assume

### The published corpus contains corrupt records

Some files carry records **truncated at a 64 KiB buffer boundary**: the cut line is unterminated JSON
and the next line is its tail, so every incident costs two unparseable lines. Measured: **0** at 1 M,
**6** (3 incidents) at 10 M, **32** (16 incidents) at 100 M — leaving 1,000,000 / 9,999,994 /
99,999,968 rows.

ClickHouse's own JSONBench loader handles this by retrying the file with
`input_format_allow_errors_num = 1e9`, i.e. it **silently drops** what it cannot parse. That is fine
for a single-engine benchmark and fatal for a differential: the two engines would hold different row
sets and every count would differ by an unknown amount. `clean-corpus.py` drops the same rows once,
explicitly, prints their line numbers, and both engines load the result.

### The reference answers must be generated in UTC

`toHour(fromUnixTimestamp64Micro(...))` and `DateTime64` *formatting* both read the session timezone.
References produced on a Europe/Berlin box answer hour **17** where UTC answers **16**, and print
`2024-11-21 17:25:49.000167` where UTC prints `16:25:49.000167`. SirixDB's hour key is deliberately
pure integer arithmetic (`(time_us idiv 3600000000) mod 24`) — timezone-free, and equal to the UTC
hour — and it emits raw microseconds, so the comparator's parse back to microseconds is only well
defined against a UTC reference. `clickhouse-setup.sh` appends `SETTINGS session_timezone='UTC'` to
every reference query.

---

## 9. Semantics matched deliberately

Each was measured against ClickHouse, not assumed.

**Absent paths print as the empty string, not NULL.** Because the reference schema types
`data.commit.collection` as `LowCardinality(String)`, an event with no `commit` object reads as `''`.
Q1's reference therefore has a row with an empty first column and count 5328. A JSONiq deref of an
absent path serializes as `null`, so Q1 wraps its group key in `fn:string(...)`: `string(())` is `""`,
exactly ClickHouse's substitution.

**Q5's span truncates each end to milliseconds before subtracting.** ClickHouse's
`date_diff('milliseconds', a, b)` counts unit boundaries crossed, so it is `(max idiv 1000) − (min idiv
1000)`, not `(max − min) idiv 1000`. The two genuinely differ on this corpus: for the top actor (min
`…582101`, max `…589060`) they give 813007 and 813006, and the reference says 813007. The two forms are
not even order-preserving with respect to each other, so the aggregate must order by the truncated
form.

**Neither LIMIT-3 boundary ties** on the 1 M corpus, so the differential can demand exact answers. The
comparator still handles ties correctly on other corpora: it requires the *sequence of ordering keys*
to match exactly and compares rows as a multiset only *within* a run of equal keys — precisely the
freedom SQL leaves, and no more.

---

## 10. The projection index and the ambiguity guard

`JsonBenchProjection` declares five columns: `/[]/kind`, `/[]/did`, `/[]/time_us`,
`/[]/commit/collection` and `/[]/commit/operation` — the same five fields the ClickHouse schema types
explicitly.

Creating it originally failed with `Projected field name 'did' is ambiguous: it also occurs at a
different path under the record set`. That is true of the corpus — `did` occurs at six further paths
below `commit.record`, and `collection` recurs at `commit.record.skyfeedBuilder.blocks[].collection` —
but the guard was protecting against a hazard that no longer exists. Column lookup now matches a
column by its declared path *relative to the record root*, so a query dereferencing
`$e.commit.record.did` produces the token `commit/record/did` and simply finds no column;
`CreateProjectionIndex#assertUnambiguousFieldNames` was narrowed to check only declarations that are
not relativizable against the declared root. Verified on the real corpus:
`count(... where exists($e.commit.record.did) ...)` returns **74**, the true number of events carrying
that nested field, while the top-level `did` exists on all 1,000,000 — the nested deref is not
answered from the projected column.

---

## 11. Serving status and diagnosing a decline

All five queries are served from the projection index — one `groupAggregates` increment each, so a
three-try run reports `groupAggregates=15`. At 100 M the dense group table adds `groupDense=2`. The
answers are byte-identical to the generic pipeline's and match the ClickHouse reference 5/5.

A declined pipeline used to be silent, indistinguishable from "no fast path exists". Run with
`-Pjsonbench.jvmArgs="-Dsirix.projDiag=true"` and the detection stage prints one line per declined
FLWOR naming the first shape element that failed:

```
[groupagg-decline] let: unmodelable pre-group binding: FunctionCall[string]
[groupagg-decline] where: selection is representable by neither Brackit's predicate tree nor a chain predicate
[groupagg-decline] pipe: no ForBind at the chain head
```

The third line is the harness's own `let $events := jn:doc(...) return (...)` wrapper, which is not a
group-by pipeline and is expected on every run. The same flag enables the executor's and the catalog's
projection diagnostics, so one switch reports the whole route.

Other useful runner flags: `--queries 1,3-5` (one-based) to select a subset, `--build-projection` to
add the projection to an already-loaded corpus without re-shredding, `--query-file F` to run one
hand-written body against the same binding, and
`-Pjsonbench.jvmArgs="-Dsirix.query.autoVectorize=false"` to force the generic pipeline for a
correctness A/B.
