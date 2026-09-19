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

# 4. SirixDB: load if absent, then isolated cold/hot query rounds + the differential
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
| `clickhouse-setup.sh <tier> <corpus> <chbin> [workdir]` | DDL + durably synchronized load + UTC answers + the cold/hot ClickHouse baseline |
| `run-benchmark.sh <tier> <db> <ref>` | SirixDB: load if absent, N isolated cold/hot rounds, differential, scoreboard |
| `pgo-native.sh <db>` | the three-step instrument → collect → optimise native build |
| `queries.sql` | the five upstream ClickHouse queries, verbatim |
| `compare-results.py` | the differential: SirixDB's JSONL dump vs ClickHouse's TSV |
| `../common/evict.py` | the page-cache evictor (`--verify` proves it worked) |
| `../common/bench-common.sh` | cool gate, eviction, failure reporting, shared by both kits |

The SirixDB side itself lives in
`bundles/sirix-query/src/main/java/io/sirix/query/bench/jsonbench/`: `JsonBenchSchema` (encoding
contract), `JsonBenchLoadMain` (loader), `JsonBenchProjection` (the 5-column projection index),
`JsonBenchQueries` (the five queries in the exact FLWOR shape detection recognises) and
`JsonBenchRunMain` (the runner). The default `-Djsonbench.loader=parallel` path uses the generic
parallel bulk importer with node history and one-pass projection maintenance. It reads the NDJSON
stream through `ClickBenchSource.openParallelInput(...)`, which fabricates the enclosing JSON array
without buffering the corpus. `-Djsonbench.loader=gson` and `jackson` retain the sequential paths
for matched comparisons.

On the 100M Bluesky corpus, the parallel path loaded 99,999,968 rows in 3423.214 s versus
10274.520 s for the sequential path on the same host, an observed 3.00× speedup with 29.4% less
database space. Those full-scale runs occurred at different times; the matched 5M comparison measured
2.12×. The 100M load retained revision 1 and node creation history (129/129 sampled keys), and its
Q1–Q5 answers matched ClickHouse exactly. A separate 200k-row ClickBench differential passed all 43
queries. Its compact JSON summary of the numbers and rejected tuning screens is in branch history
(see the note at the end of this section).

The subsequent [constant-bucket counting experiment](evidence/20260914-constant-bucket-count-100m/README.md)
reduced warmed JVM Q3 time by 37% in an isolated comparison. The native candidate, also including
sorted-directory prefix jumps, measured 2.34–2.40× behind ClickHouse across the five-query hot score,
with exact results. That evidence separates the native protocol from warmed-JVM timings and records
the ClickBench regression screen.

The later [column-layout and sorted-group-summary candidate](evidence/20260914-column-layout-group-summaries/README.md)
measured **1.61–1.67× behind ClickHouse** at 100M with exact Q1–Q5 results. It retains versioning
and updates summaries per affected sorted leaf. The evidence includes the full summary audit,
matched smaller comparisons, ClickBench checks, and a rejected batch-read experiment.
[Writer dictionary ingestion work](evidence/20260914-writer-dictionary-ingestion/README.md) is measured
separately; allocation reductions alone are not ingestion throughput results.
The subsequent [query-local predicate-slice reuse](evidence/20260914-composite-predicate-reuse/README.md)
reduced native Q3 warm time by 16.55% on the same 100M database; all five results and the 43-query
ClickBench screen on the retained 200,000-row fixture remained exact.
The combined [parallel-directory and sorted-leaf-bounds candidate](evidence/20260914-parallel-directory-leaf-bounds/README.md)
then measured **0.85 / 0.81× ClickHouse's geometric score** at 100M (lower is better), with exact
results and preserved revision-1 source leaves. Q2, Q3, and Q5 still take longer individually;
the evidence records both the geometric score and the higher total elapsed time.
The subsequent [bounded writer readback cache](evidence/20260914-writer-readback-cache/README.md)
reduced matched 10M ingestion time by **32.0%** (1.47× throughput), including the cost of the
combined query improvements. A subsequent isolated 100M pair reduced ingestion time by **26.2%**
(1.35× throughput), from 44m 14s to 32m 39s. All query, bounds, layout and creation-history
checks passed. The final checksum-identity correction passed 106 focused tests and a separate
10M cost check (+0.69% in one pair). Final native warm ratios were **0.770 / 0.767× ClickHouse's
geometric score**; Sirix's total warm query time remains higher, chiefly due to Q3 and Q5.
All 43 ClickBench queries remained exact in ABBA and BAAB screens on 200,000 rows, with no
consistent timing regression. Disk-cold ranking is unverified because visible-file eviction
does not control the lower filesystem cache on this host's eCryptfs workspace.

The projection's sorted view is now declared by columns only — `kind`, `operation`, `collection`,
`did`, `time_us`, ClickHouse's `ORDER BY` for this table — and Q4/Q5's equality filter is served
as a key range of that view. The column-only view has been built at 100M: it loads in 35m13s to a
37,170,382,376-byte database, 6.54% larger than the earlier view's, and all five answers are exact
with Q4/Q5 served by the sorted prefix range. Because the earlier view stored only the rows matching
Q4/Q5's literals while this one holds every row, the load time, data size and Q4/Q5 figures quoted
elsewhere in this README still describe that earlier structure. On the measured column-only view,
Q4/Q5 cold/hot medians are 0.047/0.032 s and 0.108/0.074 s, against 0.046/0.027 s and 0.084/0.055 s
for the earlier one: still well ahead of ClickHouse, but slower than the filtered structure.

Databases built by earlier heads of this branch (up to and including commit `7a619dd20`), among them
the retained local 100M JSONBench databases, must be rebuilt. Their index catalogue still declares
the removed literal-filtered view, which this code rejects when the resource is opened, and their
sorted-view directory uses an older header format.

The generated JSON evidence artifacts (build archives, raw-file inventories, validation and summary
dumps) were removed from `evidence/`. The scripts, READMEs, evidence notes, checksums and raw
archives remain. The removed files are in this branch's history before commit `7a619dd20`, e.g.
`git show 7a619dd20^:bundles/sirix-query/bench/jsonbench/evidence/20260914-parallel-ingest-100m/summary.json`.

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

**Distinguish visible-file eviction from disk-cold reads.** `common/evict.py` calls
`posix_fadvise(DONTNEED)` over every file of the target directory. This needs no root and targets
the files under test. `evict.py --verify` reports their visible mapping residency before and after
with `mincore(2)`; every query also needs a fresh process. The script calls `sync(2)` first, because
**fadvise cannot evict dirty pages**. Set `BENCH_EVICT_FLAGS=--verify` to record this evidence:

```
  /var/tmp/jsonbench/db-1m: 6 files, 321.5 MiB on disk, cached 321.5 MiB -> 0.0 MiB
evicted 6 files, 321.5 MiB on disk; page cache 321.5 MiB -> 0.0 MiB (freed 321.5 MiB)
```

A non-zero residual means visible-page eviction was incomplete. Zero residency alone does not
prove physical disk-cold reads: this workspace uses [eCryptfs, a stacked filesystem](https://www.kernel.org/doc/html/latest/filesystems/ecryptfs.html),
and the lower filesystem cache is not controlled by this check. The final v68 comparison observed
millions of input blocks on a fresh database where earlier nominally cold runs read almost none.
Record filesystem type and process block I/O, retain the `cold` label's exact scope, and do not
equate this protocol with a global cache drop or use it to assert verified disk-cold ranking.

**Prove the route with counters, not with timing.** The runner prints `# served: …`. A route can
decline silently and the differential still passes — vacuously, because both legs then ran the same
pipeline. Check the counters. Two diagnosis cycles were also lost to reading `# served: 0/0/0` as "the
route is dead" when the print simply did not include that counter: check that the counter you are
reasoning about is in the line at all. `-Dsirix.projDiag=true` prints *why* a query declined.

**Every timed query is isolated.** For each query, the harness cool-gates and evicts the database,
then launches one fresh cold process and three fresh hot processes without another eviction. The
revisioned projection-header check runs inside each process's query timer; each query opens only the
projection data its serving route needs. Both scripts default to `ROUNDS=2` and `TRIES=4`;
the baseline records those values and the scoreboard refuses a mismatch or a legacy baseline.
Every timed ClickHouse query uses the same UTC session setting as reference generation. Attribution
runs must reproduce the complete protocol.

**Load time ends after durable synchronization.** Both loaders close their database writers and run
`sync(1)` inside the measured ingestion window, failing the load if the barrier fails. The ClickHouse
table uses plain `MergeTree`, so replicated-table synchronization commands are not applicable. Both
loaders report monotonic, millisecond-resolution wall time.

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
* **The loader creates a FILE_CHANNEL resource.** `JsonBenchLoadMain` selects
  `-DstorageType=FILE_CHANNEL` unless the flag names another backend, the same default
  `ClickBenchLoadMain` uses and the backend of the 100 M database the campaign measured. The store's own
  default on 64-bit Linux and macOS is MEMORY_MAPPED. Both backends write the load-time projection's
  pages out before the final commit; FILE_CHANNEL's preallocated profile also reuses an aborted load's
  tail instead of leaving it in the file. A database records its backend as `storageKind` in
  `<db>/<name>/resources/<res>/ressetting.obj`, and the runner opens it with that backend.
* **`-Dsirix.projection.promoteMaxBytes=0` is a workaround, not a tuning.** It disables the
  byte-kernel promotion at 100 M, where the promotion tries to materialise ~30 GB of row-group
  payloads and OOMs a 14 GB heap (open defect, task #36). **Remove it once #36 is fixed.** With
  promotion enabled, smaller tiers legitimately route Q4/Q5 to the byte kernel — which is why their
  `groupDense` counter reads zero and the 100 M runs' reads two.

---

## 6. Results being reproduced

Ahead-of-time (GraalVM native image) binaries with a **freshly collected** profile, cool-gated,
evicted page cache, min of two rounds, `--tries 3`. These are historical campaign figures from the
earlier whole-suite process protocol; new runs use isolated per-query processes and must not be
compared to these rows as though the protocols were identical.

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
14. **Cold-read bottlenecks depend on verified cache state.** An earlier 100M visible-eviction
    control moved by only 4%, but eCryptfs lower-cache state was uncontrolled. The fresh v68 database
    incurred substantial block I/O. That earlier control does not prove disk-cold reads are CPU-bound.
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
defined against UTC. `clickhouse-setup.sh` appends `SETTINGS session_timezone='UTC'` to every
reference and timed baseline query.

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
explicitly — and a sorted view ordered by `kind`, `operation`, `collection`, `did`, `time_us`. Both
the load-time declaration and the second-pass `jn:create-projection-index` call declare that view,
and the runner requires Q4 and Q5 to be answered from it.

Projection creation is part of the benchmark load contract and fails the loader by default. On the
explicit second-pass route, `-Djsonbench.projection.required=false` may retain a successfully shredded
resource after an index failure for later repair with `JsonBenchRunMain --build-projection`; timings
from that recovery mode are not benchmark results. The runner rejects a missing, stale, or
non-columnar projection before executing a timed query unless the diagnostic-only
`--allow-missing-projection` flag is supplied.

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
three-try run reports `groupAggregates=15`; Q4 and Q5 each also add one `sortedGroupBys` increment
per try. Unless `--allow-missing-projection` is given, the runner fails a suite whose counts do not
match the tries it ran. At 100 M the dense group table adds `groupDense=2`. The answers are
byte-identical to the generic pipeline's and match the ClickHouse reference 5/5.

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
hand-written body against the same binding, `--allow-missing-projection` for an explicitly
non-benchmark generic diagnostic, and
`-Pjsonbench.jvmArgs="-Dsirix.query.autoVectorize=false"` to force the generic pipeline for a
correctness A/B.
