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
| `hft_gc_gate.py`      | dependency-free fixed-heap ingest gate: rejects old/full GC and unbounded post-young occupancy |
| `hft_maintenance_gate.py` | validates actual projection-maintenance boundaries and cold historical results |
| `hft_saturation_gate.py` | validates bounded worker-only p=1/q=1 append saturation and cold reopen |
| `hft_campaign_gate.py` | requires one identity-bound campaign across every versioning type and saturation |
| `test_hft_gc_gate.py` | standard-library unit tests for the GC-log parser and per-run/cross-scale verdicts |

Requirements: the `duckdb` CLI (for `prepare-data.sh`) and the `duckdb` Python module (for
`duckdb_reference.py`). `compare-results.py` uses the standard library only. Verified against
DuckDB **1.5.2 (Variegata)**.

---

## The JSON encoding contract

Everything here — and `ClickBenchSchema.java` on the Java side — agrees on exactly this:

* **one JSON object per hit, all 105 columns always present, in `create.sql` column order**;
* the whole file is **ONE JSON ARRAY** of those objects, `[{...},\n{...}]`; the loader also accepts
  newline-delimited JSON — the shape of the official `hits.json.gz`. File sources default to the
  general chunked parallel importer. A streaming delimiter adapter frames LDJSON as an array, and an
  allocation-stable byte filter exposes the official stream's quoted `BIGINT` values and
  space-separated timestamps as the canonical token types. Neither adapter buffers a record or
  writes a second input file. `-Dclickbench.parallelImport=false` retains the Jackson cursor path;
* `SMALLINT`/`INTEGER`/`BIGINT` → JSON numbers, exact int64, never quoted, never floats;
* `TEXT`/`VARCHAR`/`CHAR` → JSON strings; `NULL` is coalesced to `""`, which is what ClickBench
  itself uses as the "missing" marker, so the file never contains a JSON `null`;
* `EventDate` → `"YYYY-MM-DD"`;
* `EventTime`, `ClientEventTime`, `LocalEventTime` → `"YYYY-MM-DDTHH:MM:SS"` (ISO-8601, `T`
  separator, second resolution, no timezone).

`ClickBenchSchema.java` is the authoritative definition of this encoding; where this prose and
the code disagree, the code wins.

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
java -Dclickbench.expectedRows=99997497 -Dclickbench.projection=true \
     -Dclickbench.projection.incremental=true -DbuildPathSummary=true \
     -cp <sirix-query classpath> io.sirix.query.bench.clickbench.ClickBenchLoadMain \
     /var/tmp/sirix-clickbench hits.json
java -cp <sirix-query classpath> io.sirix.query.bench.clickbench.ClickBenchRunMain \
     /var/tmp/sirix-clickbench --tries 3 --dump results-sirix

# 4. DuckDB, over the SAME JSON file, so the differential compares identical data
./duckdb_reference.py --source hits.json --format json \
    --db /var/tmp/hits.duckdb --out results-duckdb --tries 3

# 5. diff
./compare-results.py results-sirix results-duckdb

# Full correctness gate over the official 99,997,497-row corpus. DuckDB's
# database and spill files must stay inside the dedicated work directory.
work=/var/tmp/clickbench-differential
sirix_resources="-Xms4g -Xmx4g -XX:MaxDirectMemorySize=1g -Dsirix.offheap.bytes=8589934592 -Dsirix.allocator=frame -Dsirix.arena.strategy=shared"
HITS_JSON=/path/to/hits.json EXPECTED_ROWS=99997497 REQUIRE_AUTO_GLOBAL_DICT=1 \
SIRIX_LOAD_JVM_ARGS="$sirix_resources" SIRIX_QUERY_JVM_ARGS="$sirix_resources" \
DUCKDB_DB="$work/hits.duckdb" DUCKDB_TEMP_DIRECTORY="$work/duckdb-tmp" \
DUCKDB_MEMORY_LIMIT=12GB DUCKDB_THREADS=4 \
./run-differential.sh 99997497 "$work"
```

The default parallel path requires `hashType=NONE` and `storeNodeHistory=false`; both are already the
ClickBench defaults. A non-standard hashed or temporal-history load must set
`-Dclickbench.parallelImport=false`. The loader checks this before allocating off-heap memory or
opening the target.

`run-differential.sh` always loads with `clickbench.projection=true`,
`clickbench.projection.incremental=true`, and the exact `buildPathSummary=true` property. For a file
source it requires an explicit positive expected-row count (the first argument or `EXPECTED_ROWS`)
and forwards it as `clickbench.expectedRows`; the official corpus value is **99,997,497**. This lets
the global-dictionary election reject unsuitable high-cardinality columns before ingestion instead
of abandoning the projection partway through. The script captures both loader streams and aborts
before querying if it sees `[proj] PROJECTION ABANDONED`. The loader's existing persisted projection
acceptance check also verifies its final row count against the same positive hint; the script does
not add a repair or post-load index build.

AUTO applies `sirix.projection.globalDict.budgetBytes` as one aggregate, not as an equal slice for
every declared string column. After the bounded leading sample it ranks only worthwhile, V0-safe
candidates by sampled local-dictionary pressure (then smaller reservation and column order), reserves
at least twice each projected footprint per simultaneously resident structure, and admits the
deterministic subset that fits. A streaming column therefore reserves a combined four-times envelope
which is split into disjoint generation-writer and whole-load probe-front halves; neither structure
can spend the other's cap. Any spare aggregate is divided among the selected combined envelopes
without letting their finite ceilings sum past the configured total. Thus unrelated low-cardinality
strings cannot make a safe dictionary fail a per-column pre-gate; both structures' byte/structural
checks remain runtime fail-closed boundaries.

Both Sirix query legs are route-gated, not inferred from their timings. The fast leg passes
`--require-vectorized-serving`; for every one of the 43 fully serialized queries the runner snapshots
all outcome-level serving counters and requires at least one positive delta, then prints the exact
route names in that query's `note` column. Lookup, attempt, decline, and page-read counters do not
qualify. The generic leg passes `--require-generic-serving` and requires every such delta to remain
zero. Therefore a silent fast-path decline and an accidentally vectorized "generic" leg both stop the
campaign before DuckDB runs.

Set `REQUIRE_AUTO_GLOBAL_DICT=1` when AUTO is part of the campaign claim. The script appends
`-Dsirix.projection.globalDict=auto` after caller JVM flags and parses the loader's completed-build
banner; a missing banner or `globalDictColumns=0` is fatal. This is intentionally opt-in because a
corpus whose sampled string columns do not justify a resource-wide dictionary is a valid AUTO
outcome, but it cannot be used as evidence for global-dictionary performance.

The loader snapshots the process-wide HOT mutation counters immediately before `store.create*` and,
after the two cold persisted projection reopens, emits one per-load evidence record:

```text
# HOT_INCREMENTAL_DELTAS COMPLETE_STRUCTURAL_FRONTIER_SPLICE=0 STRUCTURAL_VALIDATION_FAILURE=0 STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE=0 MUTATION_TRAVERSAL_REFUSED=0 STRUCTURAL_VALIDATION_OVERSIZE_SKIPPED=0
```

The values are deltas, so earlier work in a reused JVM is excluded. A complete structural-frontier
splice is a legitimate bounded incremental operation and is reported rather than rejected. Every
load fails before publishing `Load time` if a published structure fails validation, height
propagation cannot be preflighted, or a mandatory mutation traversal is refused. Oversized
defense-in-depth validation scopes are also disclosed; the route-local mandatory guard still runs.
There is no subtree-rebuild counter anymore because no subtree-rebuild mutation entry point remains
in production.

`SIRIX_LOAD_JVM_ARGS` and `SIRIX_QUERY_JVM_ARGS` are whitespace-delimited append points for the
fixed-heap, direct-memory, allocator, off-heap, GC, and HFT flags used by a particular campaign. This
is necessary because the Gradle tasks' general-purpose defaults are `-Xmx12g` and both Java mains
otherwise reserve 24 GiB off heap. A later `-Xmx4g` and `-Dsirix.offheap.bytes=...` in these variables
override those defaults; the fixed-heap HFT section below gives the complete canonical loader string.
The script appends its mandatory expected-row/projection/path-summary properties after
`SIRIX_LOAD_JVM_ARGS`, and appends `autoVectorize=true` or `false` after `SIRIX_QUERY_JVM_ARGS`, so an
earlier duplicate cannot silently disable the projection or collapse the two query legs into the
same route.

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

## Fixed-heap HFT GC/safepoint gate

`hft_gc_gate.py` is the fast paired-corpus development gate for ingestion memory stability. Its
defaults remain the 1 M/4 M pair; `--small-log`, `--large-log`, `--small-rows`, and `--large-rows`
make the measured sizes explicit for a different pair. The legacy `--one-million` and
`--four-million` log aliases remain accepted. This is not a throughput benchmark, and it does not
infer anything from process startup or validation: the parser accepts GC/safepoint records only
between the loader's exact `# HFT_MEASURE_START` and `# HFT_MEASURE_END` lines. Missing or duplicate
markers fail the gate. After the 20% startup warmup, every collection through the end marker
participates in the plateau and slope checks; the gate does not discard a fixed tail that could hide
late promotion.

Run both sizes in fresh processes, with the same fixed Java heap, off-heap budget, auto-flush window,
seed, and GC configuration. The output directories below are newly created and are deliberately not
deleted by the commands:

```bash
gate_root=$(mktemp -d /tmp/sirix-hft-gate.XXXXXX)
hft_sha=$(git rev-parse HEAD)
load_classpath=$(./gradlew -q :sirix-query:printClickBenchRuntimeClasspath)
common_jvm="-XX:+UseG1GC -Xms4g -Xmx4g -XX:G1HeapRegionSize=4m -XX:MaxNewSize=1g -XX:+AlwaysPreTouch -XX:+DisableExplicitGC -XX:-G1UseAdaptiveIHOP -XX:InitiatingHeapOccupancyPercent=45 -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m -XX:+ExitOnOutOfMemoryError -XX:MaxDirectMemorySize=1g -XX:-UseJVMCICompiler -DstorageType=FILE_CHANNEL -Dsirix.allocator=frame -Dsirix.offheap.bytes=8589934592 -Dsirix.arena.strategy=shared -Dsirix.asyncFlush.parallelism=2 -Dsirix.asyncFlush.appendParallelism=2 -Dsirix.asyncFlush.sidePageBytes=67108864 -Dsirix.asyncFlush.sidePageCount=131072 -Dsirix.asyncFlush.stallTimeoutMillis=30000 -Dsirix.hft.telemetry=true -Dsirix.hft.gitSha=$hft_sha -Dsirix.autoCommit.nodes=4194304 -Dsirix.projection.globalDict=never -Dclickbench.parallelImport=true -Xlog:gc*,gc+heap=debug,gc+humongous=debug,safepoint:stdout:uptime,level,tags"

./gradlew --no-daemon :sirix-query:clickBenchLoad \
  -Pclickbench.args="$gate_root/db-1m generate:1000000:42" \
  -Pclickbench.jvmArgs="$common_jvm" \
  > "$gate_root/1m.log" 2>&1

./gradlew --no-daemon :sirix-query:clickBenchLoad \
  -Pclickbench.args="$gate_root/db-4m generate:4000000:42" \
  -Pclickbench.jvmArgs="$common_jvm" \
  > "$gate_root/4m.log" 2>&1

python3 bundles/sirix-query/bench/clickbench/hft_gc_gate.py \
  --small-log "$gate_root/1m.log" \
  --large-log "$gate_root/4m.log" \
  --small-rows 1000000 \
  --large-rows 4000000 \
  --expected-git-sha "$hft_sha" \
  --runtime-classpath "$load_classpath"
```

The extra `-Xms4g -Xmx4g` arguments occur after the ClickBench Gradle task's defaults, so the
measurement JVM really has a fixed 4 GiB heap. `-XX:+DisableExplicitGC` ensures a forbidden old/full
event reflects organic pressure rather than a diagnostic `System.gc()` call. Keep
`-DstorageType=FILE_CHANNEL`: 64-bit Linux otherwise defaults to `MEMORY_MAPPED`, while safe
side-page prewrite is capability-gated to the preallocated file-channel writer. Omitting the flag
would measure the fallback and can retain payload that the intended path releases.
`-XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m` likewise belongs to the measurement contract:
without the initial metaspace headroom, deterministic class loading can trigger a G1
`Metadata GC Threshold` concurrent cycle just after the start marker and make every otherwise
healthy run fail. The forbidden-event rule remains unchanged.

The explicit `-XX:G1HeapRegionSize=4m` is also part of the canonical 4 GiB profile. With G1's
automatic 2 MiB regions, the JDK's `ZipFile` class loader retains the 1,469,478-byte central
directory of the full `fastutil` JAR in a 1,469,496-byte `byte[]`; that crosses the 1 MiB
humongous threshold before Sirix begins ingesting. Four-megabyte regions move that immutable
classpath table below the 2 MiB threshold without weakening the gate's zero-humongous rule. This
is an effective-runtime setting, so the emitted `g1RegionSizeBytes` must be exactly 4 MiB and every
region-size value present in the unified heap log must agree or the gate fails closed.

`-XX:MaxNewSize=1g` remains the canonical production/development profile: it gives short-lived
async serialization graphs enough nursery lifetime to die young without hiding unbounded retained
occupancy in the 4 GiB old generation. The 4,194,304-node value remains the logical auto-commit
threshold, while `KEEP_OPEN_ASYNC_FLUSH` caps every storage-only epoch at 16,384 modifications and
the transaction-intent log may rotate earlier at 16 live entries. The emitted
`asyncFlushNodeCap=16384` comes from the engine's active mode policy; it is independent of the
separately configured `sirix.asyncFlush.sidePageCount=131072` object bound. The bounded append permit still limits
overlap. Both prefixes use
`-Dsirix.projection.globalDict=never`; otherwise dictionary election changes with the generated row
count (and the 1 M/4 M arms no longer model the dictionary shape of the 100 M target).

After an allocation reduction, the canonical profile can produce no organic young collections. A
genuine zero-event arm is valid latency/GC-safety evidence because the producer emits the effective
heap, region size, and enabled GC/safepoint selectors; malformed or absent evidence still fails
closed. It is nevertheless `INCONCLUSIVE` for retained-occupancy behavior because there are no
post-young observations. The same applies when fewer than the unchanged 5/20 sample floors occur.
Overall acceptance then requires a larger retention arm or a separate bounded-retention proof.
Changing the nursery is a different, fail-closed measurement contract and needs its own conclusive
ordinary-GC pair.

The measured 256 MiB-nursery investigation is therefore negative evidence, not an alternative
acceptance profile. In fresh 4 M and 12 M runs, the 4 M arm passed by itself, but the 12 M arm never
formed the required post-young occupancy plateau, so the strict paired verdict was **FAIL**. The
generalized invocation used for those logs is:

```bash
evidence_root=/path/to/existing-256m-logs
python3 bundles/sirix-query/bench/clickbench/hft_gc_gate.py \
  --small-log "$evidence_root/4m.log" \
  --large-log "$evidence_root/12m.log" \
  --small-rows 4000000 \
  --large-rows 12000000 \
  --expected-git-sha "$hft_sha" \
  --runtime-classpath "$load_classpath" \
  --expected-max-new-mib 256
```

The explicit `--expected-max-new-mib 256` only binds the parser to what those runs measured; it does
not turn the failed profile into a recommendation. Omitting the flag for 256 MiB logs, or using it
with 1 GiB logs, fails because `maxNewSizeBytes` must match exactly. Separate active-load 4 M/12 M
forced-full-GC runs and their post-collection class histograms are useful retention diagnostics, but
they can never count as gate acceptance: the forced full collection is itself a forbidden event.
Only unmodified ordinary-GC logs can establish a passing verdict.

At low retained occupancies, ordinary G1 survivor-target movement is quantized in heap regions and
can exceed three percent without representing old-generation growth. The gate therefore parses the
effective G1 region size from the runtime configuration and cross-checks every logged value. A candidate
plateau may span `max(3% of its median, 3 G1 regions)`, but its positive local projected OLS growth
must still remain within three percent of its median. The region allowance covers bounded survivor
jitter; it cannot make a monotonic ramp into a plateau. All samples after the candidate still pass
through the unchanged late/early median, last-decile, final-half slope, and cross-scale checks.

The deterministic native side-page path requires `-Dsirix.arena.strategy=shared`. `auto` delegates
reclamation to a Cleaner and `global` never reclaims, so both deliberately fall back to the ordinary
resident final-commit path. The loader resolves the effective HotSpot `MaxNewSize` and arena strategy
before measurement, then emits exactly one `# HFT_CONFIG` record inside the boundary. The parser
requires the canonical dictionary, logical auto-commit, `asyncFlushNodeCap=16384`, exact 4 MiB G1
region, arena, storage, `importer=parallel-bulk`, projection-mode, and row-count values. It also requires
`versioningType=FULL` and the resolved pinned-trie limits `pinnedTrieScanBudget=1024` and
`pinnedTrieBatchCapacity=64`. Changing or omitting one fails closed instead of silently measuring a
different workload or an unbounded flush/spill scan.

The gate exits non-zero if either log contains a full collection, concurrent old-generation cycle,
remark/cleanup, prepare-mixed/mixed collection, to-space exhaustion, evacuation/allocation failure,
allocation stall, preventive/humongous-allocation collection, or OOM. It also requires:

* parseable GC and safepoint evidence with the effective heap and G1 region size embedded in the
  configuration; zero events are valid latency/safety evidence but make retained occupancy
  `INCONCLUSIVE`, while malformed event lines fail closed;
* exactly one async-append telemetry record proving that projection side pages really used the
  bounded path, that two fixed 64 MiB native reservoirs (128 MiB total) were initialized, that active
  payload stayed within one reservoir, that every async KVL identity encoding was copied from its
  scoped serializer directly into the disposable native page frame with zero heap/capacity fallback,
  and that every epoch completed. The KVL proof is fail-closed: `kvlAttemptedPages` must equal
  `kvlPages + kvlPromotedPages`, `kvlPromotedPages` must be zero,
  `kvlAttemptedPagesMax` must be nonzero when pages were attempted, no greater than both 16 and the
  total attempted pages, and large enough that its product with the combined-epoch count covers the
  aggregate attempts; the
  frame/fallback split must account for every appended `kvlPages` page. Thus an over-capacity or
  unresolved page promoted back into the live TIL cannot disappear from the profiler verdict.
  `foregroundFlushCount` must equal the combined-epoch count, its total must cover its maximum, and its
  maximum measures the complete foreground index-maintenance plus async-rotation call. Rotation and explicit-drain permit
  counts/totals/maxima are separate required fields, and their sums/max must reproduce the aggregate
  counters exactly. Append submission wait count/total/maximum are required too, and
  `callerThreadAppendRuns` must be zero: saturated submission may backpressure, but the append itself
  remains worker-owned. The same record must prove the foreground structural path was exercised
  (`pinnedTrieSpillPages > 0`) and stayed inside its fixed 64-page capture buffer. Spill epoch/page
  counts and batch maximum are hard evidence; `pinnedTrieLiveMax` and append-only
  `pinnedTrieHighWater` are reported for measurement without imposing an unmeasured
  retained-occupancy limit;
* no permit, rotation, drain, worker, submission, whole `startAsyncFlush`, complete foreground
  async-flush, final-drain, young-GC, or safepoint stall above the immutable 250 ms canonical cap;
  `--max-permit-wait-ms` may tighten that cap, and `--expected-side-batch-mib` changes the payload
  contract;
* a real post-young occupancy plateau after discarding only the first 20% of young collections;
  every remaining sample through `# HFT_MEASURE_END` is ingestion evidence, including a growing
  final tail;
* at least 5 post-warmup and post-plateau samples for the smaller cross-scale baseline, and 20 for the
  larger proof run (`--min-small-samples` / `--min-large-samples` can raise these floors);
* the expected fixed 4 GiB capacity (`--expected-heap-gib` changes the contract explicitly) and
  effective `MaxNewSize` (`--expected-max-new-mib`, default 1024, changes it fail-closed);
* post-plateau late/early median ratio no greater than 1.05;
* projected OLS growth over the final half no greater than 3% of heap capacity;
* bounded last-decile growth; and
* larger-run steady occupancy no more than `max(256 MiB, 10% of heap)` above the smaller run.

It prints maximum young-GC, total-safepoint, whole `startAsyncFlush`, complete foreground
async-flush, and whole final-drain elapsed time for each arm. The immutable 250 ms limit applies to
every one of those foreground or stop-the-world latencies. A run with no GC or safepoint in the
measured region remains valid hard-safety evidence but cannot by itself pass retained-occupancy
acceptance; if either event occurs, its maximum must remain within the same limit. The gate exits 0
for `PASS`, 1 for definite `FAIL`, and 3 for `INCONCLUSIVE`. Run sampled profiling separately so it
does not perturb this hard GC verdict.

For the spill scan itself, run a separate lowest-interval allocation profile. This is an acceptance
run, not a throughput number: `interval=1` disables additional async-profiler downsampling, but the
HotSpot allocation hooks still report TLAB refills and allocations outside TLABs rather than every
`new` executed inside an existing TLAB. A non-zero matching stack therefore proves a real allocation;
zero matching samples are regression evidence, not a standalone proof of zero allocation. Pair them
with the focused ownership/wire tests and source-level bounded-scratch audit below. Under the canonical
`globalDict=never` profile, the 100k stream exercises immediate row-group streaming and enough later
foreground epochs to spill projection-trie pages:

```bash
test -n "$ASYNC_PROFILER"
profile_root=$(mktemp -d /tmp/sirix-trie-spill-profile.XXXXXX)
profile_agent="-agentpath:$ASYNC_PROFILER/lib/libasyncProfiler.so=start,event=alloc,interval=1,file=$profile_root/trie-spill-alloc.html"

./gradlew --no-daemon :sirix-query:clickBenchLoad \
  -Pclickbench.args="$profile_root/db generate:100000:42" \
  -Pclickbench.jvmArgs="$common_jvm $profile_agent" \
  > "$profile_root/load.log" 2>&1
```

Acceptance requires the log to show `asyncFlushNodeCap=16384` and the exact 1024/64
`# HFT_CONFIG` limits,
`pinnedTrieSpillPages > 0`, and `pinnedTrieSpillBatchMax <= 64`. The full operation is the unit of
profile evidence: after first-use scratch growth, there must be no recurring allocation sample whose
stack contains `NodeStorageEngineWriter.spillEligiblePinnedTriePages`. This sampling result does not
replace the focused tests that prove fixed-capacity ownership and reuse. Inspect all of the following
rather than stopping when the candidate scan itself is clean:

* `TransactionIntentLog.capturePinnedSpillCandidates`;
* `NodeStorageEngineWriter.isPinnedTrieSpillPageEligible`;
* `IndirectPage.allChildReferencesDurableAndUnclaimed`; or
* `HOTLeafPage.allSideReferencesDurableAndUnclaimed`.

The same rejection applies to descendants that pack HOT slots, sort side-reference keys, clone
HOT-indirect fields, copy an empty compression pipeline, construct `bytesForRead`/segment slices,
or convert checksums to heap arrays. A scan-only result is not HFT acceptance. Compare a warmed
100k profile with a larger profile that publishes more trie pages: allocation below the spill root
must stay flat rather than scale with the number of published pages.

The one-time `PinnedSpillBatch` construction is expected; per-epoch list/iterator/candidate-array
allocation is not. Keep this alloc profile separate from the fixed-heap GC logs, whose uninstrumented
young/old-generation verdict remains authoritative.

Run the parser tests with no third-party test runner:

```bash
python3 -m unittest discover \
  -s bundles/sirix-query/bench/clickbench \
  -p 'test_hft*.py'
```

### AUTO-global ordinary-maintenance arm

`hft_maintenance_gate.py` is the separate fixed-heap gate for an already loaded AUTO-global
projection. It changes the same 100,001 records in two different base sizes, requires at least
three durable dictionary radix generations, clears all global caches, verifies stable anchors and
ids plus value-sensitive predicate results through every cold historical fast route, and compares
touched-unit operations and bytes across the two arms. Both the producer and gate derive `HEAD`,
reject tracked worktree changes, and require it to match the SHA embedded in the log and manifest.
The maintenance writer uses `KEEP_OPEN_ASYNC_COMMIT`, so its logical 16,384-node threshold creates
real revisions and its configuration must report `asyncFlushNodeCap=0`; the 16,384 storage-only
epoch cap applies only to ingestion's `KEEP_OPEN_ASYNC_FLUSH` mode. Maintenance may publish
side-page-only epochs, but its combined-TIL, KVL-frame, foreground-full-flush, and pinned-trie
full-epoch counters must all remain zero. Canonical `globalDict=never` ingestion and AUTO-global
maintenance deliberately use separate databases: reusing one would either weaken the cross-scale
ingestion representation or fail to exercise AUTO dictionary maintenance.

```bash
test -z "$(git status --porcelain)"
hft_sha=$(git rev-parse HEAD)
maintenance_root=$(mktemp -d /tmp/sirix-maintenance-gate.XXXXXX)
maintenance_classpath=$(./gradlew -q :sirix-query:printClickBenchRuntimeClasspath)
saturation_classpath=$(./gradlew -q :sirix-query:printClickBenchTestRuntimeClasspath)

ingestion_jvm="-XX:+UseG1GC -Xms4g -Xmx4g -XX:G1HeapRegionSize=4m -XX:MaxNewSize=1g -XX:+AlwaysPreTouch -XX:+DisableExplicitGC -XX:-G1UseAdaptiveIHOP -XX:InitiatingHeapOccupancyPercent=45 -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m -XX:+ExitOnOutOfMemoryError -XX:MaxDirectMemorySize=1g -XX:-UseJVMCICompiler -DstorageType=FILE_CHANNEL -Dsirix.allocator=frame -Dsirix.offheap.bytes=8589934592 -Dsirix.arena.strategy=shared -Dsirix.asyncFlush.parallelism=2 -Dsirix.asyncFlush.appendParallelism=2 -Dsirix.asyncFlush.sidePageBytes=67108864 -Dsirix.asyncFlush.sidePageCount=131072 -Dsirix.asyncFlush.stallTimeoutMillis=30000 -Dsirix.hft.telemetry=true -Dsirix.hft.gitSha=$hft_sha -Dsirix.autoCommit.nodes=4194304 -Dsirix.projection.globalDict=never -Dclickbench.parallelImport=true -Xlog:gc*,gc+heap=debug,gc+humongous=debug,safepoint:stdout:uptime,level,tags"
maintenance_jvm="-XX:+UseG1GC -Xms4g -Xmx4g -XX:G1HeapRegionSize=4m -XX:MaxNewSize=1g -XX:+AlwaysPreTouch -XX:+DisableExplicitGC -XX:+ExitOnOutOfMemoryError -XX:MaxDirectMemorySize=1g -DstorageType=FILE_CHANNEL -Dsirix.arena.strategy=shared -Dsirix.hft.telemetry=true -Dsirix.hft.gitSha=$hft_sha -Dsirix.projection.globalDict=auto -Dsirix.asyncFlush.appendParallelism=1 -Dsirix.asyncFlush.appendQueueCapacity=1 -Dsirix.asyncFlush.stallTimeoutMillis=30000 -Xlog:gc*,gc+heap=debug,gc+humongous=debug,safepoint:stdout:uptime,level,tags"

for versioning in FULL DIFFERENTIAL INCREMENTAL SLIDING_SNAPSHOT; do
  ./gradlew --no-daemon :sirix-query:clickBenchLoad \
    -Pclickbench.args="$maintenance_root/ingestion-db-small-$versioning generate:1000000:42" \
    -Pclickbench.jvmArgs="$ingestion_jvm -DversioningType=$versioning" \
    > "$maintenance_root/ingestion-small-$versioning-$hft_sha.log" 2>&1
  ./gradlew --no-daemon :sirix-query:clickBenchLoad \
    -Pclickbench.args="$maintenance_root/ingestion-db-large-$versioning generate:4000000:42" \
    -Pclickbench.jvmArgs="$ingestion_jvm -DversioningType=$versioning" \
    > "$maintenance_root/ingestion-large-$versioning-$hft_sha.log" 2>&1
  python3 bundles/sirix-query/bench/clickbench/hft_gc_gate.py \
    --small-log "$maintenance_root/ingestion-small-$versioning-$hft_sha.log" --small-rows 1000000 \
    --large-log "$maintenance_root/ingestion-large-$versioning-$hft_sha.log" --large-rows 4000000 \
    --versioning-type "$versioning" --expected-git-sha "$hft_sha" \
    --runtime-classpath "$maintenance_classpath" \
    --manifest "$maintenance_root/ingestion-$versioning-$hft_sha.manifest.json"

  # Maintenance must start from a separately built AUTO-global projection. Reusing either
  # canonical ingestion database would change the projection representation under test.
  ./gradlew --no-daemon :sirix-query:clickBenchLoad \
    -Pclickbench.args="$maintenance_root/small-$versioning generate:1000000:42" \
    -Pclickbench.jvmArgs="$maintenance_jvm -DversioningType=$versioning" \
    > "$maintenance_root/maintenance-load-small-$versioning-$hft_sha.log" 2>&1
  ./gradlew --no-daemon :sirix-query:clickBenchLoad \
    -Pclickbench.args="$maintenance_root/large-$versioning generate:4000000:42" \
    -Pclickbench.jvmArgs="$maintenance_jvm -DversioningType=$versioning" \
    > "$maintenance_root/maintenance-load-large-$versioning-$hft_sha.log" 2>&1
  ./gradlew --no-daemon :sirix-query:clickBenchMaintenance \
    -Pclickbench.args="$maintenance_root/small-$versioning 1000000 100001 16384" \
    -Pclickbench.jvmArgs="$maintenance_jvm -DversioningType=$versioning" \
    > "$maintenance_root/small-$versioning-$hft_sha.log" 2>&1
  ./gradlew --no-daemon :sirix-query:clickBenchMaintenance \
    -Pclickbench.args="$maintenance_root/large-$versioning 4000000 100001 16384" \
    -Pclickbench.jvmArgs="$maintenance_jvm -DversioningType=$versioning" \
    > "$maintenance_root/large-$versioning-$hft_sha.log" 2>&1

  python3 bundles/sirix-query/bench/clickbench/hft_maintenance_gate.py \
    --small-log "$maintenance_root/small-$versioning-$hft_sha.log" --small-rows 1000000 \
    --large-log "$maintenance_root/large-$versioning-$hft_sha.log" --large-rows 4000000 \
    --dirty-records 100001 --versioning-type "$versioning" --expected-git-sha "$hft_sha" \
    --runtime-classpath "$maintenance_classpath" \
    --manifest "$maintenance_root/maintenance-$versioning-$hft_sha.manifest.json"

  ./gradlew --no-daemon :sirix-query:clickBenchAppendSaturation \
    -Pclickbench.args="$maintenance_root/saturation-$versioning 4096" \
    -Pclickbench.jvmArgs="$maintenance_jvm -DversioningType=$versioning" \
    > "$maintenance_root/saturation-$versioning-$hft_sha.log" 2>&1
  python3 bundles/sirix-query/bench/clickbench/hft_saturation_gate.py \
    --log "$maintenance_root/saturation-$versioning-$hft_sha.log" \
    --versioning-type "$versioning" --expected-git-sha "$hft_sha" \
    --runtime-classpath "$saturation_classpath" \
    --manifest "$maintenance_root/saturation-$versioning-$hft_sha.manifest.json"
done

python3 bundles/sirix-query/bench/clickbench/hft_campaign_gate.py \
  --maintenance-manifest "$maintenance_root/maintenance-FULL-$hft_sha.manifest.json" \
  --maintenance-small-log "$maintenance_root/small-FULL-$hft_sha.log" \
  --maintenance-large-log "$maintenance_root/large-FULL-$hft_sha.log" \
  --maintenance-manifest "$maintenance_root/maintenance-DIFFERENTIAL-$hft_sha.manifest.json" \
  --maintenance-small-log "$maintenance_root/small-DIFFERENTIAL-$hft_sha.log" \
  --maintenance-large-log "$maintenance_root/large-DIFFERENTIAL-$hft_sha.log" \
  --maintenance-manifest "$maintenance_root/maintenance-INCREMENTAL-$hft_sha.manifest.json" \
  --maintenance-small-log "$maintenance_root/small-INCREMENTAL-$hft_sha.log" \
  --maintenance-large-log "$maintenance_root/large-INCREMENTAL-$hft_sha.log" \
  --maintenance-manifest "$maintenance_root/maintenance-SLIDING_SNAPSHOT-$hft_sha.manifest.json" \
  --maintenance-small-log "$maintenance_root/small-SLIDING_SNAPSHOT-$hft_sha.log" \
  --maintenance-large-log "$maintenance_root/large-SLIDING_SNAPSHOT-$hft_sha.log" \
  --maintenance-gate-script bundles/sirix-query/bench/clickbench/hft_maintenance_gate.py \
  $(for versioning in FULL DIFFERENTIAL INCREMENTAL SLIDING_SNAPSHOT; do printf '%s ' \
    --ingestion-manifest "$maintenance_root/ingestion-$versioning-$hft_sha.manifest.json" \
    --ingestion-small-log "$maintenance_root/ingestion-small-$versioning-$hft_sha.log" \
    --ingestion-large-log "$maintenance_root/ingestion-large-$versioning-$hft_sha.log" \
    --saturation-manifest "$maintenance_root/saturation-$versioning-$hft_sha.manifest.json" \
    --saturation-log "$maintenance_root/saturation-$versioning-$hft_sha.log"; done) \
  --ingestion-gate-script bundles/sirix-query/bench/clickbench/hft_gc_gate.py \
  --saturation-gate-script bundles/sirix-query/bench/clickbench/hft_saturation_gate.py \
  --expected-git-sha "$hft_sha" --maintenance-runtime-classpath "$maintenance_classpath" \
  --ingestion-runtime-classpath "$maintenance_classpath" \
  --saturation-runtime-classpath "$saturation_classpath" \
  --manifest "$maintenance_root/canonical-$hft_sha.manifest.json"
```

The final campaign manifest passes only when every `VersioningType` has canonical ingestion,
maintenance, and worker-only p=1/q=1 saturation evidence bound to the required clean commit,
complete ordered runtime classpaths, child logs, the committed gate scripts, and the committed
classpath-identity helper. Child manifests bind the canonical 4 GiB heap; ingestion additionally
binds its 1 GiB nursery and 64 MiB side-page batch. Saturation evidence emits its effective heap,
nursery, and G1 region configuration inside the measured region and requires exactly 4 GiB,
1 GiB, and 4 MiB respectively, alongside observed full worker/queue/admission occupancy, a complete
drain, GC and safepoint bounds, and zero positive humongous-region samples.

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
--memory-limit SIZE        e.g. 12GB; default is DuckDB's setting
--temp-directory <dir>     explicit DuckDB spill directory
--column-spec <file>       JSON from ClickBenchSchema.duckdbColumnSpecJson();
                           cross-checked against the embedded type table
--only 0,7,42              run a subset (debugging)
--full-reference           emit untimed qNN.full.jsonl oracles for LIMIT queries
--candidate-reference ID=DIR
                           emit bounded exact qNN.oracle-ID.json files tied to
                           the candidate results; repeat for multiple paths
```

* The `hits` table is created from the **verbatim `duckdb/create.sql` DDL**, so its column types are
  identical whichever source is used (checked: `DESCRIBE hits` matches `create.sql` on all 105
  columns including nullability).
* On the JSON path the columns are read with an **explicit `columns=` map** so DuckDB's sampler
  cannot guess `BIGINT` as `DOUBLE`; the four temporal columns arrive as ISO strings, are read as
  `VARCHAR` and `CAST` on insert.
* Every try calls `fetchall()` **inside** the timed region — DuckDB's streaming result would
  otherwise let the timer stop before the work is done.
* Outputs: `.clickbench-result-format`, `qNN.jsonl` per query (`NN` = 0-based ClickBench index,
  matching `ClickBenchQueries.byIndex(NN)`), `clickbench-results.txt` (the `Load time:` /
  `Data size:` / 43 × `[t1, t2, t3],` block), and `summary.json`. The version marker is mandatory:
  a non-empty unmarked directory may contain the former rounded encoding and is rejected rather
  than silently upgraded. Start fresh or reuse a directory carrying the current marker. On reuse,
  every selected final/partial result and oracle is invalidated before query work; a completed JSONL
  file is published by same-directory atomic rename, so a failed run is missing rather than stale or
  truncated.
  With `--full-reference`, each LIMIT query also gets an atomically published, streamed
  `qNN.full.jsonl` containing the complete untimed relation. Q24/Q26 carry EventTime (and Q26's
  visible SearchPhrase secondary key) as explicit sidecar keys without changing the measured row.
  `--candidate-reference` is the scalable correctness mode: it writes only the exact bounded window
  keys, the candidate and DuckDB rows to which the proof is bound, and grouped full-relation matches
  with exact multiplicities. It never writes the complete relation.
* A failing query is reported, written as `[null, null, null],` and makes the exit status non-zero;
  the other 42 still run.

Result canonicalisation is lossless for every current ClickBench numeric type. Integers keep every
digit. DuckDB writes finite `DOUBLE` values using Python's shortest round-trip binary64 rendering;
`ClickBenchRunMain.canonicalCell` round-trips the Brackit token through Java `double` and lets Gson
write the corresponding shortest representation. A future non-integral DuckDB `DECIMAL` fails
closed until a lossless cross-engine encoding is defined. NULL is `null`, and timestamps/dates are
the ISO-8601 strings above.

## `compare-results.py`

```
./compare-results.py <sirix-results-dir> <duckdb-results-dir> [--queries queries.sql]
                     [--max-diff-rows N] [--strong [--bounded-oracle ID]]
```

Rows are compared as a **multiset** — SQL `GROUP BY` has no inherent order, so line order carries no
information — with integers and decoded binary64 values exact, strings exact, and `null` equal only
to `null`. An integral float remains exactly equivalent to the same integer because JSON has one
number type (`1666` versus `1666.0` is only a serialisation difference). There is no epsilon: a
one-ULP aggregate disagreement is a mismatch.

Whenever the ORDER BY key is resolvable, both sides are additionally checked to be **actually
ordered by it** — a multiset comparison alone would not notice an engine that returns the right rows
in the wrong order.

`run-differential.sh` uses `--strong --bounded-oracle ID`. Candidate output is available before
DuckDB runs. For each LIMIT query, one DuckDB-side join covers the union of the vectorized, generic,
and DuckDB window rows. The join groups matching full-relation rows and returns exact multiplicities;
there is no probabilistic hash. Each small sidecar embeds the corresponding candidate and DuckDB
rows, so changing or swapping a result after oracle generation fails closed. Exact result
cardinality, exact ordered-window key sequence, and multiplicity-respecting membership are all
mandatory. Independent lossless typed key tokens bind raw `DOUBLE` sort-key identity as well as the
lossless result payload. Q24/Q26 carry their otherwise-hidden EventTime keys. Q17
may choose any ten rows, but
every one must be a real full-relation group row and no full row may be reused beyond its
multiplicity. Only then may different tied members receive a `TIE-AMBIGUOUS` verdict; it is labelled
`STRONGLY VERIFIED`, never `UNVERIFIABLE`.

The boundedness contract is explicit. Every current LIMIT is at most 25 rows. Relations without
floating output are joined on their complete projected row (all ClickBench group keys are projected);
the six relations containing an `AVG` use reviewed exact group identities: RegionID, CounterID,
derived host, or the two-column engine/IP and watch/IP keys. SQL returns at most one grouped match
per distinct requested identity, with a one-past limit that detects and rejects identity drift. A
new floating LIMIT query without a reviewed identity, duplicate output names, a changed width, or an
identity matching multiple relation rows is an error. DuckDB still has to scan or aggregate the
underlying relation, but its memory limit and spill directory bound that engine work; Python and the
per-candidate sidecars stay `O((candidate directories + 1) × LIMIT × projected width)` (the CLI caps
candidate directories at eight; the gate uses two). On the official corpus, use an on-disk
`DUCKDB_DB`, a `DUCKDB_MEMORY_LIMIT`, and a `DUCKDB_TEMP_DIRECTORY` inside the run work directory:
the DuckDB table is roughly 20.5 GiB before grouped-query working memory.

`--full-reference` remains available for small diagnostic corpora. Omitting `--bounded-oracle` from
`compare-results.py --strong` selects those streamed `qNN.full.jsonl` files instead.

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

Without `--strong`, `UNVERIFIABLE` verdicts are counted in the summary line and listed by query. That
legacy/reporting mode is useful for result directories without full sidecars, but it is not the
correctness gate and can accept a fabricated row that merely shares a boundary aggregate.

The script fails loud rather than degrading quietly: a `queries.sql` that does not resolve to exactly
43 statements, a missing queries file, or a `ClickBenchSchema.java` whose column roster has drifted
from the one embedded here all abort with exit 2 — each of them would otherwise silently weaken the
comparison.

Exit status: `0` all MATCH/verified TIE-AMBIGUOUS, `1` at least one real MISMATCH, `2` no mismatches
but at least one MISSING result file, missing/unusable strong sidecar, or an unusable setup. Strong
mode defensively returns 2 if an `UNVERIFIABLE` verdict ever escapes validation.

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

## Two real cross-engine semantics handled by the port

Both differences are represented explicitly so the SirixDB result matches DuckDB rather than only
matching on an ASCII or formatting-convenient subset.

1. **`STRLEN` counts bytes, `string-length` counts code points.** Q27 (`AVG(STRLEN(URL))`) and Q28
   (`AVG(STRLEN(Referer))`) use `jn:utf8-length`, which counts the stored UTF-8 bytes without
   materialising another byte array:

   ```console
   $ duckdb -c "SELECT strlen('ünïcode') AS bytes, length('ünïcode') AS chars;"
   bytes = 9,  chars = 7
   ```

2. **Q42's group key renders differently by default.** `DATE_TRUNC('minute', EventTime)` is a `TIMESTAMP` in
   DuckDB and canonicalises to `"2013-07-15T03:46:00"`, while the JSONiq port's
   `substring($h.EventTime, 1, 16)` produces `"2013-07-15T03:46"`. Appending `":00"` on the JSONiq
   side fixes it and changes nothing else — the port includes that suffix, and a constant suffix
   preserves the lexicographic order the query's `ORDER BY` relies on:

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
  every shape tried — a changed scalar, a changed non-boundary row, a dropped row, a one-ULP float
  drift, a NULLed value, a Q42 key truncated to 16 characters, a reversed or swapped ordering
  (exit 1), and a deleted file (exit 2). A swap of two real rows *tied at the cut-off* correctly stays
  TIE-AMBIGUOUS. Unmarked old result directories and unknown marker versions exit 2 before any row
  comparison.
* Running the two load paths against each other on the same data is itself a live tie test. The two
  loads hold provably identical rows (same md5 over the whole table) yet 23 of the 43 result files
  differ byte for byte, because DuckDB's parallel scan picks a different winner among rows tied at
  the cut-off. The tool reports **0 MISMATCH** — ~18 MATCH and ~25 TIE-AMBIGUOUS, the split moving
  from run to run exactly as the ties do.
