# SirixDB Performance Benchmarks

Two benchmarks with real, reproducible numbers: REST-API behavior under concurrency
(validating the unordered-`executeBlocking` fix) and a core-level large-history benchmark
(10,000 commits). Raw logs for every number in this document live in `/tmp/wave4-d/logs/`
(`read-ladder.log`, `mixed.log`, `read-recheck.log`, `read-control-reseed.log`, `seed.log`,
`large-history-10k.log`, `large-history-1k.log`, `server.log`).

## Environment

| | |
|---|---|
| CPU | Intel Core i7-12700H (12th gen, 14 cores / 20 threads, hybrid P+E) |
| RAM | 31 GiB |
| Disk | WDC PC SN810 NVMe 1 TB, ext4 |
| OS | Linux 6.8.0-107-generic |
| JVM | Oracle GraalVM 25.0.3+9.1 (JDK 25), ZGC |
| Server | `sirix-rest-api-1.0.0-alpha22-fat.jar`, `-Xms1g -Xmx4g`, CI launch flags, HTTP (no TLS) |
| Auth | Keycloak 25.0.1 from `bundles/sirix-rest-api/src/test/resources/docker-compose.yml` (test realm `sirixdb`, user `admin`) |
| Topology | Client and server co-located on the same host, loopback, HTTP/1.1 keep-alive |

Benchmark sources (zero dependencies beyond the JDK + sirix-core test classpath):

- `bundles/sirix-core/src/test/java/io/sirix/bench/RestConcurrencyBenchMain.java` — load generator (virtual threads, closed loop, exact percentiles over all per-request latencies)
- `bundles/sirix-core/src/test/java/io/sirix/bench/LargeHistoryBenchMain.java` — large-history core benchmark (compile/run instructions in the class javadoc; no gradle needed)

---

## Benchmark 1 — REST API under concurrency

**What it validates.** The REST handlers previously ran every blocking task on the Vert.x
context's **ordered** worker queue: all blocking work of the verticle executed strictly
serially, server-wide, so p95 latency at concurrency C approached `C × p95(1)` (the
"~20× at c=16" finding from the 2026-06-09 audit, measured against the pre-fix server).
The fix passes `ordered = false` (`AbstractGetHandler.kt`), keeping per-resource write
exclusivity via sirix's single-writer lock. The pre-fix server was not re-measured here
(it would require a rebuild); the validation criterion is that the ratio is now far from
the concurrency level and near-flat until CPU saturation.

**Setup.** One database/resource seeded with a 1.71 MB JSON document
(`{"hot":0,"data":[…20,000 objects…]}`) + 5 single-field update revisions (6 revisions
total). Measured request:
`GET /bench-db/big?maxLevel=4&maxChildren=50&withMetaData=nodeKeyAndChildCount`
(~18 KB response). Closed loop from N virtual threads, 5 s warm-up (excluded), 30 s
measured window, every request timed; zero HTTP/transport errors in every run.

### Read-only ladder (6-revision resource)

| Concurrency | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2,155 | 0.41 | 0.87 | 1.19 | 6.5 | 0 |
| 4 | 7,001 | 0.55 | 0.77 | 0.90 | 8.7 | 0 |
| 8 | 8,826 | 0.84 | 1.37 | 1.68 | 9.4 | 0 |
| 16 | 10,451 | 1.36 | 2.68 | 4.17 | 13.3 | 0 |
| 32 | 11,030 | 2.64 | 5.58 | 7.88 | 14.9 | 0 |

### Headline: concurrency ratio

> **p95(c=16) / p95(c=1) = 2.68 ms / 0.87 ms ≈ 3.1×** (old ordered-queue behavior
> approached ~16×). A later same-process re-run on a freshly seeded resource gave
> 2.0 ms / 0.7 ms ≈ **2.9×** (`read-control-reseed.log`), so ~3× is stable.

Throughput scales 1→16 by 4.85× and only +5.5% from 16→32 while p95 doubles — classic
CPU saturation (client and server share the 20 hardware threads), not queue serialization.
Minor oddity: p95(c=4)=0.77 ms is *below* p95(c=1)=0.87 ms; the c=1 run executed first
on the freshly started server, so its tail still contains some JIT/page-cache warmth —
if anything the true ratio is slightly better than reported.

### Mixed workload: 16 readers + 1 writer (single-field commit per request)

| Role | Throughput | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|---|---:|---:|---:|---:|---:|
| 16 readers | 1,791 req/s | 4.7 | 10.8 | 224.1 | 448.7 |
| 1 writer (commits) | 39.1 commits/s | 14.0 | 33.7 | 283.5 | 467.5 |

A single small-commit writer (~39 commits/s) costs the readers ~6× throughput vs the
read-only c=16 run and introduces a heavy tail (p99 224 ms vs 4.2 ms). Part of this is
*not* classic write-contention — see the anomaly below: the commits themselves grow the
revision history, which slows every subsequent read.

### ANOMALY (controlled): read-latest latency degrades with revision count

After the mixed run the resource had ~1.4k revisions (6 seed + ~1,370 writer commits).
Re-running the *pure read-only* bench on the same server process, then deleting and
re-seeding back to 6 revisions and running it again:

| Resource state | c | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|---|---:|---:|---:|---:|---:|---:|
| 6 revisions (ladder) | 1 | 2,155 | 0.41 | 0.87 | 1.19 | 6.5 |
| ~1.4k revisions | 1 | 501 | 1.6 | 2.2 | 2.5 | 307.6 |
| 6 revisions (re-seeded, same process) | 1 | 2,363 | 0.4 | 0.7 | 0.9 | 7.1 |
| 6 revisions (ladder) | 16 | 10,451 | 1.36 | 2.68 | 4.17 | 13.3 |
| ~1.4k revisions | 16 | 1,042 | 7.8 | 13.9 | 245.4 | 1,312.3 |
| 6 revisions (re-seeded, same process) | 16 | 12,426 | 1.1 | 2.0 | 3.3 | 70.4 |

Reading the **latest** revision of the **same-sized document** is ~4× slower at c=1 and
~10–12× lower throughput at c=16 once the resource carries ~1.4k revisions — and fully
recovers after re-seeding on the same JVM, ruling out server aging/GC/heap as the cause.
The degradation is a function of revision count alone.

Code-level correlate (hypothesis, consistent with Benchmark 2's measurements): the REST
layer opens the database + resource session per request, and every storage open eagerly
runs `loadRevisionFileDataIntoMemory` + `loadRevisionIndex`
(`bundles/sirix-core/src/main/java/io/sirix/io/StorageType.java`, `FILE_CHANNEL.getInstance`)
— O(revisions) work per request, measured at ~0.46 µs/revision in-core (see below). At
c=16 this O(R)-per-request work multiplies across all workers and saturates CPU early;
the 245 ms / 1.3 s tail spikes under concurrency are unexplained by the linear term alone
and deserve profiling (suspects: contended Caffeine revision-data cache loads and
allocation bursts from per-open array copies). **Follow-up: cache the loaded revision
index across request-scoped opens (it is already held in a global
`REVISION_INDEX_REPOSITORY`, but the eager per-open reload dominates).**

---

## Benchmark 2 — Large history (10,000 commits, core API)

**Setup.** `LargeHistoryBenchMain`: one resource (FILE_CHANNEL storage, SLIDING_SNAPSHOT
versioning, rolling hashes, path summary on), initial tiny document
`{"counter":0,"label":…,"tags":[…]}`, then 9,999 explicit `setNumberValue` + `wtx.commit()`
commits (no auto-commit batching) on one field. **Cold** = first run after
`Databases.clearGlobalCaches()` (in-process caches dropped; OS page cache stays warm).
**Warm** = median of 7 runs. A 3-iteration JIT warm-up precedes each metric so cold
isolates cache state, not compilation.

**Build:** 10,000 commits in **48.6 s** (4.86 ms/commit average), 15.9 MB on disk
(~1.6 KB/commit). Single 1k/10k-commit runs; treat small deltas (<2×) as noise.

| Metric | Cold (ms) | Warm median (ms) |
|---|---:|---:|
| open database + resource session (incl. close) | 6.28 | 4.64 |
| `getHistory()` full list [10,000 revisions] | 50.77 | 3.05 |
| `getHistory(100)` most-recent page | 0.90 | 0.03 |
| `beginNodeReadOnlyTrx(1)` + 3-step read | 0.54 | 0.018 |
| `beginNodeReadOnlyTrx(5000)` + read | 0.46 | 0.018 |
| `beginNodeReadOnlyTrx(10000)` + read (latest) | 0.33 | 0.018 |
| `diff(1, 2)` (BasicJsonDiff) | 1.74 | 0.18 |
| `diff(9999, 10000)` | 1.31 | 0.29 |
| serialize revision 1 (full document) | 1.13 | 0.20 |
| serialize revision 10000 | 1.04 | 0.21 |

**Flat (good):** random-revision access is position-independent — trx open+read is
~18 µs warm whether the revision is the 1st, 5,000th, or 10,000th; diff and full-document
serialization are likewise flat across history position. `getHistory(100)` does *not*
scan the full history (0.9 ms cold vs 50.8 ms for the full list — properly paged).
Cold `getHistory()` of all 10k revisions is 50.8 ms (~5 µs/revision) and the
(alpha20) history cache brings warm calls to 3 ms.

### SCALING FLAG 1: session open is linear in history length

Warm open+close of the same resource at three history sizes (200-commit run from the
smoke log, plus `large-history-1k.log` / `large-history-10k.log`):

| Revisions | Warm open (ms) | Cold open (ms) |
|---:|---:|---:|
| 200 | 0.42 | 0.58 |
| 1,000 | 0.95 | 1.24 |
| 10,000 | 4.64 | 6.28 |

Linear fit ≈ 0.4 ms fixed + **~0.46 µs per revision**. Cause (by code inspection):
every storage open eagerly loads all per-revision file data and rebuilds/loads the
revision index (`StorageType.FILE_CHANNEL.getInstance` →
`loadRevisionFileDataIntoMemory` + `loadRevisionIndex`). Harmless at 10k revisions in
absolute terms (4.6 ms), but it is exactly the per-request cost that produces the REST
anomaly above, and extrapolates to ~0.5 s per open at 1 M revisions.

### SCALING FLAG 2: per-commit cost grows linearly with history

Per-1,000-commit build rate declines monotonically once JIT-warm:

| Commits | 2k | 3k | 4k | 5k | 6k | 7k | 8k | 9k | 10k |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| commits/s (last 1000) | 289 | 275 | 246 | 225 | 199 | 187 | 176 | 163 | 154 |

Per-commit cost roughly doubles from ~3.5 ms to ~6.5 ms over 10k commits
(≈ +0.33 µs per existing revision per commit ⇒ cumulative build is O(R²)).
A code-confirmed O(R)-per-commit path exists: `RevisionIndex.withNewRevision`
(`bundles/sirix-core/src/main/java/io/sirix/io/RevisionIndex.java`) copies the full
timestamp/offset arrays **and rebuilds the Eytzinger search layout on every commit**
(the comment "O(n) but only on commit" acknowledges it). Whether that copy dominates
the measured +0.33 µs/rev/commit, or the eager revision-data reload / another O(R) path
contributes, needs a profile — flagged for follow-up. An incremental (append-only or
batched) index update would remove the quadratic term.

---

## Methodology notes & honest caveats

- **Local loopback, single machine, co-located client+server.** No network latency; the
  load generator competes with the server for CPU, so absolute throughput ceilings
  (≈11–12k req/s) understate a dedicated server and saturation onset (~c=16) is partly
  client-induced. Latency *ratios* between concurrency levels are the meaningful signal.
- **Closed-loop load** (each worker waits for its response): percentiles are exact over
  every measured request (~65k–373k samples/run), but there is no coordinated-omission
  correction; under saturation closed loops self-throttle.
- **"Cold" ≠ cold disk.** `Databases.clearGlobalCaches()` drops sirix's in-process caches
  only; the OS page cache stays warm (dropping it needs root). True cold-disk numbers
  would be higher.
- **Single runs** per configuration (plus the c=1/c=16 re-run and re-seed control for
  Bench 1, and 1k/10k history sizes for Bench 2). Variance was not characterized beyond
  those repeats; treat <2× differences as noise, the flagged 3×–12× effects replicated.
- **Tiny document in Benchmark 2** (4-key object) — deliberately isolates per-revision
  overheads from data-volume effects; it says nothing about large-document scaling.
- **HTTP/1.1 forced** in the client so concurrency = real connections (no h2 multiplexing).
- Auth token fetched once per run (admin/admin against the test realm); token validation
  is part of every measured request, as in production.

## Reproducing

```bash
# Core benchmark (no gradle; uses prebuilt classes + the captured test classpath)
javac --enable-preview --release 25 --add-modules jdk.incubator.vector \
  -cp "$(cat /tmp/sirix-test-cp.txt)" -d /tmp/wave4-d/classes \
  bundles/sirix-core/src/test/java/io/sirix/bench/*.java
java --enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
  -Xms1g -Xmx4g -cp "/tmp/wave4-d/classes:$(cat /tmp/sirix-test-cp.txt)" \
  io.sirix.bench.LargeHistoryBenchMain 10000

# REST benchmark
(cd bundles/sirix-rest-api/src/test/resources && docker compose up -d keycloak)  # wait for realm + users
java -Xms1g -Xmx4g -Duser.home=/tmp/wave4-d/server-home --enable-preview \
  --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED ... (CI flag set, see .github/workflows/gradle.yml) \
  -jar bundles/sirix-rest-api/build/libs/sirix-rest-api-1.0.0-alpha22-fat.jar \
  -conf bundles/sirix-rest-api/src/main/resources/sirix-conf.json &
java --enable-preview -cp /tmp/wave4-d/classes io.sirix.bench.RestConcurrencyBenchMain seed  http://localhost:9443 bench-db big 20000 5
java --enable-preview -cp /tmp/wave4-d/classes io.sirix.bench.RestConcurrencyBenchMain read  http://localhost:9443 bench-db big 16 5 30
java --enable-preview -cp /tmp/wave4-d/classes io.sirix.bench.RestConcurrencyBenchMain mixed http://localhost:9443 bench-db big 16 5 30
(cd bundles/sirix-rest-api/src/test/resources && docker compose down)
```

---

## Post-fix re-measurement (same day): revision-history scaling

The two anomalies above (read throughput collapsing with revision count; session
opens linear in history) were root-caused to `IOStorage.loadRevisionIndex`
re-reading **every** revision record on **every** storage open, while the
in-JVM `RevisionIndexHolder` was already kept current by the writer. Fixes:

- `loadRevisionIndex` now reloads only when the in-memory index size disagrees
  with the on-disk revision count (covers fresh processes AND out-of-band
  truncation in both directions) — session opens are O(1) in history.
- `RevisionIndex.withNewRevision` appends amortized (capacity-doubling shared
  arrays + deferred Eytzinger rebuild once the uncovered tail exceeds
  max(64, size/8); searches bridge with a bounded binary search on the tail) —
  removes the former O(size) copy + rebuild per commit (O(size²) cumulative).

### Large-history core benchmark, 10,000 commits (before → after)

| Metric | Before (warm) | After (warm) |
|---|---:|---:|
| open database+session | 4.64 ms (linear: ~0.46 µs/revision) | **0.18 ms, flat** |
| getHistory() full [10k] | 3.05 ms | 0.84 ms |
| everything else | — | unchanged (already flat) |

The per-commit rate decline was subsequently root-caused and FIXED (same day).
The hunt eliminated, by direct experiment: the revision index (above),
`storeNodeHistory` record growth, GC, buffer-pool occupancy, per-transaction
state, syscall-count growth in opens/stats/preads/fsyncs, and file-extent
fragmentation. Wall-clock profiling (async-profiler, `wall` event) then showed
the late-phase main thread dominated by `access(2)` — and a syscall census
confirmed a perfect quadratic: **50,196,928 access() calls over a 10k-commit
build (~50M of them ENOENT), vs 520k for 1k commits (Σi ≈ N²/2)**.

Root cause: `AbstractResourceSession.initializeIndexController` probed
`revision.xml, (revision-1).xml, …, 0.xml` with one `Files.exists` per step to
find the most recent index definitions — O(revision) syscalls per index-
controller creation, and a new controller is created per commit. With no
secondary indexes (the default), NO file ever exists and every commit walked
the entire history. Fix: one directory listing picking the max-numbered file
≤ revision (an empty directory short-circuits instantly).

Second contributor fixed: the commit protocol issued 7 sync calls per commit
(strace: 5 fsync + 2 fdatasync). The t3 `forceAll` was fully redundant with
`writeUberPageReference`'s internal write-ahead barrier (which flushes the
buffered tail FIRST and then forces both files — the t3 barrier ran while the
tail was still buffered and covered strictly less), and the commit-acknowledge
barrier only needs a data-only `fdatasync` (the primary beacon is an in-place
overwrite; the revisions file saw no writes after its own barrier). New
protocol: **4 sync calls** — fsync(data) write-ahead, fsync(revisions),
fdatasync(data) beacon-order, fsync(data) acknowledge. The two data barriers
that cover the tail append stay full fsyncs deliberately: the power-loss
simulation's metadata-split model (stricter than POSIX fdatasync) loses acked
revisions if size durability leans on fdatasync semantics. Re-validated GREEN
by the power-loss gate (force-contract AND metadata-split) and the SIGKILL
gate.

### Result (same 10k-commit build)

| | Before | After |
|---|---:|---:|
| total build | 48.4 s (4.84 ms/commit) | **20.5 s (2.05 ms/commit)** |
| commit rate at depth 10k | ~150 commits/s, declining | **~570 commits/s, FLAT** |
| access() syscalls | 50.2M (quadratic) | O(commits) |
| sync calls per commit | 7 | 4 |

The decline is eliminated, not reduced — the curve is flat, so 100k+ revision
builds no longer degrade.

On "is one fsync per commit enough": with per-commit acknowledged durability
and the dual-file layout, the logical floor is two ordered barriers
(write-ahead: data+revisions durable before beacons; acknowledge: primary
beacon durable before return), which costs 4 calls across two files as
implemented. Reaching ONE explicit sync call per commit is possible by opening
the revisions channel and a dedicated beacon channel with
`StandardOpenOption.DSYNC` — tiny synchronous writes (FUA on NVMe, cheaper
than full cache flushes) make the record and beacons durable at write-return,
leaving a single explicit `fdatasync` for the data tail. Documented as a
follow-up design; the ordering guarantees stay identical.

### REST read throughput at high revision count (the collapse scenario)

Re-run with the rebuilt fat jar, `auth.mode=none` (no per-request JWT
validation — within-run comparisons are the meaningful ones), same generated
document, history grown to **1,901 revisions** via the mixed workload:

| Cell | Before fix (~1.4k revs) | After fix (1.9k revs) |
|---|---:|---:|
| read c=1 | 501 req/s, p50 1.6 ms | **2,897 req/s, p50 0.29 ms** |
| read c=16 | 1,042 req/s, p99 245 ms | **18,361 req/s, p99 1.84 ms** |
| fresh-resource baseline c=1 (same run) | 2,155–2,273 req/s | 2,273 req/s |

Read performance at 1.9k revisions now **exceeds** the fresh-resource baseline
— the history-depth degradation is eliminated, not merely reduced. Zero errors
in both measured cells.

### Follow-up implemented: write-through (O_SYNC/O_DSYNC) commit protocol

The "one explicit sync" design was implemented: the revisions record goes
through an `O_SYNC` channel (durable incl. size at write-return), both beacon
slots through an `O_DSYNC` channel (in-place overwrites; write-return gives
secondary-before-primary ordering and makes the primary's return the commit
acknowledge). Per commit: ONE explicit `fsync` (data tail write-ahead) plus
three small write-through writes; the async acknowledge machinery is gone and
`Writer.writeUberPageReference` now carries a durable-on-return contract.

Measured on this workstation (ext4, consumer NVMe with FUA): **parity** with
the 4-sync protocol (2.09 vs 2.05 ms/commit) — three serialized write-through
round-trips cost about what the saved flushes did here. The win is structural
(simpler, contract-explicit) with expected gains on server stacks where FUA
writes are materially cheaper than cache flushes. All power-loss and SIGKILL
gates re-validated green (the simulation now models per-write durability for
write-through channels).
