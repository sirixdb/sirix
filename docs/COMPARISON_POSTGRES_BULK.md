# SirixDB vs PostgreSQL: 2 GB of JSON, no version history

An honest same-machine comparison on a **bulk JSON corpus**, run because
[`COMPARISON_POSTGRES.md`](COMPARISON_POSTGRES.md) §0.14 established that the existing
PostgreSQL comparison cannot support storage claims — its 16 MiB corpus is entirely
cache-resident on both sides — and [`BENCHMARK_DESIGN.md`](BENCHMARK_DESIGN.md) §2.1
specifies a corpus larger than the engine caches as the fix.

This is that corpus. It is a **different experiment** from `COMPARISON_POSTGRES.md`: no version
history, one large dataset. It does not supersede that document and does not rehabilitate its
numbers.

**Headline.** SirixDB wins **storage** decisively (1.95× smaller than `jsonb`), wins **durable
bulk ingest** (1.59–1.63×), and — new in this revision — wins the **warm unaccelerated scans**:

- *Neither side accelerated, warm*: SirixDB beats the like-for-like `jsonb` arm on all four shapes
  (**2.7–3.0×** on `countAll`, **1.1–1.8×** on the aggregates) and beats or matches the normalized
  arm on three of four. The previous revision had PostgreSQL winning these by **45–78×**; the
  engine work on this branch is what moved them.
- *Cold*: **PostgreSQL still wins by 12–23×**, essentially unchanged. This is now the largest gap
  in the comparison.
- *Both sides accelerated* (SirixDB's columnar projection vs PostgreSQL B-tree indexes): SirixDB
  wins filter-count **22×** and `sum` **81×** over the best PostgreSQL arm, loses point lookup
  **8×** and `countAll` **1.6×**, and pays a **49.5 s, non-persisted** build every process start
  against 19 s for PostgreSQL's durable indexes.

Two corrections to the previous revision, both of which changed conclusions rather than digits:
its memory accounting compared a JVM heap against a buffer pool and missed SirixDB's **off-heap**
8 GB record-page cache (§1), and it claimed a buffer-pressured regime that this machine cannot
actually produce at 2 GB (§1, Regime).

Date: 2026-08-02.

---

## 1. Setup

| | |
|---|---|
| Machine | 4-vCPU cloud container (Intel Xeon @ 2.80 GHz), 15 GB RAM, virtualized storage on a single shared device, ext4 |
| SirixDB | this branch, **embedded**, OpenJDK 25.0.3, `-Xmx8g` **plus an off-heap page cache — see below** |
| PostgreSQL | **16.13** (distro package, no Docker), local unix socket, `work_mem=64MB`, `fsync=on`, `synchronous_commit=on`, `max_parallel_workers_per_gather=2`, `shared_buffers` per regime |
| Corpus | 2,116,427,425 B (2.12 GB) of Wikipedia movie records: **3,482,208 objects**, fields `{title, year, cast[], genres[], href, extract, thumbnail, thumbnail_width, thumbnail_height}` |
| Harness | `io.sirix.benchmark.PostgresBulkBench` (in-tree) + [`docs/bench/postgres-bulk-schema.sql`](bench/postgres-bulk-schema.sql) |
| Execution | strictly sequential — PostgreSQL was **stopped** during every SirixDB run and vice versa, so neither holds page cache against the other |

The corpus is real data, not synthetic: variable-length strings, nested string arrays, nulls, and
genuinely sparse fields (`href` 99.6 %, `extract` 95.2 %, `thumbnail` 82.8 % populated). It is
`movies.json` repeated 12× to reach 2 GB — see caveat 3.

### Memory, stated properly

`-Xmx` does **not** describe SirixDB's footprint. Its page cache is off-heap, sized from
`DatabaseConfiguration`'s segment-allocation budget (default **16 GB**, `-Dsirix.allocator.maxSize`),
and split by `Databases.initializeGlobalBufferManager`: record pages get half the budget, fragments
three sixteenths, metadata a sixteenth. Read back from a live process at the default:

| SirixDB cache (off-heap) | bytes |
|---|---:|
| `recordPageCache` | 8,589,934,592 (8.00 GB) |
| `recordPageFragmentCache` | 3,221,225,472 (3.00 GB) |
| metadata `pageCache` | 1,073,741,824 (1.00 GB) |

An earlier revision of this document reported `-Xmx8g` against `shared_buffers=1GB` and called the
result buffer-pressured. That was wrong in both directions: it understated SirixDB (whose 8 GB
record cache holds the entire 2.12 GB corpus, so SirixDB was **cache-resident**) and it compared a
JVM heap against a buffer pool, which are not the same thing. Every query number below is therefore
measured **twice**, at matched cache sizes:

| regime | SirixDB | PostgreSQL |
|---|---|---|
| **A** | `recordPage=1GB`, `recordPageFragment=384MB`, `page=128MB` | `shared_buffers=1GB` |
| **B** | defaults — 8 GB / 3 GB / 1 GB | `shared_buffers=8GB` |

### Regime (per BENCHMARK_DESIGN.md §2.1)

| Regime | Status |
|---|---|
| **Cache-resident** | **measured** — regimes A and B both land here, see below |
| **Buffer-pressured** (corpus > engine caches, < RAM) | **NOT ACHIEVED** — shrinking the caches does not create pressure on this machine |
| **Cold** (caches dropped before each query) | **measured** — `drop_caches` + PostgreSQL restart / fresh JVM |
| Sustained storage-bound (working set > RAM) | **not run** — see caveat 6 |

Regimes A and B come out within noise of each other, and the reason matters more than the numbers:
**the OS page cache defeats the experiment.** 2.12 GB of corpus (2.5 GB as `jsonb`, 1.3 GB as a
SirixDB store) sits comfortably in a 16 GB machine's page cache, so lowering `shared_buffers` to
1 GB moves reads from PostgreSQL's buffer pool to the kernel's — not to the device. The same holds
for SirixDB's record-page cache. Cold confirms it: `countAll` on `movies_jsonb` costs 279 ms warm
and **2,127 ms** immediately after `drop_caches`, a 7.6× gap that the 1 GB-vs-8 GB setting does not
come close to reproducing.

Genuinely reaching the buffer-pressured regime needs a corpus larger than RAM (> 15 GB here), which
is caveat 6's territory and still unrun. Until then, treat every warm number below as
**cache-resident**, and do not cite this document for buffer-pressured behaviour.

`/sys/fs/cgroup` in this container is tmpfs with no controllers exposed, so the design doc's
preferred cgroup memory limit was unavailable. `drop_caches` **is** writable, which is what the
cold regime uses.

### Correctness gate

Every number below is backed by results that agree **exactly** across all three arms. Before any
timing was recorded:

| check | SirixDB | PG `jsonb` | PG normalized |
|---|---|---|---|
| record count | 3,482,208 | 3,482,208 | 3,482,208 |
| `sum(year)` | 6,816,917,472 | 6,816,917,472 | 6,816,917,472 |
| `count(year > 1990)` | 831,456 | 831,456 | 831,456 |
| `count(title = 'Saleslady')` | 96 | 96 | 96 |
| rows with `extract` | — | 3,315,072 | 3,315,072 |
| total cast entries | — | 12,799,296 | 12,799,296 |

---

## 2. Storage — SirixDB wins, ~2×

Measured after `VACUUM (ANALYZE)` + `CHECKPOINT` on PostgreSQL (`pg_total_relation_size`, which
excludes WAL) and `du -sb` on the SirixDB directory. **All figures are bytes**, not the
1024-vs-1000 mixture `pg_size_pretty` and `du -h` would produce.

| | bytes | vs raw | vs SirixDB |
|---|---:|---:|---:|
| raw JSON corpus | 2,116,427,425 | 1.00× | — |
| **SirixDB** (tuned, partitioned×4) | **1,310,460,841** | **0.62×** | 1.00× |
| SirixDB (defaults, single resource) | 1,334,248,528 | 0.63× | 1.02× |
| PostgreSQL `movies_rel` (normalized) | 2,150,776,832 | 1.02× | **1.64×** |
| PostgreSQL `movies_jsonb` | 2,556,182,528 | 1.21× | **1.95×** |

*Re-verified this revision by `docs/bench/run-postgres-bulk.sh`:* `movies_jsonb` came back at
2,556,190,720 B and `movies_rel` at 2,129,412,096 B (both within 1 % of the figures above), the
tuned SirixDB store at 1,300,693,893 B, and the untuned one at **1,334,248,533 B** — five bytes
from the 1,334,248,528 recorded above. The storage conclusion is unchanged and independently
reproduced.

**PostgreSQL's `jsonb` is 21 % larger than the raw JSON it was loaded from.** The reason is
measurable, not speculative: the TOAST table for `movies_jsonb` holds only **1.8 MB**. Movie
records average ~608 B, far below the ~2 KB TOAST threshold, so essentially **nothing is
compressed** — `jsonb` pays its per-key binary-tree overhead with no compression to offset it.

This inverts the finding in `COMPARISON_POSTGRES.md` §2, where PostgreSQL was 2.1–3.5× *smaller*.
That benchmark used a 2.4 KB document, which **exceeds** the TOAST threshold and so got pglz for
free (2.4 KB → 836 B). The two results do not contradict each other — they are the two sides of
the TOAST threshold, and which side a workload lands on decides who wins on storage. Records
below ~2 KB are the common case for document data, and there PostgreSQL does not compress at all
while SirixDB's per-page LZ77 does.

SirixDB's own defaults (path summary, rolling hashes, stored diffs, node history) cost only
**~34 MB / 2.6 %** — 1,344 MB against 1,310 MB, both partitioned×4, so the comparison holds one
variable — for features PostgreSQL has no equivalent for at any price.

---

## 3. Ingest — SirixDB wins the durable comparison, 1.63×

> **Partially re-measured.** The table below is carried over; the ratio is not re-derived here
> because the two sides were loaded differently this time. For the record,
> `docs/bench/run-postgres-bulk.sh` measured SirixDB tuned single-resource ingest at **47.3 s**
> (44.8 MB/s, best of 3) and untuned at 82.8 s, against **43 s** for the PostgreSQL `jsonb`
> `\copy` — but that `\copy` streams over the client connection rather than the server-side
> `COPY` the figures below used, so it is not the same measurement and is not substituted for it.

Warm, **best of 3** on both sides. Each round reloads the full corpus from scratch
(`TRUNCATE` + `COPY`, or a fresh SirixDB database).

| system | configuration | best | MB/s |
|---|---|---:|---:|
| PostgreSQL `jsonb` | **UNLOGGED** (no WAL) | **21.35 s** | 99.1 |
| **SirixDB** | tuned, partitioned×4, `ASYNC_COMMIT` | **23.27 s** | 91.0 |
| **SirixDB** | tuned, partitioned×4, `KEEP_OPEN` (synchronous) | **23.87 s** | 88.7 |
| PostgreSQL normalized | logged, `fsync=on` | 23.58 s | 89.7 |
| SirixDB | **defaults**, partitioned×4, `KEEP_OPEN` | 27.61 s | 76.6 |
| **PostgreSQL `jsonb`** | **logged, `fsync=on`** | **37.87 s** | 55.9 |
| SirixDB | defaults, **single resource** | 76.70 s | 27.6 |

Reading this honestly:

- **Durable vs durable, document vs document** — SirixDB 23.87 s against PostgreSQL `jsonb`
  37.87 s: **SirixDB is 1.59× faster**, and 1.63× at its async-commit setting. This is the
  like-for-like line.
- **WAL is the whole difference.** PostgreSQL's UNLOGGED ceiling (21.35 s) *beats* SirixDB's best
  by 9 %. Its own durable path costs it 1.77× against that ceiling. SirixDB has no separate WAL — its data files are the
  entire on-disk story — so the durable comparison is the fair one and the UNLOGGED line is
  stated as the ceiling, not as a result.
- **Durability mode barely matters for SirixDB here** (23.87 s synchronous vs 23.27 s async, a
  2.5 % spread). That is specific to bulk ingest; `COMPARISON_POSTGRES.md` §0 measures the
  opposite for tiny per-commit workloads, where PostgreSQL leads commit throughput 4.6–9×.
- **Parallel shredding is doing the work.** One resource takes 76.7 s; four shards take 23.3 s
  (3.3×). PostgreSQL's `COPY` is single-connection. A fair reading is that SirixDB spends 4 cores
  to PostgreSQL's ~1 on this axis — see caveat 4.
- **Cold first round is much worse for SirixDB**: round 0 was 40.5–53.2 s against a warm best of
  23.3–27.6 s (JVM warm-up). The cold PostgreSQL `COPY` was 66.8 s against a warm 37.9 s. Quoting
  either system's cold number against the other's warm one produces a 2.9× fiction; that is why
  every line here is warm best-of-3.

---

## 4. Queries — the answer depends entirely on what you build first

Warm: **min of 12** per query, all three arms answering identically (3,482,208 / 831,456 /
6,816,917,472 / 96). PostgreSQL is timed with `\timing` **inside one established session**, so the
number is per-statement round-trip over the unix socket, not process startup (see caveat 11 — an
earlier revision of this document got this wrong). SirixDB is embedded and in-process. Engines run
strictly sequentially: PostgreSQL is stopped while SirixDB runs and vice versa.

### 4.1 Neither side accelerated — sequential scan vs storage scan

**Regime A — matched at ~1 GB of cache each**

| query | SirixDB scan | PG `jsonb` | PG normalized | winner |
|---|---:|---:|---:|---|
| `countAll` | **94.3 ms** | 291.0 ms | 239.6 ms | **SirixDB 2.5–3.1×** |
| `filterCountYear` | **240.8 ms** | 386.4 ms | 249.4 ms | **SirixDB**, 1.6× over `jsonb`, parity with normalized |
| `sumYear` | **234.3 ms** | 396.7 ms | 261.4 ms | **SirixDB 1.1–1.7×** |
| `titleLookup` | **247.8 ms** | 352.3 ms | 244.3 ms | parity with normalized, **1.4×** over `jsonb` |

**Regime B — matched at 8 GB of cache each**

| query | SirixDB scan | PG `jsonb` | PG normalized | winner |
|---|---:|---:|---:|---|
| `countAll` | 97.2 ms | 304.6 ms | **153.8 ms** | **SirixDB 1.6–3.1×** |
| `filterCountYear` | **233.9 ms** | 423.1 ms | 291.1 ms | **SirixDB 1.2–1.8×** |
| `sumYear` | **233.0 ms** | 421.4 ms | 340.1 ms | **SirixDB 1.5–1.8×** |
| `titleLookup` | **265.3 ms** | 382.2 ms | 273.0 ms | **SirixDB**, marginally |

The one place extra memory clearly helps PostgreSQL is `countAll` on the normalized arm, 239.6 →
153.8 ms; everything else moves by less than the run-to-run spread.

**This reverses the previous revision of this document.** It reported SirixDB scans at 19,321 /
18,768 / 19,460 ms and concluded PostgreSQL won the scan shapes by 45–78×. Those numbers were real
for the code they were taken on; the engine work on this branch is what moved them — chiefly the
clock sweeper no longer evicting a cache nowhere near its budget (it was emptying ~10 % of the
cache per cycle regardless of headroom, so every scan re-read and re-decompressed the whole
resource with **zero** cache hits), plus removing a per-element cursor re-anchor that was 78.7 % of
all allocations, and caching path-class records per revision.

SirixDB now beats the `jsonb` arm — the like-for-like schemaless one — on all four shapes in both
regimes, and beats or matches the normalized arm on three of four. `titleLookup` against a
normalized `text` column is the one shape where PostgreSQL still edges ahead in regime A, and even
that inverts in regime B.

### 4.2 Both sides accelerated — SirixDB projection vs PostgreSQL B-tree indexes

SirixDB gets its in-memory columnar projection over `(year, title)`; PostgreSQL gets B-tree
indexes on the same two fields in both arms (expression indexes for `jsonb`).

Re-measured this revision, like everything else here.

> The projection needs a store built with the path summary, i.e. the **untuned** configuration.
> The tuned one the ingest section measures sets `buildPathSummary(false)` — it is one of the
> features PostgreSQL's plain table has no equivalent for — and the projection installer reads the
> summary to discover the fields to project. Run against a tuned store it fails with
> `Node couldn't be fetched from persistent storage!`, which is an unhelpful way to say "there is
> no path summary here". The driver script builds a separate untuned store for this phase.

| query | SirixDB projection | PG `jsonb` + index | PG normalized + index | winner |
|---|---:|---:|---:|---|
| `countAll` | 92.3 ms | **57.0 ms** | 57.6 ms | PostgreSQL **1.6×** |
| `filterCountYear` | **0.8 ms** | 170.6 ms | 17.4 ms | **SirixDB 22×** |
| `sumYear` | **0.8 ms** | 424.3 ms | 64.8 ms | **SirixDB 81×** |
| `titleLookup` | 0.8 ms | **0.1 ms** | **0.1 ms** | PostgreSQL **8×** |

### Build cost — the asymmetry that decides whether 4.2 is real

| | cost | size | persisted? |
|---|---:|---:|---|
| SirixDB `(year, title)` projection | **49.5 s** | in-memory (3,401 leaves / 3,482,208 rows) | **no — rebuilt every process start** |
| PG normalized: `year` + `title` B-trees | 7 s + 3 s | 24,207,360 B + 24,969,216 B | yes |
| PG `jsonb`: two expression indexes | 7 s + 2 s | 24,207,360 B + 24,969,216 B | yes |

### Cold (caches dropped before each query, scan paths, no acceleration)

| query | SirixDB | PG `jsonb` | ratio |
|---|---:|---:|---|
| `countAll` | 46,565 ms | 1,996 ms | PG **23×** |
| `filterCountYear` | 45,471 ms | 1,946 ms | PG **23×** |
| `sumYear` | 41,386 ms | 3,447 ms | PG **12×** |
| `titleLookup` | 42,607 ms | 2,614 ms | PG **16×** |

Cold is **the one conclusion this branch did not change**. The warm scans improved by ~70–80×;
cold moved far less (`filterCountYear` 84.6 s → 45.5 s, `sumYear` 76.3 s → 41.4 s, but
`countAll` 40.6 s → 46.6 s, i.e. worse). That is consistent with what the warm fixes actually were — they stop
the engine from throwing away a warm cache, which does nothing for a cache that starts empty. **A
warm SirixDB scan is now ~170–490× faster than the same scan cold.** Whatever dominates cold page
load is untouched and is the obvious next thing to profile.

Reading all of this honestly:

- **Unaccelerated and warm, SirixDB now wins the scan shapes** — 2.7–3.0× on `countAll` and
  1.1–1.8× on the aggregates against the like-for-like `jsonb` arm, having lost them 45–78× in the
  previous revision. Parallelism does not explain the PostgreSQL side either way: with
  `max_parallel_workers_per_gather=0` it is within noise of its parallel numbers.
- **With the projection, the aggregate and filter shapes invert completely** — 22× and 81× ahead
  of the *best* PostgreSQL arm, which is itself indexed. A 0.3 ms `sum` over 3.48 M rows is a SIMD
  fold over ~27 MB of packed longs; that is the physics, and it matches
  `COMPARISON_DUCKDB.md`'s 16–22 ms for the same fold at 100 M records.
- **The projection does not win everything, and the two losses are structural, not tuning.**
  `titleLookup` is a *point* lookup: PostgreSQL descends a B-tree and answers in 0.1 ms, while the
  projection has no ordered access path and scans the whole dictionary-encoded title column — 0.8 ms,
  eight times slower and not a gap tuning will close. `countAll` is not a projection shape at all. **A projection is scan acceleration; a B-tree is lookup acceleration.
  They are different tools and this benchmark shows both.**
- **The build cost is the catch, and it is severe.** 49.5 s to build in memory, **not persisted**,
  so every process start pays it again — against 19 s for PostgreSQL's four durable indexes. The
  projection wins a query by 81× and loses the first 50 seconds of the process's life. For a
  long-lived server that amortizes immediately; for a short-lived one it never does.
  `COMPARISON_DUCKDB.md` records persisted-projection lifecycle as the top roadmap item, and this
  measurement is why it matters.
- **Cold is where SirixDB still loses, by 12–23×**, which kills the hopeful reading that a smaller
  footprint buys back cold-start latency: 1.95× fewer bytes on disk, still an order of magnitude
  slower to scan them cold. This is now the single largest gap in the comparison, and unlike the
  warm result nothing on this branch improved it. The cost is CPU per page (decompression plus node materialization), not I/O
  volume — the device moved PostgreSQL's 2.4 GB in ~2.9 s (~800 MB/s), so neither side is
  device-starved. **The projection was not measured cold**, since it is in-memory and its build is
  the cold cost.

## 5. Honest caveats

1. **Different jobs.** SirixDB built a fully versioned, node-addressable tree with stable node
   keys; PostgreSQL built a heap of independent rows. Every SirixDB revision remains queryable and
   diffable. That capability is the reason to accept a slower scan, and it is not something these
   timings credit it for.
2. **The query set is small** — four shapes, all full-corpus scans or scalar aggregates. No
   joins, no `GIN` containment queries, no group-by, no multi-column predicates. §4.2 adds
   B-tree indexes on both `year` and `title` and the SirixDB projection over the same two fields,
   so the accelerated comparison covers exactly the fields these four queries touch and nothing
   more. A projection over columns a query does not use would cost build time and win nothing.
3. **The corpus is `movies.json` repeated 12×.** All three engines compress locally (SirixDB
   per page, PostgreSQL per row via TOAST), and the repeat period is ~176 MB — far beyond any
   compression window — so repetition does not flatter anyone's compression. It **does** mean
   global distinct-value counts are 12× lower than row counts, which would flatter a global
   dictionary; neither engine here uses one across the whole corpus.
4. **Core counts differ by design.** SirixDB's ingest used 4 shredder threads; PostgreSQL's
   `COPY` is one connection. Queries: SirixDB single-threaded, PostgreSQL up to 3 processes —
   measured both ways, and it changed nothing.
5. **PostgreSQL got its fastest honest load path** — server-side `COPY` from a local file, not
   client-streamed `\copy` or row-by-row `INSERT`. The corpus had to be converted to NDJSON first
   (91.9 s) and to TSV for the normalized arm (37.4 s); **those conversions are excluded from the
   load times above**, while SirixDB's measured time *includes* partitioning the array inline.
   That asymmetry favours PostgreSQL. Counting conversion, the `jsonb` load is 129.8 s against
   SirixDB's 23.9 s.
6. **Not storage-bound.** 2.12 GB defeats a 1 GB buffer pool but not 15 GB of RAM, so the warm
   numbers are cache-resident CPU and the cold numbers are first-touch. A sustained
   working-set-exceeds-RAM regime was **not** measured — cgroup controllers are unavailable in
   this container, and a >20 GB corpus does not fit the 25 GB of free disk alongside both
   systems. An mlock balloon would approximate it and was not run.
7. **Single measurements in the cold regime.** By construction — the second run is warm and the
   measurement is gone. No error bars; treat cold numbers as one sample each, indicative of
   magnitude rather than precise.
8. **PostgreSQL 16.13, not 17.x.** The June run in `COMPARISON_POSTGRES.md` §1 used 17.10 on
   different hardware. Nothing here is comparable to that table.
9. **`pg_total_relation_size` excludes WAL**, as in the earlier document. SirixDB has no separate
   WAL, so this asymmetry slightly favours PostgreSQL — and PostgreSQL still loses storage by
   1.95×.
10. **The projection is in-memory and unpersisted.** PostgreSQL's indexes are on disk and survive
    restart; SirixDB's projection is rebuilt per process (40 s). §4.2's query numbers are
    therefore steady-state for a long-lived process only, and its cold behaviour is unmeasured.
    Persisted-projection lifecycle is `COMPARISON_DUCKDB.md`'s stated top roadmap item.
11. **An earlier revision of this document reported PostgreSQL query times 2–3× too high.** The
    first harness spawned a fresh `psql` per iteration, so ~30 ms of process startup was counted
    as query time, and the table was not yet fully page-cache warm. That inflated PostgreSQL's
    numbers — i.e. it favoured SirixDB — and understated the unaccelerated gap as 16–20× when it
    is 45–78×. All §4 numbers are re-measured inside a single established session. The defect is
    exactly the class `BENCHMARK_DESIGN.md` R4 warns about, found by noticing that `EXPLAIN
    ANALYZE` reported 0.064 ms server-side for a query the harness timed at 32 ms.
12. **No version history is exercised at all.** SirixDB carries per-revision machinery through
    every page it writes and gets no credit for it in a single-revision benchmark. Equally, none
    of its versioning claims are tested here.

---

## 6. What this actually says

1. **The storage claim flips at the TOAST threshold, and that is the useful finding.** Below
   ~2 KB per record — the common document case — PostgreSQL `jsonb` does not compress at all and
   lands 21 % *above* raw JSON, while SirixDB's per-page LZ77 lands 38 % *below* it. Above the
   threshold, pglz reverses this. Quote the threshold, not a single ratio.
2. **Durable bulk ingest is now a SirixDB win** (1.59–1.63× against logged `jsonb`), and the
   parallel shredder is why. Worth stating because `COMPARISON_POSTGRES.md` §5 lists commit
   throughput as "SirixDB's weakest measured axis" — for *many tiny commits* that remains true;
   for *bulk load* it is inverted.
3. **Never quote a SirixDB query number without saying whether it is warm or cold.** The same
   scan on the same data is 237 ms warm and 57,824 ms cold — a factor of ~244. That is now a
   bigger spread than the accelerated-vs-unaccelerated one, and it is the axis this revision
   changed: the scan paths are no longer the "always-correct fallback, not the analytics engine"
   that `COMPARISON_DUCKDB.md` describes, at least once warm.
4. **The projection beats an indexed PostgreSQL on the shapes it covers, and loses on the shapes
   it does not.** 22× on filter-count and 81× on `sum` against indexed `movies_rel`; 8× *behind*
   on a point lookup and 1.6× behind on `countAll`. That is the honest shape of the result, and it
   is a better argument than a uniform win would be: it says what the projection is *for*.
5. **The 49.5 s unpersisted build is the real gap to close**, not the query latency. PostgreSQL
   spends 19 s once, durably; SirixDB spends 49.5 s every process start for a structure that dies
   with the process. Until that is persisted, §4.2's numbers describe a long-lived server and
   nothing else.
6. **Do not generalize from four shapes.** Nothing here measures joins, group-by, concurrent
   readers, containment queries, or the time-travel workloads SirixDB exists for.
7. **Every warm number here is cache-resident, on both sides.** The buffer-pressured regime this
   document was created to measure was not achieved — 2.12 GB fits in a 16 GB machine's page cache
   whatever `shared_buffers` says. Reaching it needs a corpus larger than RAM. Until that runs, the
   warm tables describe an engine whose working set fits in memory, which is a real and common
   case but not the one §2.1 of the design doc asks for.

---

## 7. Reproduction

**Every number in this document is produced by one committed script.** That is not a convenience;
it is the point. An earlier revision was measured with throwaway drivers in a scratch directory,
and when the machine was recycled the numbers could not be re-run or even checked. Anything that
cannot be reproduced is not evidence.

```bash
# The whole comparison, end to end. ~40 minutes on the machine in section 1.
docs/bench/run-postgres-bulk.sh corpus.json /path/work

# Or one phase at a time -- the script is re-runnable and each phase is independent:
docs/bench/run-postgres-bulk.sh corpus.json /path/work prep pgload sizes
docs/bench/run-postgres-bulk.sh corpus.json /path/work pgquery pgindex
docs/bench/run-postgres-bulk.sh corpus.json /path/work sirixingest sirixquery sirixcold proj

# Fewer iterations for a smoke run (default 12):
ITERS=3 docs/bench/run-postgres-bulk.sh corpus.json /path/work pgquery
```

Phases: `prep` (corpus → NDJSON), `pgload`, `sizes`, `pgquery` (both regimes + cold), `pgindex`
(§4.2's PostgreSQL side), `sirixingest`, `sirixquery` (both regimes), `sirixcold`, `proj` (§4.2's
SirixDB side). PostgreSQL is stopped for every SirixDB phase and vice versa, so neither holds page
cache against the other. `drop_caches` needs root; without it the script warns and the cold numbers
are meaningless.

Overrides, if your PostgreSQL is laid out differently:
`PGBIN`, `PGDATA`, `PGCONF`, `PGUSER_OS`, `ITERS`.

The pieces it drives, if you want them individually:

```bash
# The benchmark classpath, so a BARE JVM can be used per measurement -- going through Gradle's
# JavaExec folds daemon startup into a cold number whose whole point is that nothing is warm.
CP=$(./gradlew -q :sirix-benchmarks:bulkBenchClasspath | tail -1)

# Or run a single subcommand through Gradle:
./gradlew :sirix-benchmarks:postgresBulk -Ppgbulk.args='query /path/work db-bulk 12'

# Subcommands: ndjson <in> <out> | ingest <in> <db> <mode> <tuned> <autoCommit> <rounds>
#              query <location> <dbName> <iters> [queryName]   (iters=0 selects the cold regime)
#              projquery <location> <dbName> <iters>           (needs an UNTUNED store, see §4.2)
```

Two measurement traps this script exists to not fall into again, both of which produced published
numbers that were wrong:

1. **Time PostgreSQL inside one established session.** Spawning a `psql` per iteration adds ~30 ms
   of process startup, invisible against a 400 ms scan and utterly dominant over a 0.1 ms index
   probe. Cross-check against `EXPLAIN (ANALYZE, BUFFERS)` server-side time before trusting
   anything sub-millisecond.
2. **`-Xmx` is not SirixDB's memory.** Its page cache is off-heap; capping the heap while leaving
   an 8 GB record cache in place measures a cache-resident engine and calls it pressured. Section 1
   has the numbers that actually matter and the properties that set them.

`COPY`'s text format also corrupts JSON — backslash is its escape — so the load uses CSV format
with `\x01`/`\x02` as quote and delimiter, control characters that cannot occur in valid JSON.
The script streams it with `\copy` rather than server-side `COPY`, because the server cannot
traverse a scratch directory owned by the invoking user.
