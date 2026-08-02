# SirixDB vs PostgreSQL: 2 GB of JSON, no version history

An honest same-machine comparison on a **bulk JSON corpus**, run because
[`COMPARISON_POSTGRES.md`](COMPARISON_POSTGRES.md) §0.14 established that the existing
PostgreSQL comparison cannot support storage claims — its 16 MiB corpus is entirely
cache-resident on both sides — and [`BENCHMARK_DESIGN.md`](BENCHMARK_DESIGN.md) §2.1
specifies a corpus larger than the engine caches as the fix.

This is that corpus. It is a **different experiment** from `COMPARISON_POSTGRES.md`: no version
history, one large dataset. It does not supersede that document and does not rehabilitate its
numbers.

**Headline.** SirixDB wins **storage** decisively (1.95× smaller than `jsonb`) and wins **durable
bulk ingest** (1.59–1.63×). On queries PostgreSQL still wins the scan shapes, by a much smaller
margin than before:

- *Neither side accelerated, warm*: **PostgreSQL wins all four shapes by 2.5–5×.** The previous
  revision measured that gap at **45–78×**, so the engine work on this branch closed it by ~7.5×
  — but it did not close it. An earlier draft of this revision claimed SirixDB had overtaken
  PostgreSQL here; that was a harness artifact and is corrected in §4.1.
- *Cold*: **PostgreSQL wins by 12–23×**. Still the largest gap in the comparison.
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
genuinely sparse fields (`href` 99.6 %, `extract` 95.2 % `thumbnail` 82.8 % populated). It is the
in-repo `bundles/sirix-core/src/test/resources/json/movies.json` — 36,273 Wikipedia movie records —
minified and repeated **96×**, which is exactly 3,482,208 records. Build it from a clean checkout
with [`docs/bench/make-corpus.py`](bench/make-corpus.py); see caveat 3 for what repetition does and
does not distort.

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
| **Cache-resident** | **measured** — regime B |
| **Buffer-pressured** (corpus > engine caches, < RAM) | **measured** — regime A, where SirixDB's 1 GB record cache is below its own 1.24 GB store |
| **Cold** (caches dropped before each query) | **measured** — `drop_caches` + PostgreSQL restart / fresh JVM |
| Sustained storage-bound (working set > RAM) | **not run** — see caveat 6 |

The two regimes behave completely differently, but **not symmetrically**, and an earlier draft of
this document got the reason wrong. For PostgreSQL the OS page cache really does absorb the
difference: 2.5 GB of `jsonb` fits in a 16 GB machine's page cache, so lowering `shared_buffers`
from 8 GB to 1 GB moves reads from its buffer pool to the kernel's rather than to the device, and
its timings barely move. For SirixDB it does not, because a record-cache miss costs a decode rather
than a copy — see §4.1. Regime A is therefore a genuine buffer-pressured measurement for SirixDB
and a mild one for PostgreSQL, which is itself the finding.

`/sys/fs/cgroup` on this container turns out to expose a working **cgroup v1 memory controller**
(an earlier revision said no controllers were available). Reading a 1.24 GB file inside a 512 MB
cgroup caps usage at 511 MB, so the design doc's preferred hard limit is usable here after all;
regime A does not need it, because SirixDB's own cache cap already produces the pressure, but it is
the right tool for the sustained storage-bound regime caveat 6 still lists as unrun.

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

> **A correction, and it changes the conclusion.** An earlier draft of this section reported
> SirixDB at 94–265 ms and claimed it beat PostgreSQL on every scan shape. That was a measurement
> artifact, not a result. `PostgresBulkBench` bound `$doc` **once** and then timed N repeats, and
> `AbstractJsonDBArray` memoizes its element list on first access — so the untimed warm-up pass
> materialized all 3,482,208 elements as `Sequence` objects and every timed iteration afterwards
> walked that Java list instead of the store, while PostgreSQL re-executed a full heap scan each
> time. Measured directly, with the page cache and JIT equally warm and only the binding
> refreshed: `countAll` 96.5 ms reusing the binding against **1,177.3 ms** rebound (12.2×), and
> `filterCountYear` 203.8 ms against **2,884.0 ms** (14.1×). The harness now rebinds per iteration
> by default (`-Dsirix.bench.reuseBinding=true` restores the old behaviour, which must not be
> published as a scan number). Everything below is the rebound measurement.

**Regime A — matched at ~1 GB of cache each.** This is the buffer-pressured regime, and it is
where the architectural difference shows.

| query | SirixDB scan | PG `jsonb` | PG normalized | winner |
|---|---:|---:|---:|---|
| `countAll` | 20,850 ms | 291.0 ms | **239.6 ms** | PostgreSQL **72–87×** |
| `filterCountYear` | 21,140 ms | 386.4 ms | **249.4 ms** | PostgreSQL **55–85×** |
| `sumYear` | 20,795 ms | 396.7 ms | **261.4 ms** | PostgreSQL **52–80×** |
| `titleLookup` | 21,211 ms | 352.3 ms | **244.3 ms** | PostgreSQL **60–87×** |

SirixDB's 1 GB record cache is smaller than its own 1.24 GB store, and the result is not a
gentle degradation — it is a **15–28× cliff** off regime B, landing within half of the *cold*
numbers below. PostgreSQL barely moves between the regimes.

**Why the cliff is asymmetric, and it is the important finding here.** A `shared_buffers` miss in
PostgreSQL usually falls through to the OS page cache and costs a `memcpy` of an 8 KB page whose
bytes are already in RAM. A record-cache miss in SirixDB costs an **LZ77 decompress plus a full
slotted-page deserialization**, and the OS page cache does not help with either — it saves the
read, not the decode. Profiling a cold scan puts `deserializeSlottedPage` at **53.9 %** of CPU
(LZ77 decode 16.7 %, region-table reads 12.0 %) against 5.2 % in `pread`. SirixDB's buffer manager
caches *materialized objects*, so its miss penalty is CPU, not I/O, and it does not shrink when the
data is already in RAM.

That is the mechanism behind every unfavourable number in this document, and it is why the cold and
pressured columns are nearly identical.

**Regime B — matched at 8 GB of cache each**

| query | SirixDB scan | PG `jsonb` | PG normalized | winner |
|---|---:|---:|---:|---|
| `countAll` | 747.4 ms | 304.6 ms | **153.8 ms** | PostgreSQL **2.5–4.9×** |
| `filterCountYear` | 1,424.1 ms | 423.1 ms | **291.1 ms** | PostgreSQL **3.4–4.9×** |
| `sumYear` | 1,361.3 ms | 421.4 ms | **340.1 ms** | PostgreSQL **3.2–4.0×** |
| `titleLookup` | 1,282.9 ms | 382.2 ms | **273.0 ms** | PostgreSQL **3.4–4.7×** |

Run-to-run spread on the SirixDB column is ±15–25 %; treat differences smaller than that as noise.

**The same four shapes under JMH**, because min-of-12 is not a steady-state estimator and
[`BENCHMARK_DESIGN.md`](BENCHMARK_DESIGN.md) R4 asks for one. `BulkQueryScanBenchmark`, 5 warm-up
+ 10 measured iterations, same store, same regime-B caches:

| query | SirixDB, JMH mean | SirixDB, min-of-12 above |
|---|---:|---:|
| `countAll` | 798.1 ± 22.7 ms | 747.4 ms |
| `filterCountYear` | 1,726.3 ± 76.1 ms | 1,424.1 ms |
| `sumYear` | 1,574.7 ± 64.0 ms | 1,361.3 ms |
| `titleLookup` | 1,597.1 ± 26.0 ms | 1,282.9 ms |

The means run **7–25 % above the minima**, which is what a min-of-N estimator does: it reports the
luckiest sample, not the steady state. The table above it compares min against min, so it is
internally consistent and its ratios stand; this table is the more conservative measurement of the
SirixDB side, and comparing it against PostgreSQL's *minima* would overstate PostgreSQL's lead by
roughly that same 7–25 %. PostgreSQL was not re-measured under a mean estimator (it is a separate
process, not a JMH-able callee), so no winner is restated from mixing the two.

**PostgreSQL wins every warm unaccelerated scan shape, by 2.5–5×.** That is the honest result, and
it is the same direction the previous revision reported — but the margin has closed a great deal.
That revision measured SirixDB scans at 19,321 / 18,768 / 19,460 ms and put PostgreSQL ahead by
45–78×; the same shapes now run at 1,424 / 1,361 / 1,283 ms, roughly **13× faster than before**.
The engine work on this branch is what moved them — chiefly the clock sweeper no longer evicting a
cache nowhere near its budget (it was emptying ~10 % of the cache per cycle regardless of headroom,
so every scan re-read and re-decompressed the whole resource with **zero** cache hits), plus
removing a per-element cursor re-anchor that was 78.7 % of all allocations, and caching path-class
records per revision.

So: a real and large improvement, and still a real gap. Nothing here supports a claim that SirixDB
beats PostgreSQL on unaccelerated scans.

#### Where the remaining warm gap goes — the walk itself, bisected

"Trie navigation should be faster than a heap scan" is a claim, so it was measured directly, with no
query engine on top. `CursorWalkBisectBenchmark` walks the same 3,482,208 array elements three ways
against a warm regime-B store. It is a **JMH** benchmark, not a timing loop —
[`BENCHMARK_DESIGN.md`](BENCHMARK_DESIGN.md) R4, and because the first pass over this corpus is a
~50 s cold page load that a hand-rolled loop folds straight into the first sample. 2 forks × (5
warm-up + 10 measured) iterations:

| walk | what it does | ms/op | per element |
|---|---|---:|---:|
| `denseMoveTo` | `moveTo` over 3,482,208 **consecutive** node keys — perfect locality | 98.0 ± 10.0 | **28.1 ns** |
| `stridedMoveTo` | `moveTo` over the element keys, collected up front into a `long[]` | 200.1 ± 17.2 | **57.5 ns** |
| `siblingWalk` | the ordinary walk: `moveToRightSibling()` from each element | 726.8 ± 25.1 | **208.7 ns** |

All three perform exactly 3,482,208 binds, and they differ only in where the next node key comes
from, so the deltas are attributable:

* **28 ns is the bind floor** — resolve the page, read the slot directory, point the flyweight
  cursor at the record — with the next key already in a register and the page resident. It will not
  get much smaller without changing what a bind *is*.
* **+29 ns for locality (28 → 58 ns, ×2.0).** Dense and strided do the *same* work per call; strided
  just skips ~15 keys between calls, because each element's ~9 field nodes sit between it and the
  next (measured mean stride 14.9). That is enough to leave the record's cache line every time.
* **+151 ns for the pointer chase (58 → 209 ns, ×3.6).** Strided and sibling visit identical keys in
  identical order. In strided the next key comes from a sequential `long[]` the prefetcher runs
  ahead of; in sibling it is a field of the record just bound, so each hop *depends* on the previous
  load and nothing overlaps.

**The dependency chain, not the bind, is the cost**: 151 of 209 ns per element — 72 % — is the walk
waiting on its own previous load. And 726.8 ms for the bare walk against a `countAll` of 798.1 ms
measured the same way, on the same store, means **the sibling walk is 91 % of the warm scan**; query
compilation, sequence construction and serialization are the remaining 9 %.

For comparison, PostgreSQL's 154 ms `countAll` is ~44 ns/tuple — between SirixDB's bind floor and
its strided cost, and well under the pointer-chase figure. A heap scan walks a line-pointer array
inside a page: no per-tuple binding, and the next offset is two bytes further along the same cache
line.

So the honest conclusion is that **a faster bind cannot close this gap.** Even a free bind leaves
181 ns/element of locality and dependency stalls. Two things can:

1. **Prefetch the chain.** The sibling key is known one hop early; issuing the next record's load
   before the current element is materialized would hide most of the 151 ns, because the stall is
   latency, not bandwidth.
2. **Don't walk per element.** That is what the columnar projection in §4.2 does — and it is exactly
   why that is the arm where SirixDB wins by 22–81×.

### 4.2 Both sides accelerated — SirixDB projection vs PostgreSQL B-tree indexes

SirixDB gets its in-memory columnar projection over `(year, title)`; PostgreSQL gets B-tree
indexes on the same two fields in both arms (expression indexes for `jsonb`).

Re-measured this revision, like everything else here, and with the per-iteration rebinding §4.1
describes. The three projection-served shapes are essentially unchanged by that correction — the
vectorized path does not go through the array element memo — while `countAll`, which does, moved
from 92.3 ms to 1,322.0 ms.

> The projection needs a store built with the path summary, i.e. the **untuned** configuration.
> The tuned one the ingest section measures sets `buildPathSummary(false)` — it is one of the
> features PostgreSQL's plain table has no equivalent for — and the projection installer reads the
> summary to discover the fields to project. Run against a tuned store it fails with
> `Node couldn't be fetched from persistent storage!`, which is an unhelpful way to say "there is
> no path summary here". The driver script builds a separate untuned store for this phase.

| query | SirixDB projection | PG `jsonb` + index | PG normalized + index | winner |
|---|---:|---:|---:|---|
| `countAll` | 1,322.0 ms | **57.0 ms** | 57.6 ms | PostgreSQL **23×** |
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

Cold is **one of two conclusions this branch did not change** (the other being that PostgreSQL
wins warm scans). The warm scans improved by ~7.5×;
cold moved far less (`filterCountYear` 84.6 s → 45.5 s, `sumYear` 76.3 s → 41.4 s, but
`countAll` 40.6 s → 46.6 s, i.e. worse). That is consistent with what the warm fixes actually were — they stop
the engine from throwing away a warm cache, which does nothing for a cache that starts empty. **A
warm SirixDB scan is ~16–18× faster than the same scan cold**, so the cold penalty is roughly an
order of magnitude rather than the ~200× an earlier draft computed from memoized warm numbers. Whatever dominates cold page
load is untouched and is the obvious next thing to profile.

Reading all of this honestly:

- **Unaccelerated and warm, PostgreSQL still wins, by 2.5–5×** — down from 45–78× two revisions
  ago, a real ~13× improvement on the SirixDB side but not a reversal. Parallelism
  does not explain the PostgreSQL side either way: with `max_parallel_workers_per_gather=0` it is
  within noise of its parallel numbers.
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
3. **The corpus is `movies.json` repeated 96×.** All three engines compress locally (SirixDB
   per page, PostgreSQL per row via TOAST), and the repeat period is ~22 MB — far beyond any
   compression window — so repetition does not flatter anyone's compression. It **does** mean
   global distinct-value counts are 96× lower than row counts, which would flatter a global
   dictionary; neither engine here uses one across the whole corpus.

   A provenance note, because it matters for reproduction. The sections above were measured
   against a 2,116,427,425 B build of this corpus that was assembled by a throwaway script and did
   not survive the machine. `make-corpus.py` rebuilds the same 3,482,208 records from the checkout
   and yields **2,114,044,225 B** — 0.11 % smaller, from minification details, not from different
   data. The §4.1 walk bisection was measured on the rebuilt corpus; everything else on the
   original. Treat a 0.11 % corpus difference as inside the ±15–25 % run-to-run spread, but do not
   expect byte-identical store sizes.
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
# 0. Build the corpus itself from the checkout -- 3,482,208 records, 2,114,044,225 B, ~11 s.
docs/bench/make-corpus.py \
    bundles/sirix-core/src/test/resources/json/movies.json /path/work/corpus.json 96

# The whole comparison, end to end. ~40 minutes on the machine in section 1.
docs/bench/run-postgres-bulk.sh /path/work/corpus.json /path/work

# Or one phase at a time -- the script is re-runnable and each phase is independent:
docs/bench/run-postgres-bulk.sh /path/work/corpus.json /path/work prep pgload sizes
docs/bench/run-postgres-bulk.sh /path/work/corpus.json /path/work pgquery pgindex
docs/bench/run-postgres-bulk.sh /path/work/corpus.json /path/work sirixingest sirixquery sirixcold
docs/bench/run-postgres-bulk.sh /path/work/corpus.json /path/work sirixwalk proj

# Fewer iterations for a smoke run (default 12):
ITERS=3 docs/bench/run-postgres-bulk.sh /path/work/corpus.json /path/work pgquery
```

Phases: `prep` (corpus → NDJSON), `pgload`, `sizes`, `pgquery` (both regimes + cold), `pgindex`
(§4.2's PostgreSQL side), `sirixingest`, `sirixquery` (both regimes), `sirixcold`, `sirixwalk`
(§4.1's cursor-hop bisection, under JMH), `sirixscan` (§4.1's four warm shapes, under JMH), `proj`
(§4.2's SirixDB side). PostgreSQL is stopped for every SirixDB phase and vice versa, so neither
holds page cache against the other. `drop_caches` needs root; without it the script warns and the
cold numbers are meaningless.

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
