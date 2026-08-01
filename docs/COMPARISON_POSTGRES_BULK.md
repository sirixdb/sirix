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
bulk ingest** (1.59–1.63×). On **queries there is no single answer** — it depends entirely on what
each side is allowed to build first:

- *Neither side accelerated*: PostgreSQL wins the scan shapes by **45–78×**.
- *Both sides accelerated* (SirixDB's columnar projection vs PostgreSQL B-tree indexes): SirixDB
  wins filter-count by **32×** and `sum` by **192×** over the best PostgreSQL arm — but loses
  **point lookup 3.5×** and `countAll` **4.8×**, and pays a **40 s, non-persisted** build every
  process start against PostgreSQL's 4.4 s durable indexes.

A projection is scan acceleration; a B-tree is lookup acceleration. This benchmark shows both,
and neither subsumes the other.

Date: 2026-08-01.

---

## 1. Setup

| | |
|---|---|
| Machine | 4-vCPU cloud container (Intel Xeon @ 2.80 GHz), 15 GB RAM, virtualized storage on a single shared device, ext4 |
| SirixDB | this branch (`origin/main` @ `5ce5556`), **embedded**, OpenJDK 25.0.3, `-Xmx8g` |
| PostgreSQL | **16.13** (distro package, no Docker), local unix socket, `shared_buffers=1GB`, `work_mem=64MB`, `fsync=on`, `synchronous_commit=on`, `max_parallel_workers_per_gather=2` |
| Corpus | 2,116,427,425 B (2.12 GB) of Wikipedia movie records: **3,482,208 objects**, fields `{title, year, cast[], genres[], href, extract, thumbnail, thumbnail_width, thumbnail_height}` |
| Harness | `io.sirix.benchmark.PostgresBulkBench` (in-tree) + [`docs/bench/postgres-bulk-schema.sql`](bench/postgres-bulk-schema.sql) |
| Execution | strictly sequential — PostgreSQL was **stopped** during every SirixDB run and vice versa, so neither holds page cache against the other |

The corpus is real data, not synthetic: variable-length strings, nested string arrays, nulls, and
genuinely sparse fields (`href` 99.6 %, `extract` 95.2 %, `thumbnail` 82.8 % populated). It is
`movies.json` repeated 12× to reach 2 GB — see caveat 3.

### Regime (per BENCHMARK_DESIGN.md §2.1)

| Regime | Status |
|---|---|
| Cache-resident | not the target here |
| **Buffer-pressured** (corpus > engine caches, < RAM) | **measured** — 2.12 GB against a 1 GB buffer pool |
| **Cold** (caches dropped before each query) | **measured** — `drop_caches` + PostgreSQL restart / fresh JVM |
| Sustained storage-bound (working set > RAM) | **not run** — see caveat 6 |

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

Warm: 1 untimed warm-up + **min of 3**. PostgreSQL is timed with `\timing` **inside one
established session**, so the number is per-statement round-trip over the unix socket, not process
startup (see caveat 11 — an earlier revision of this document got this wrong). SirixDB is embedded
and in-process.

### 4.1 Neither side accelerated — sequential scan vs storage scan

| query | SirixDB scan | PG `jsonb` | PG normalized | winner |
|---|---:|---:|---:|---|
| `countAll` | **202 ms** | 246 ms | 253 ms | **SirixDB 1.2×** |
| `filterCountYear` | 19,321 ms | 420 ms | 255 ms | PostgreSQL **46–76×** |
| `sumYear` | 18,768 ms | 421 ms | 267 ms | PostgreSQL **45–70×** |
| `titleLookup` | 19,460 ms | 385 ms | 251 ms | PostgreSQL **51–78×** |

### 4.2 Both sides accelerated — SirixDB projection vs PostgreSQL B-tree indexes

SirixDB gets its in-memory columnar projection over `(year, title)`; PostgreSQL gets B-tree
indexes on the same two fields in both arms (expression indexes for `jsonb`).

| query | SirixDB projection | PG `jsonb` + index | PG normalized + index | winner |
|---|---:|---:|---:|---|
| `countAll` | 253 ms | **53 ms** | 56 ms | PostgreSQL **4.8×** |
| `filterCountYear` | **0.5 ms** | 197 ms | 16.2 ms | **SirixDB 32×** |
| `sumYear` | **0.3 ms** | 446 ms | 57.7 ms | **SirixDB 192×** |
| `titleLookup` | 0.4 ms | 0.172 ms | **0.113 ms** | PostgreSQL **3.5×** |

### Build cost — the asymmetry that decides whether 4.2 is real

| | cost | size | persisted? |
|---|---:|---:|---|
| SirixDB `(year, title)` projection | **39.9–43.2 s** | in-memory (3,401 leaves / 3,482,208 rows) | **no — rebuilt every process start** |
| PG normalized: `year` + `title` B-trees | 1.8 s + 2.6 s | 23 MB + 24 MB | yes |
| PG `jsonb`: two expression indexes | 4.9 s + 2.4 s | 23 MB + 24 MB | yes |

### Cold (caches dropped before each query, scan paths, no acceleration)

| query | SirixDB | PG `jsonb` | ratio |
|---|---:|---:|---|
| `countAll` | 40,566 ms | 2,849 ms | PG 14× |
| `filterCountYear` | 84,606 ms | 3,143 ms | PG 27× |
| `sumYear` | 76,274 ms | 2,880 ms | PG 26× |
| `titleLookup` | 82,408 ms | 1,677 ms | PG 49× |

Reading all of this honestly:

- **Unaccelerated, SirixDB loses badly and it is not a measurement artifact.** 45–78× on the three
  scan shapes. Parallelism does not explain it: PostgreSQL with
  `max_parallel_workers_per_gather=0` is within noise of its parallel numbers.
- **With the projection, the aggregate and filter shapes invert completely** — 32× and 192× ahead
  of the *best* PostgreSQL arm, which is itself indexed. A 0.3 ms `sum` over 3.48 M rows is a SIMD
  fold over ~27 MB of packed longs; that is the physics, and it matches
  `COMPARISON_DUCKDB.md`'s 16–22 ms for the same fold at 100 M records.
- **The projection does not win everything, and the two losses are structural, not tuning.**
  `titleLookup` is a *point* lookup: PostgreSQL descends a B-tree and touches 11 buffers
  (`EXPLAIN` confirms an Index Only Scan, 0.064 ms server-side), while the projection has no
  ordered access path and scans the whole dictionary-encoded title column. `countAll` is not a
  projection shape at all. **A projection is scan acceleration; a B-tree is lookup acceleration.
  They are different tools and this benchmark shows both.**
- **The build cost is the catch, and it is severe.** 40 s to build in memory, **not persisted**, so
  every process start pays it again — against 4.4 s for PostgreSQL's durable indexes. The
  projection wins a query by 192× and loses the first 40 seconds of the process's life. For a
  long-lived server that amortizes immediately; for a short-lived one it never does.
  `COMPARISON_DUCKDB.md` records persisted-projection lifecycle as the top roadmap item, and this
  measurement is why it matters.
- **Cold makes the unaccelerated gap worse, not better**, which kills the hopeful reading that a
  smaller footprint buys back cold-start latency: 1.95× fewer bytes on disk, still 26× slower to
  scan them cold. The cost is CPU per page (decompression plus node materialization), not I/O
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
3. **Never quote a SirixDB query number without saying whether a projection is built.** The same
   query on the same data is 19,321 ms on the scan path and 0.5 ms on the projection — a factor of
   ~39,000. Every analytical claim about SirixDB is a claim about the projection, and
   `COMPARISON_DUCKDB.md` already says the scan paths "exist as always-correct fallbacks, not as
   the analytics engine". This run is the PostgreSQL-side confirmation of exactly that.
4. **The projection beats an indexed PostgreSQL on the shapes it covers, and loses on the shapes
   it does not.** 32× on filter-count and 192× on `sum` against indexed `movies_rel`; 3.5× *behind*
   on a point lookup and 4.8× behind on `countAll`. That is the honest shape of the result, and it
   is a better argument than a uniform win would be: it says what the projection is *for*.
5. **The 40 s unpersisted build is the real gap to close**, not the query latency. PostgreSQL
   spends 4.4 s once, durably; SirixDB spends 40 s every process start for a structure that dies
   with the process. Until that is persisted, §4.2's numbers describe a long-lived server and
   nothing else.
6. **Do not generalize from four shapes.** Nothing here measures joins, group-by, concurrent
   readers, containment queries, or the time-travel workloads SirixDB exists for.

---

## 7. Reproduction

```bash
# 0. Corpus -> NDJSON for PostgreSQL's COPY (SirixDB reads the array directly)
java -cp <bench-cp> io.sirix.benchmark.PostgresBulkBench ndjson corpus.json corpus.ndjson

# 1. PostgreSQL
initdb -D "$PGDATA" -A trust -U postgres          # must run as an unprivileged user
#   shared_buffers=1GB, fsync=on, synchronous_commit=on
psql -f docs/bench/postgres-bulk-schema.sql
#   COPY text format corrupts JSON (backslash is its escape); use CSV with control characters
#   that cannot occur in valid JSON:
psql -c "COPY movies_jsonb (doc) FROM 'corpus.ndjson' \
         WITH (FORMAT csv, QUOTE E'\x01', DELIMITER E'\x02');"

# 2. SirixDB ingest (best of 3; 'true' = tuned, 'false' = defaults)
java <flags> -cp <bench-cp> io.sirix.benchmark.PostgresBulkBench \
     ingest corpus.json /path/db partitioned true 100000 3 KEEP_OPEN

# 3. Queries — warm (min of 3) and cold (0 = no warm-up, single timed run)
java <flags> -cp <bench-cp> io.sirix.benchmark.PostgresBulkBench query /path db-name 3
sync; echo 3 > /proc/sys/vm/drop_caches
java <flags> -cp <bench-cp> io.sirix.benchmark.PostgresBulkBench query /path db-name 0 sumYear

# 4. Same queries through the (year, title) columnar projection (§4.2)
java <flags> -cp <bench-cp> io.sirix.benchmark.PostgresBulkBench projquery /path db-name 3

# 5. PostgreSQL's side of §4.2 — B-tree indexes on the same two fields
psql -c "CREATE INDEX mr_year  ON movies_rel (year)"
psql -c "CREATE INDEX mr_title ON movies_rel (title)"
psql -c "CREATE INDEX mj_year  ON movies_jsonb (((doc->>'year')::int))"
psql -c "CREATE INDEX mj_title ON movies_jsonb ((doc->>'title'))"
psql -c "VACUUM ANALYZE movies_rel" -c "VACUUM ANALYZE movies_jsonb"

# Time PostgreSQL INSIDE one session. Spawning a psql per iteration adds ~30 ms of process
# startup, which is invisible against a 400 ms scan and dominates a 0.1 ms index lookup:
#   psql -q -t <<'EOF'
#   \timing on
#   SELECT count(*) FROM movies_rel WHERE title = 'Saleslady';   -- repeat, take the min
#   EOF
# Cross-check against EXPLAIN (ANALYZE, BUFFERS) server-side time before trusting any of it.

# flags: --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED
#        --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED -Xmx8g
```

Stop each system while the other is measured; otherwise they contend for page cache.
