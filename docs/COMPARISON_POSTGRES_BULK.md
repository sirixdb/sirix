# SirixDB vs PostgreSQL: 2 GB of JSON, no version history

An honest same-machine comparison on a **bulk JSON corpus**, run because
[`COMPARISON_POSTGRES.md`](COMPARISON_POSTGRES.md) §0.14 established that the existing
PostgreSQL comparison cannot support storage claims — its 16 MiB corpus is entirely
cache-resident on both sides — and [`BENCHMARK_DESIGN.md`](BENCHMARK_DESIGN.md) §2.1
specifies a corpus larger than the engine caches as the fix.

This is that corpus. It is a **different experiment** from `COMPARISON_POSTGRES.md`: no version
history, one large dataset. It does not supersede that document and does not rehabilitate its
numbers.

**Headline: SirixDB wins storage decisively (1.95× smaller than `jsonb`) and wins durable
ingest (1.63×). PostgreSQL wins every scan query — 16–20× warm against the document-parity
`jsonb` arm, 61–64× against a normalized schema, and 14–49× cold. Both findings are large and
neither is close.**

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

## 4. Queries — PostgreSQL wins, 16–64× warm and 14–49× cold

Warm: 1 untimed warm-up + **min of 3**. Cold: OS page cache dropped **and** the server restarted
(or a fresh JVM) before **each** query, single timed execution.

### Warm (buffer-pressured)

| query | SirixDB | PG `jsonb` | PG `jsonb` 1-thread | PG normalized | best PG vs SirixDB |
|---|---:|---:|---:|---:|---|
| `countAll` | **202 ms** | 776 ms | 696 ms | 285 ms | **SirixDB 1.4× ahead** |
| `filterCountYear` | 19,321 ms | 1,168 ms | 1,051 ms | 304 ms | PG **63×** |
| `sumYear` | 18,768 ms | 1,134 ms | 1,081 ms | 309 ms | PG **61×** |
| `titleLookup` | 19,460 ms | 951 ms | 982 ms | 306 ms | PG **64×** |

### Cold (caches dropped before each query)

| query | SirixDB | PG `jsonb` | ratio |
|---|---:|---:|---|
| `countAll` | 40,566 ms | 2,849 ms | PG 14× |
| `filterCountYear` | 84,606 ms | 3,143 ms | PG 27× |
| `sumYear` | 76,274 ms | 2,880 ms | PG 26× |
| `titleLookup` | 82,408 ms | 1,677 ms | PG 49× |

Reading this honestly:

- **This is a real gap and it is not a measurement artifact.** Parallelism does not explain it:
  PostgreSQL with `max_parallel_workers_per_gather=0` is within noise of its parallel numbers
  (sometimes faster — parallel setup costs on 4 cores), and it still beats single-threaded
  SirixDB by 16–20× against the `jsonb` arm.
- **`countAll` is the one SirixDB win**, and it is a structural one rather than a scan victory:
  SirixDB answers it from stored child counts without visiting records. Its 200× warm-to-cold
  ratio (202 ms → 40.6 s) shows exactly that — cold, it has to fault the structure in.
- **The scan queries are SirixDB's scan-path fallbacks, not its analytical engine.**
  [`COMPARISON_DUCKDB.md`](COMPARISON_DUCKDB.md) documents this directly: without a projection
  index, "analytical workloads belong on the projection; the scan paths exist as always-correct
  fallbacks, not as the analytics engine." **No projection index was built for this run**, so
  these numbers measure the fallback. Whether a projection closes the gap on this corpus is
  unmeasured here and should not be assumed.
- **Cold makes it worse, not better**, which rules out the hopeful reading that SirixDB's smaller
  footprint buys back cold-start latency. It stores 1.95× fewer bytes and still takes 26× longer
  to scan them cold — the cost is CPU per page (decompression plus node materialization), not I/O
  volume. The device moved PostgreSQL's 2.4 GB in ~2.9 s (~800 MB/s), so neither side is
  device-starved.

---

## 5. Honest caveats

1. **Different jobs.** SirixDB built a fully versioned, node-addressable tree with stable node
   keys; PostgreSQL built a heap of independent rows. Every SirixDB revision remains queryable and
   diffable. That capability is the reason to accept a slower scan, and it is not something these
   timings credit it for.
2. **The query set is small** — four shapes, all full-corpus scans or scalar aggregates. No
   joins, no index-backed lookups, no `GIN` containment queries, no group-by. `titleLookup` is a
   sequential scan on **both** sides (no index was built on either), so it measures scan speed,
   not lookup capability. A `GIN`/B-tree index would move PostgreSQL's number by orders of
   magnitude and was deliberately not built, since SirixDB had no index either.
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
10. **No version history is exercised at all.** SirixDB carries per-revision machinery through
    every page it writes and gets no credit for it in a single-revision benchmark. Equally, none
    of its versioning claims are tested here.

---

## 6. What this actually says

1. **The storage claim flips at the TOAST threshold, and that is the useful finding.** Below
   ~2 KB per record — the common document case — PostgreSQL `jsonb` does not compress at all and
   lands 21 % *above* raw JSON, while SirixDB's per-page LZ77 lands 38 % *below* it. Above the
   threshold, pglz reverses this. Quote the threshold, not a single ratio.
2. **Durable bulk ingest is now a SirixDB win** (1.59–1.63× against logged `jsonb`), and the
   parallel shredder is why. This is worth stating because
   `COMPARISON_POSTGRES.md` §5 lists commit throughput as "SirixDB's weakest measured axis" — for
   *many tiny commits* that remains true; for *bulk load* it is now inverted.
3. **Query latency is the standing gap, and it is large.** 16–20× warm against the `jsonb` arm
   (61–64× against a normalized schema), 14–49× cold — all against PostgreSQL's ordinary
   sequential scan with no index. Two levers are known and unmeasured on
   this corpus: the projection index (`COMPARISON_DUCKDB.md` shows it worth ~1000× over scan
   paths at 100M records) and per-page decode cost (`COMPARISON_POSTGRES.md` §0.12 identifies it
   as the read-path lever). Neither should be quoted as a fix until measured here.
4. **Do not generalize from four scans.** This says nothing about indexed lookups, joins,
   concurrent readers, or the time-travel workloads SirixDB exists for.

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

# flags: --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED
#        --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED -Xmx8g
```

Stop each system while the other is measured; otherwise they contend for page cache.
