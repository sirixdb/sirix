# SirixDB vs DuckDB: Analytical Queries at 100M Records

The honest head-to-head for the analytical fast paths: DuckDB is a
best-in-class columnar OLAP engine — the bar. SirixDB is a versioned,
bitemporal document store whose analytical layer (an in-memory columnar
projection over the versioned tree) aims for the same physics on the
shapes it covers.

## Setup

| | |
|---|---|
| Machine | 20-core Linux (32 GB RAM, NVMe), same box for both engines |
| Dataset | 100,000,000 records `{id, age: 18..65, dept: 8 values, city: 8 values, active: bool}` — identical uniform distributions in both systems |
| DuckDB | 1.5.2 (official binary), native table generated in-engine, `threads=20`, 1 untimed warmup + 3 timed runs per query, `fetchall()` materialization |
| SirixDB | dev build (1.0.0-beta1 branch), JVM (Oracle GraalVM 25.0.3, `-Xmx8g`), wildcard projection over (age, active, dept, city, amount, score), **fresh `SirixVectorizedExecutor` per timed iteration** so executor-level result caches never serve a timed run; store, page caches and projection shared across iterations — the moral equivalent of DuckDB's loaded table. 1 untimed warmup + 3 timed runs; results serialized and verified byte-identical to SirixDB's interpreted pipeline |
| Harness | `io.sirix.query.bench.SirixVsDuckBenchMain` (in-tree) + `duck_bench.py` |

Durable difference to keep in mind: DuckDB's columns are its primary,
persisted storage format. SirixDB's projection is a secondary structure over
the versioned store — every revision of every document remains queryable
underneath it.

## Results (min of 3, milliseconds)

SirixDB measured twice: on the JVM (Oracle GraalVM 25.0.3 JIT) and as a
**profile-guided-optimized native binary** (GraalVM Native Image, `-O3
--pgo` with instrumented profiles from both projection modes,
`-H:+VectorAPISupport`).

| query | SirixDB PGO native | SirixDB JVM | DuckDB | best vs DuckDB |
|---|---:|---:|---:|---|
| `count(... where age > 40 and active)` | **33** | 53 | 40 | **SirixDB ahead** |
| `group by dept → count` | 71 | 80 | 28 | 2.5× |
| `sum(age)` | 16 | 22 | 10 | 1.6× |
| `avg(age)` | 18 | 22 | 16 | 1.1× |
| `min(age), max(age)` | 19 | 21 | 18 | 1.1× |
| `group by dept, city → count` (two keys) | 240 | 251 | 115 | 2.1× |
| `where active group by dept → count` | **43** | **41** | 59 | **SirixDB ahead** |
| `count(distinct dept)` | 81 | 76 | 18 | 4.2× |
| `count(... where age > 30 and age < 50 and active)` | **42** | 52 | 44 | **SirixDB ahead** |

The PGO native binary comes out **ahead of DuckDB on three of the nine
shapes** and within 1.1–2.5× on all but count-distinct. Two
native-image findings worth knowing: `-H:+VectorAPISupport` is OFF by
default (without it every SIMD kernel runs as fallback objects — the same
suite measured 10–600× slower), and the projection *build* runs ~7.6×
slower under native than the JVM (cursor-walk code; build on JVM or use a
persisted projection). Ingest: DuckDB generated its 100M-row table in 27.3 s; SirixDB shredded
the same logical data into its versioned store at 182k records/s (548 s,
including path-summary construction) — different jobs (a column table vs a
fully versioned tree), stated for completeness.

## What this took (the gap was 3 orders of magnitude two days ago)

Measured on the same machine and dataset during development:

| query | before | after | mechanism |
|---|---:|---:|---|
| `sum/avg/min+max(age)` | ~59,000 ms | 21–22 ms | aggregates previously page-walked the per-page number regions; now a parallel `NUMERIC_LONG` column fold over projection leaves (full-word fast path per 64-row mask block) |
| `group by dept, city` | ~43,700 ms | 251 ms | multi-key grouping previously took the typed record-at-a-time slot walk; now a composite dict-id sweep over projection leaves with per-leaf lazy key compose |
| `group by age` (numeric key) | **empty result** | correct, ~1.4 s scan-path | the string-only kernels silently dropped non-string group keys; typed kernels + verification counters fixed the wrong-results family |

## Correctness gates (how we keep "fast" honest)

- Every fast path is **fail-closed**: the optimizer claims a pipeline only
  when the query shape provably matches the kernel's output, and kernels
  verify their own coverage (e.g. a projection column that ever saw a
  non-integral number is flagged by the builder, and value-exact consumers —
  aggregates, numeric comparisons — decline it rather than serve truncated
  values).
- A 27-case differential suite runs every shape through the vectorized AND
  interpreted pipelines and requires **byte-identical serialized results**,
  including typed group keys, multi-key, renamed outputs, predicated
  variants, and the must-decline cases.

## Honest caveats

- **Cold projection build**: constructing the 6-column projection at 100M
  costs ~319 s (one full-DB walk). Re-encoding an already-persisted
  projection takes ~1.2 s. Persisting wider projections currently trips a
  known HOT-storage chunk-split limitation (documented in
  `KNOWN_LIMITATIONS.md`) — persisted-projection lifecycle is the top
  roadmap item this comparison motivates.
- **Sparse/absent fields**: projection columns have no NULL representation
  (absent values read as defaults); anchor-based scans don't visit records
  lacking the anchor field. Dense, uniformly-shaped record sets — the target
  analytical workload — are exact; sparse data falls back or is documented.
- **count(distinct)** and single-key group-by still trail 3–4×: the dict-id
  paths do per-leaf dictionary decode + hash merge that DuckDB amortizes in
  its hash tables; known optimization headroom.
- DuckDB numbers are its general-purpose engine doing what it was built
  for; SirixDB reaches this neighborhood only on the projected shapes, and
  falls back to scan-class latencies (page-region paths: tens of ms at 1M,
  seconds at 100M) without a projection.

## Reproduction

```bash
# DuckDB
python3 duck_bench.py 100000000 3 /tmp/duck-100m.db

# SirixDB: shred with a path summary, then run the matrix
java --add-modules jdk.incubator.vector -Xmx8g \
  -DbuildPathSummary=true -Dsirix.shredDbPath=/tmp/sirix-100m \
  -cp <sirix-query cp> io.sirix.query.bench.ScaleBenchMain 100000000 true 0
java --add-modules jdk.incubator.vector -Xmx8g \
  -Dsirix.projection.forceRebuild=true -Dsirix.projection.persist=false \
  -cp <sirix-query cp> io.sirix.query.bench.SirixVsDuckBenchMain /tmp/sirix-100m 3 20 true
```
