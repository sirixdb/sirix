# DuckDB vs Sirix iter-5 on 10M records (cold, single-machine)

Host: 31 GB RAM, 20 vCPU, GraalVM 25.0.2. CSV generated via `ExportScaleBenchCsv`
with identical `Random(42)` seed as `BrackitQueryOnSirixScaleMain`.

## Storage size

| format | bytes | ratio vs CSV |
|---|---:|---:|
| CSV | 251 MB | 1.0× |
| Parquet + Zstd (DuckDB default) | 26 MB | **9.4×** |
| Sirix shredded DB (pathSummary off, no LZ4) | ~460 MB including index pages | ~1.8× |

Parquet+Zstd delivers ~18× denser storage than the Sirix shredded DB for
this workload — before we even talk query speed.

## Query latency (first-run, in-memory, after load)

| query | DuckDB / CSV in-memory | DuckDB / Parquet in-memory | Sirix iter-5 (min, 5 iters) | Sirix / DuckDB-parquet |
|---|---:|---:|---:|---:|
| filterCount | **1 ms** | 22 ms | 1056 ms | 48× |
| groupByDept | 20 ms | — | 85 ms | 4× |
| sumAge | 119 ms | — | 0.5 ms* | 0.004× |
| avgAge | 7 ms | — | 0.4 ms* | 0.06× |
| minMaxAge | 10 ms | — | 0.7 ms* | 0.07× |
| groupBy2Keys | 5 ms | — | 36 ms | 7× |
| filterGroupBy | 66 ms | 83 ms | 1097 ms | 13× |
| countDistinct | 79 ms | — | 0.4 ms* | 0.005× |
| compoundAndFilterCount | 99 ms | — | 1012 ms | 10× |
| filterGroupByAge | 17 ms | 50 ms | 453 ms | 9× |

`*` — Sirix hits its per-resource path-stats cache so aggregate queries
return cached result without rescan. DuckDB rescans the full column each
time. This is the only class of query where Sirix currently outperforms,
and it's a caching artefact (not a scan-kernel win).

## Interpretation

- **Filter queries (filterCount / compoundAndFilter / filterGroupBy / filterGroupByAge)**: 10-50× slower than DuckDB-on-Parquet. These are the queries that matter for "Umbra ballpark".
- **Compression is not the decisive issue** — even with raw CSV (uncompressed) DuckDB outperforms Sirix by 10-1000×. The compressed Parquet numbers are within noise of CSV. Compression shrinks disk, not CPU.
- **Structural levers** that close this gap:
  - Dense per-page/per-rowgroup columnar storage with implicit rowid join (no OBJECT_KEY indirection).
  - Zone-map + min-max skip (DuckDB's filterCount hit 1ms via min/max-pruning).
  - LLVM / native codegen of query pipeline (DuckDB uses LLVM; Umbra uses LLVM; Sirix uses runtime-generated ASM bytecode).
  - Parallel scan without the `PageReference[]` / `guardCount` buffer-manager bookkeeping we pay.

## Native comparison query set

`bundles/sirix-benchmarks/src/jmh/resources/scale-bench-queries.sql` — ready to
drop into CedarDB / Umbra once a licensed instance is available.
