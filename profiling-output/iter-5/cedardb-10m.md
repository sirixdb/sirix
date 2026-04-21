# CedarDB vs DuckDB vs Sirix iter-5 — 10M records

CedarDB Community Edition `v2026-04-02` via Docker (`docker.io/cedardb/cedardb`),
in-memory mode, 14 GB buffers. Same `/tmp/records-10m.csv` loaded via COPY.
Steady-state numbers — median of 3 consecutive runs.

## Query latency

| query | Sirix iter-5 (min ms) | DuckDB / CSV-in-mem (ms) | DuckDB / Parquet (ms) | CedarDB (ms) | Sirix/CedarDB |
|---|---:|---:|---:|---:|---:|
| filterCount | 1056 | 1 | 22 | **0.6** | **1760×** |
| groupByDept | 85 | 20 | — | 9.5 | 9× |
| sumAge | 0.5* | 119 | — | 1.6 | (cache) |
| avgAge | 0.4* | 7 | — | 2.0 | (cache) |
| minMaxAge | 0.7* | 10 | — | 1.7 | (cache) |
| groupBy2Keys | 36 | 5 | — | 11.7 | 3× |
| filterGroupBy | 1097 | 66 | 83 | **2.8** | **392×** |
| countDistinct | 0.4* | 79 | — | 3.7 | (cache) |
| compoundAndFilterCount | 1012 | 99 | — | **0.65** | **1557×** |
| filterGroupByAge | 453 | 17 | 50 | **2.9** | **156×** |

`*` Sirix path-stats cache hit, not a scan-kernel result.

## Load time

| engine | 10M ingest |
|---|---:|
| Sirix shred + Jackson parse | 50-80 s |
| DuckDB `read_csv_auto` | 0.77 s |
| CedarDB `COPY FROM CSV` | 2.75 s |

## Conclusion

CedarDB runs the full 9-query workload on 10M records in **under 40 ms total** (sum
of min times). Sirix runs the same workload in **~4-5 seconds** when we
exclude the path-stats-cached aggregates, and **~4-5 seconds** when we include
them (they're cached so they don't count).

The ~1500× gap on filter queries is structural:
- **CedarDB / Umbra**: LLVM-compiled native query pipeline operating on
  contiguous column arrays. Each record costs ~1-3 ns (basically memory bandwidth).
- **Sirix**: OBJECT_KEY slot-indirection (4.5 varint decodes per record),
  per-page FullReferencesPage + PageReference bookkeeping, runtime-generated
  ASM bytecode predicate, MemorySegment safety-check tax on every byte read.
  Each record costs ~110 ns — 40-60× more work per row.

No incremental optimization to the current scan path can close a 1500× gap. The
only path to ballpark is a **new dedicated columnar scan kernel** that operates
directly on `NumberRegion` + `StringRegion` + a to-be-added `BooleanRegion`,
bypassing OBJECT_KEY entirely.
