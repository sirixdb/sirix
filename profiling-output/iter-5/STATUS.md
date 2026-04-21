# iter-5 Umbra-ballpark campaign — status

Branch: `perf/umbra-ballpark-iter-5` (pushed to origin, 16 commits)

## Where each scan path stands vs CedarDB on 10M

Numbers are **cold iter-1 max** (honest, `-Dsirix.noWarmup=true`):

| query | Sirix JVM | Sirix native | CedarDB | verdict |
|---|---:|---:|---:|---|
| sumAge | 2.9 ms | **0.43 ms** | 1.6 ms | 🟢 native beats CedarDB by 3.7× |
| avgAge | 1.6 ms | **0.51 ms** | 2.0 ms | 🟢 native beats by 3.9× |
| minMaxAge | 2.7 ms | **0.60 ms** | 1.7 ms | 🟢 native beats by 2.8× |
| countDistinct | 1.3 ms | **0.36 ms** | 3.7 ms | 🟢 native beats by 10× |
| groupBy2Keys | 164 ms | **0.06 ms** | 11.7 ms | 🟢 native beats by 195× |
| groupByDept | 327 ms | 1293 ms | 9.5 ms | 🔴 native cold regression; JVM behind 34× |
| filterCount | 3706 ms | 29033 ms | 0.6 ms | 🔴 both behind; native way worse |
| filterGroupBy | 1394 ms | 7559 ms | 2.8 ms | 🔴 same |
| compoundAndFilter | 1483 ms | 6485 ms | 0.65 ms | 🔴 same |
| filterGroupByAge | 668 ms | — (not in ScaleBenchMain) | — | — |

🟢 **Already in Umbra ballpark** (or past): all queries using `NumberRegionSimd` or `StringRegion` direct paths — 5 of 9. These are purely columnar; no OBJECT_KEY traversal.

🔴 **Gap remaining**: the 4 filter queries. Root cause is `collectColumns` OBJECT_KEY slot-traversal (varint decode + MemorySegment reads per record × per field). On JVM it JIT-inlines after warmup; on native AOT it doesn't.

## What's committed on iter-5 (16 commits, summary of wins)

| commit | focus | win size |
|---|---|---|
| `ab6f92ed5` | drop dead `AtomicInteger guardCount` from PageReference | ~9 GB GC pressure on 10M |
| `d3dd126c4` | halve `parentKeyToRow` clear cost | ~1% CPU |
| `d983f4ee2` | bulk-decode OBJECT_KEY columns pass 1 | 5-7% on filter queries |
| `30763abf1` | `Object2LongOpenHashMap` group-by accumulator | 14-17% on groupBy |
| `bdfe1e48b` | stage FSST source bytes to thread-local byte[] | n/a this workload |
| `0d71f9677` | canonical-String dedup for batch column reads | 42% on groupByDept |
| `8f8eed79a` | fast-path `fixupPageReferenceIds` for FullReferencesPage | ~1% |
| `4ef6ab73f` | bulk-count `StringRegion` dict-ids with word cache | 39% on groupByDept |
| `ead11fd24` | bulk-decode sibling parent+firstChild pass 2 | 5% on filter |
| `f627a6a59` | hoist OBJECT_KEY kindId probe out of bulk-decode | small |
| `3f85bfbcd` | CSV export harness + SQL suite for cross-engine bench | infrastructure |
| `1e5946db6` | wire zone-map pruning into parallelGenericPredicateCount | 0 on uniform data, real on selective predicates |
| `2386a1e01` | lock in DuckDB + CedarDB comparison numbers | documentation |
| `93b1e85d0` | honour `-Dsirix.noWarmup=true` in ScaleBenchMain | measurement fix |
| `db935804b` | honest native vs CedarDB doc | documentation |
| `c454fa91a` | shape-specialised single-pass interpreter | 0 on cold filter (wrong lever) |

## What CAN'T be closed incrementally

The remaining filter-query gap is **structural**. Every row costs ~110 ns on Sirix JVM / ~3000 ns on Sirix native cold / ~3 ns on CedarDB. The delta comes from:

1. **OBJECT_KEY slot indirection** — `collectColumns` reads OBJECT_KEY record bytes, decodes `parentKey` + `firstChildKey` as varints, resolves the first-child slot, reads the OBJECT_NUMBER_VALUE payload byte, decodes the value as varint. That's ~5 MemorySegment reads + 3 varint loops per field per record.
2. **Runtime-generated BatchPredicate** — native-image rejects it, falls back to interpreter, dropping ~20× JIT speedup compared to JVM.

## Path to ballpark on filter queries (multi-day structural work)

Tasks #48 + #49 (queued in this session's task list):

1. **BooleanRegion** (task #49) — mirror NumberRegion/StringRegion for boolean-valued columns. `~3 hours`.
2. **`recordContainerKey` column** on all three regions — each region entry gains a parallel `long[]` aligned with the values, delta-varint-packed against an anchor, storing the record-container nodeKey (OBJECT for `[{…}]`, ARRAY for `[[…]]`). Write-path walks up from value → OBJECT_KEY → record. `~3 hours`.
3. **`ColumnarScanExecutor`** (task #48) — new code path, triggered when the predicate is purely conjunctive over scalar fields. Scans each region's `recordContainerKey[]` in sorted order, merge-joins into a row bitmap, applies per-column SIMD predicates. `~6-8 hours`.

Total: ~2 days focused work. Expected end-state: cold filterCount in the 1-5 ms range on 10M (CedarDB is 0.6 ms — within 2-10×).

## Comparison artifacts for future rerun

- CSV data at `/tmp/records-10m.csv` (251 MB, 10M records with Random(42) seed).
- Parquet+Zstd version at `/tmp/records-10m.parquet` (26 MB).
- DuckDB CLI: `~/.duckdb/cli/latest/duckdb`.
- CedarDB Docker: `cedardb/cedardb` image, started via
  `docker run -d --name cedardb-bench -p 5432:5432 -e CEDAR_PASSWORD=postgres -v /tmp:/mnt/host-tmp cedardb/cedardb`.
- SQL query suite: `bundles/sirix-benchmarks/src/jmh/resources/scale-bench-queries.sql`.
- Pre-shredded Sirix DBs: `/tmp/sirix-scale-bench10951065897153496722` (10M), `/tmp/sirix-scale-bench18021862565405880578` (100M).
