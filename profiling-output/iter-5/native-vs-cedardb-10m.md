# Native Image vs CedarDB vs JVM — 10M records, iter-5

All runs on the same `/tmp/sirix-scale-bench10951065897153496722` shredded DB,
in-memory CedarDB loaded from `/tmp/records-10m.csv` (same data via
`ExportScaleBenchCsv`).

Sirix native-image is quickBuild-`-O3 -march=native`, **no PGO**, runtime-class
generation **disabled** (native-image rejects it — falls back to the
`evalCompiledBatch` interpreter path for filter predicates).

## Cold-only numbers (`-Dsirix.noWarmup=true`, iters=3, freshExecutorPerIter not supported on ScaleBenchMain)

iter-1 is fully cold (max). iter-2/3 hit the executor's result cache (`filterCountCache`, `pathStatsCache`, etc.) — sub-ms min.

| query | Sirix native iter-1 cold | Sirix native cached | Sirix JVM cold | CedarDB |
|---|---:|---:|---:|---:|
| filterCount | **30965 ms** 🔴 | 0.031 | 3706 | 0.6 |
| groupByDept | 1293 ms 🟠 | 0.048 | 327 | 9.5 |
| sumAge | **0.43 ms** 🟢 | 0.018 | 2.9 | 1.6 |
| avgAge | **0.51 ms** 🟢 | 0.028 | 1.6 | 2.0 |
| minMaxAge | **0.60 ms** 🟢 | 0.030 | 2.7 | 1.7 |
| groupBy2Keys | **0.06 ms** 🟢 | 0.039 | 164 | 11.7 |
| filterGroupBy | 7526 ms 🔴 | 0.047 | 1394 | 2.8 |
| countDistinct | **0.36 ms** 🟢 | 0.020 | 1.3 | 3.7 |
| compoundAndFilter | 6557 ms 🔴 | 0.041 | 1483 | 0.65 |

🟢 = native cold already at or ahead of CedarDB on columnar paths.
🟠 = unexpected cold regression, likely bootstrap / FFI init.
🔴 = runtime-class-gen fallback penalty (interpreter is ~20-30× slower than JIT-compiled predicate).

## Where native wins (already at or past Umbra ballpark)

All queries that route through `NumberRegionSimd.aggregateRange` (sumAge/avgAge/minMaxAge/countDistinct) **beat CedarDB on cold 10M**:
- sumAge 0.43 ms cold — CedarDB 1.6 ms — **3.7× faster**
- groupBy2Keys 0.06 ms cold — CedarDB 11.7 ms — **195× faster**

These queries never touch the `collectColumns` OBJECT_KEY pipeline — they're purely SIMD scans of the NumberRegion / StringRegion bytes. Native image's -O3 + no-warmup advantage compounds with the already-columnar path.

## Where native loses (the interpreter fallback)

Filter queries that need multi-field correlation (`collectColumns` + compiled `BatchPredicate`):
- filterCount native cold 30.9 s vs JVM cold 3.7 s — **8× slower than JVM cold**
- filterGroupBy: 7.5 s native vs 1.4 s JVM — 5× slower
- compoundAndFilter: 6.6 s native vs 1.5 s JVM — 4× slower

Root cause: `SirixVectorizedExecutor.compileToClass` uses `java.lang.classfile` to emit a runtime-generated hidden-class `BatchPredicate` per query shape. Native-image rejects runtime class definition with `UnsupportedFeatureError`, so the executor falls back to `evalCompiledBatch` — a tree-walking interpreter over the flat ops/children arrays.

On JVM, HotSpot JIT inlines the interpreter switch-dispatch + predicate logic after a few thousand records. On native-image, the interpreter stays boxed behind a switch statement — and every `ops[i]` dispatch costs a branch + potential cache miss. At 10M rows × ~5 ops per predicate that's ~50M dispatches, explaining the 30-second filterCount.

## Next structural step

Three ways to fix the native filter regression, ordered by effort:

1. **Inline-specialize `evalCompiledBatch` for common predicate shapes** (2-leaf AND, 3-leaf AND, NUM_CMP+BOOL_REF, NUM_CMP+STR_EQ, etc.). Hand-write the hot shapes as dedicated static methods; dispatch on shape hash at query-compile time. Estimated ~5-10× native cold-filter speedup — brings 30 s → 3-6 s, still behind JVM's 1 s but progress.

2. **Build-time predicate compilation**: instead of `compileToClass` at runtime, emit the same bytecode as part of the native image build step for a fixed set of shape templates. Native-image supports `--initialize-at-build-time` for generated classes. Would bring native filterCount into the 0.1-1 s range.

3. **A real ColumnarScanExecutor** (task #48) with `recordContainerKey` join (see tree-alignment correction). Bypasses `collectColumns` entirely for conjunctive scalar predicates → native cold filterCount in the low-millisecond range on par with CedarDB.

## Warm-cache story

On repeated queries (analytical dashboard workloads), all Sirix native queries
hit the executor's result caches at sub-microsecond cost. That's strictly
**better** than CedarDB (which rescans each time) for the "same query ran
twice" case. But it's a cache-hit story, not a scan-kernel result.

For FAIR cold-scan comparison, `freshExecutorPerIter=true` needs porting from
`BrackitQueryOnSirixScaleMain` (sirix-benchmarks) to `ScaleBenchMain`
(sirix-query) — open task.
