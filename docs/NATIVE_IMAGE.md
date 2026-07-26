# sirix-bench native image (GraalVM)

The `:sirix-query:nativeCompile` task builds a Graal native image for
`io.sirix.query.bench.ScaleBenchMain` → `build/native/nativeCompile/sirix-bench`.

## Build modes

- `./gradlew :sirix-query:nativeCompile`
  default `quickBuild = true` (good enough for iterative dev).
- `./gradlew :sirix-query:nativeCompile -Pquick-build=false`
  full `-O3 -march=native` build. On this codebase/host (20 cores, GraalVM 25.0.3)
  both modes finish in ~2 min — the compile phase is only ~70-80 s because the
  reachable method count is modest; peak builder RSS ~6 GB.

The builder heap defaults to `-XX:MaxRAMPercentage=65` (coexists with a live
Gradle daemon on a 31 GB host). Pass `-Pnative.builderXmx=10g` to hard-cap it
when other agents/suites share the box.

Override the main class/image name to reuse this recipe for the write smoke:
`-Pnative.mainClass=io.sirix.query.bench.NativeWriteSmokeMain -Pnative.imageName=sirix-write-smoke`.

## PGO (profile-guided optimization)

```bash
./gradlew :sirix-query:nativeCompile -Ppgo-instrument
./build/native/nativeCompile/sirix-bench 1000000 true 5   # produces default.iprof
./gradlew :sirix-query:nativeCompile -Ppgo=default.iprof
```

## Arena blocker — RESOLVED (`io.sirix.io.SharedArenas`)

The write path is now native-clean. `MMStorage` (and the `ProjectionIndexHOTStorage`
scratch) used to map each generation into an `Arena.ofShared()` and `close()` it on
remap/teardown; closing a shared arena in a native image requires
`-H:+SharedArenaSupport`, which **GraalVM 25 cannot combine with the Vector API**.
`SharedArenas` routes all shared-access arena creation through a pluggable strategy:
`Arena.ofShared()` + explicit close on HotSpot (unchanged, deterministic unmap),
`Arena.ofAuto()` in a native image (same cross-thread access semantics, GC-reclaimed,
`close()` is a no-op). The full create/shred/commit/reopen/append-remap/time-travel
lifecycle passes in a native image — see `NativeWriteSmokeMain` (`:sirix-query:writeSmoke`
on the JVM, or build natively with `-Pnative.mainClass=io.sirix.query.bench.NativeWriteSmokeMain`).

### Why not `-H:+SharedArenaSupport` (GraalVM 25.0.3 matrix, reproduced)

| Build/run config (GraalVM 25.0.3, Vector API reachable) | Outcome |
|---|---|
| `-H:+SharedArenaSupport` **and** `-H:+VectorAPISupport` | build **rejected** up front: `Error: Support for Arena.ofShared is not available with Vector API support. Either disable Vector API support ... or replace usages of Arena.ofShared with Arena.ofAuto` |
| `-H:+SharedArenaSupport`, no `-H:+VectorAPISupport`, vector classes reachable | build **crashes** during `[6/8] Compiling`: `GraalError: ... AbstractLayout.varHandleInternal was not inlined and could access a session` at `SubstrateOptimizeSharedArenaAccessPhase.cleanupClusterNodes(:772)` (identical on 25.0.1 and 25.0.3) |
| `Arena.ofShared()` + `close()`, no `-H:+SharedArenaSupport` | builds; at **run time** the *close* throws `UnsupportedFeatureError: Support for Arena.ofShared is not active` — creation/mapping/cross-thread reads all succeed, only `close()` is gated |
| `Arena.ofShared()` **without** `close()`, no flag | works, but leaks the mapping every remap — rejected in favour of `Arena.ofAuto()` |
| **`Arena.ofAuto()`, no flag, `-H:+VectorAPISupport`** (current) | **works** — native write smoke passes (~50 ms), SIMD kernels keep AVX codegen |

The restriction is on shared-arena **close**, not creation; since the SIMD kernels are
non-negotiable for query speed we keep `-H:+VectorAPISupport` and drop the shared-arena
close instead (exactly what the builder's own error message recommends).

## Measured: native vs JVM (GraalVM 25.0.3, `-O3 -march=native`, no PGO)

Apples-to-apples: shred a 1 M-record DB **once**, then run the 9-query
`ScaleBenchMain` workload against that same on-disk DB on both runtimes
(`-Dsirix.db=<dir>`), so only query execution differs. Both use the columnar
`SirixVectorizedExecutor` over `jdk.incubator.vector` (AVX on both). Build:

```bash
# full -O3 -march=native (quickBuild=false); cap the builder heap on a shared box
./gradlew :sirix-query:nativeCompile -Pquick-build=false -Pnative.builderXmx=10g
DB=/tmp/sirix-perf-db
./bundles/sirix-query/build/native/nativeCompile/sirix-bench -Dsirix.shredDbPath=$DB 1000000 true 0   # shred once
perf stat -- ./bundles/sirix-query/build/native/nativeCompile/sirix-bench -Dsirix.db=$DB 1000000 true 30
```

### Warm steady-state (reuse DB, 30 iters) — native wins 7–17×

| query | JVM avg | native avg | factor |
|---|---|---|---|
| filterCount | 0.630 ms | **0.037 ms** | 17× |
| groupByDept | 0.323 ms | **0.067 ms** | 4.8× |
| sumAge | 0.300 ms | **0.028 ms** | 11× |
| avgAge | 0.191 ms | **0.029 ms** | 6.6× |
| minMaxAge | 0.262 ms | **0.049 ms** | 5.3× |
| groupBy2Keys | 0.282 ms | **0.096 ms** | 2.9× |
| filterGroupBy | 0.155 ms | **0.086 ms** | 1.8× |
| countDistinct | 0.092 ms | **0.060 ms** | 1.5× |
| compoundAndFilterCount | 0.128 ms | **0.065 ms** | 2.0× |

`perf stat` deltas (whole 30-iter run): native retires more of its work
(`tma_retiring` 71 % vs JVM 53 %), runs at higher IPC (3.18 vs 2.90 core),
and has a lower branch-miss rate (0.05 % vs 0.51 %) — no deopt guards, no
tiering, fully-hydrated data. This is the headline native query win and it
holds **even though predicate codegen falls back** (below).

### The runtime-codegen-fallback loss (the real native cost)

`SirixVectorizedExecutor.compileToClass()` JIT-emits a specialised
`BatchPredicate` class per distinct predicate via
`MethodHandles.Lookup.defineHiddenClass`. **A native image cannot define
classes at runtime**, so the first call of every distinct predicate throws

```
UnsupportedFeatureError: Classes cannot be defined at runtime by default
when using ahead-of-time Native Image compilation.
Tried to define class 'io/sirix/query/scan/SirixBatchPred$1'
```

and the executor falls back to the **interpreted** op-array predicate
(`evalCompiledBatch`). Correctness is identical, and *warm* the interpreter is
actually faster than the JVM's compiled predicate (above). But on a **true cold
first query** (`-Dsirix.noWarmup=true`, iter 1) the interpreted full-scan over
freshly page-faulted mmap data, with no JIT to amortise, is far slower than the
JVM:

| query (cold iter-1, no warmup) | JVM | native | note |
|---|---|---|---|
| filterCount (first predicate query) | 11.9 s | **43 s** | cold mmap hydrate + interpreted predicate |
| groupByDept | 1.48 s | 13.6 s | |
| sumAge | 0.29 s | 4.7 s | |
| avgAge / minMaxAge (pure aggregate, no predicate codegen) | ~1–2 ms | **~0.1 ms** | native already optimal — no fallback on this path |

So the earlier "cold iter-1 → 0.22 ms, 4000×" claim only held with a covering
**projection index** (`-Dprojection=true`, the `ProjectionIndexByteScan` path),
which sidesteps predicate codegen entirely — not the default generic predicate
path measured here. **Cheap mitigation applied:** `COMPILED_PREDICATE_ENABLED`
now defaults off in a native image (`SirixVectorizedExecutor`, gated on the
`org.graalvm.nativeimage.imagecode` property), so we skip the doomed classfile
build + throw/catch on the first call of each predicate and the noisy stderr
dump; the result is unchanged. The real fixes are larger and out of scope here:
emit the predicate variants at **build time** into static fields
(`--initialize-at-build-time`), or always use the projection-index scan for
predicate-bearing queries in native images.

### Ingest regression (unchanged): native shred ~8× slower

Native shred measured here: **25.4 K rec/s** (1 M records), vs JVM **~200 K
rec/s**. Profile (prior `perf stat`, 200 K records) showed IPC 3.74 but
`CPUs utilized = 1.72 / 20`: the Gson `JsonReader` tokenizer is effectively
single-threaded and native-image's per-thread tokenizer throughput is ~10×
below HotSpot tiered. Not compute-bound — it's serialization + single-thread.

**Pragmatic split: ingest on the JVM, query on native** — both share the
on-disk V0 format. A JVM-shredded DB queried natively hits the same warm
numbers above. Future levers: an AOT-friendly JSON parser (fastjson2 / a
record-shape-specific parser / simdjson) and parallel shred partitioning.
(**This split is largely obsolete on the GraalVM 25.1 line — see the update
below.**)

### Update — GraalVM 25.1-dev (EA): MemorySegment intrinsification closes most of the ingest gap

GraalVM commit `8edcbb77` ("Intrinsify MemorySegment.get/set before analysis",
2026-03-06) makes native-image intrinsify the scalar `MemorySegment` accessors
that HotSpot's JIT already intrinsifies. It is **not in any stable release**; it
first appears in the Oracle GraalVM **25.1-dev** EA line (verified here in
`graalvm-jdk-25e1-25.0.3-ea.32`, 2026-06-16). A standalone scalar get/set
microbench goes **4466 ms → ~75 ms native (≈56×)** on it — native is now ~2× the
JVM instead of ~100×.

Re-measured on this codebase/host, same binary recipe (`-O3 -march=native`),
1 M records:

| ingest (shred 1 M) | rec/s | vs JVM |
|---|---|---|
| JVM (GraalVM 25.1-dev) | 110 K | 1.0× |
| **native, GraalVM 25.1-dev `-O3`** | **~90 K** | **~1.2× slower** |
| native, GraalVM 25.0.3 `-O3` | ~23 K | ~4.8× slower |

The native ingest penalty drops from **~4.8× → ~1.2×** (near parity). That a
*MemorySegment*-specific fix alone buys ~4× indicates the un-intrinsified scalar
accessor in the page-serialization **write path** was a substantial part of the
native ingest cost — not only the single-threaded Gson tokenizer the earlier
profile flagged. On the 25.1 line a single native binary can ingest *and* query
with only a ~20 % ingest tax (was ~5×).

The warm analytical kernels above are **unchanged** — they are already
AVX-vectorized (Vector API) and never touched the slow scalar accessor.
**PGO did not help**: ingest stayed flat and the sub-ms query kernels *regressed*
(e.g. `filterCount` 0.053 ms `-O3` → 0.165 ms PGO) because the instrumented
profile is dominated by the ~28 s shred and mis-weights the microsecond kernels;
plain `-O3` is the better build here.

Caveat: 25.1-dev is a **pre-release EA build** — treat these as a preview until
the intrinsification ships in a stable GraalVM.
