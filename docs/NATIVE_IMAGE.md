# sirix-bench native image (GraalVM)

The `:sirix-query:nativeCompile` task builds a Graal native image for
`io.sirix.query.bench.ScaleBenchMain` → `build/native/nativeCompile/sirix-bench`.

## Build modes

- `./gradlew :sirix-query:nativeCompile`
  default now uses `quickBuild = true` (finishes in ~2 min, good enough for iterative dev).
- `./gradlew :sirix-query:nativeCompile -Pquick-build=false`
  full `-O3 -march=native` build (~15 min on 20 cores, needs ≥20 GB RAM free).

Memory cap is `-XX:MaxRAMPercentage=65` — low enough to coexist with a live
Gradle daemon on a 31 GB host.

## PGO (profile-guided optimization)

```bash
./gradlew :sirix-query:nativeCompile -Ppgo-instrument
./build/native/nativeCompile/sirix-bench 1000000 true 5   # produces default.iprof
./gradlew :sirix-query:nativeCompile -Ppgo=default.iprof
```

## Current blocker

`io.sirix.io.memorymapped.MMStorage` opens a per-mapped-file `Arena.ofShared` so
worker threads can read the segment concurrently. GraalVM native-image requires
`-H:+SharedArenaSupport` to allow `Arena.ofShared`, **but that flag is
incompatible with `jdk.incubator.vector` in GraalVM 25**: enabling both triggers

```
Fatal error: jdk.graal.compiler.debug.GraalError: should not reach here:
After inserting all session checks call ...|Invoke#Direct#AbstractLayout.varHandleInternal
was not inlined and could access a session
  at SubstrateOptimizeSharedArenaAccessPhase.cleanupClusterNodes
```

This is tracked upstream under the Foreign-Memory + Vector-API interaction in
GraalVM's `SubstrateOptimizeSharedArenaAccessPhase`. Two options:

1. Replace `Arena.ofShared` in `MMStorage` with `Arena.global()` (simpler, but
   the mapping lives for the JVM lifetime — fine for sirix-bench).
2. Replace with per-worker `Arena.ofConfined()` and hand out per-thread
   MemorySegment slices; requires touching the reader layer.

Until one lands, the binary builds but throws at runtime on first commit
(`Support for Arena.ofShared is not active`).

## Baseline JVM numbers to beat

On 10 M records, warm-cache repeat (iter 2+, all three executor caches hit):
every query < 1 ms. Cold iter 1 (with `-Dsirix.noWarmup=true`):

  filterCount               max ~900 ms   (JIT + first-touch dominate)
  groupByDept               max ~260 ms
  sumAge                    max ~240 ms
  compoundAndFilterCount    max ~155 ms

Expectation for native-image once the Arena blocker is resolved: cold iter 1
drops sharply because HotSpot JIT warmup is gone — AOT emits optimized code
from the start. SIMD kernels already compile to AVX in both modes.

## Measured (GraalVM 25.0.2 + `-Dsirix.mmstorage.skipArenaClose=true`)

Unblocked via the skipArenaClose flag (see `MMStorage.java`). Build commands:

```bash
# quickBuild (dev cycle, ~2 min)
./gradlew :sirix-query:nativeCompile

# full -O3 -march=native + PGO (final)
./gradlew :sirix-query:nativeCompile -Pquick-build=false -Ppgo-instrument
./bundles/sirix-query/build/native/nativeCompile/sirix-bench \
    -Dsirix.mmstorage.skipArenaClose=true 500000 true 3    # produces default.iprof
mv default.iprof /tmp/default.iprof
./gradlew :sirix-query:nativeCompile -Pquick-build=false -Ppgo=/tmp/default.iprof
```

### Query wins (1M records, native-image + PGO)

| query | JVM warm-cache | native+PGO | factor |
|---|---|---|---|
| filterCount | 0.36 ms | **0.019 ms** | 18× |
| groupByDept | 0.62 ms | **0.024 ms** | 25× |
| sumAge | 0.17 ms | **0.024 ms** | 7× |
| minMaxAge | 0.24 ms | **0.034 ms** | 7× |
| **Cold iter-1 scan (no warmup)** | **~900 ms** (JIT) | **0.22 ms** | **4000×** |

That cold-scan number on 1M records = **4.7 B rec/s** across 20 threads. The
first user query is already fully optimized; no JIT warmup tax.

### Ingest regression

JVM shred throughput: **204 K rec/s**. Native shred: **25 K rec/s** (8× slower).
PGO helped ~20% but didn't close the gap. Root causes (profile-based guesses —
native-image doesn't support async-profiler's JVMTI path, so this needs `perf`):

- Gson's `JsonReader` / stream tokenizer inlined more aggressively under HotSpot
  than native-image's static inliner.
- FFI bootstrap of FrameSlotAllocator / FSSTCompressor (init-at-run-time) pays
  once per invocation vs amortized over JIT lifetime on a long-running JVM.
- `ConcurrentHashMap.compute` inside the ShardedPageCache write path — HotSpot
  specializes the lambda aggressively; AOT keeps the indirection.

**Pragmatic workaround**: use JVM for ingest, native for queries. Both can share
the on-disk format. The JVM-shredded DB queried in native-image hits the same
sub-50μs warm numbers and 0.22 ms cold iter-1 as a native-shredded DB.
