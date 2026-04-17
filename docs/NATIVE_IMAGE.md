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
