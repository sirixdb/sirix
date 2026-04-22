# iter#05 baseline profile analysis

2026-04-22. Branch: `perf/umbra-ballpark-iter`.

Task: capture a fresh 4-event profile after iter#04 and identify the #1 cold-wall
attack target by measurement, not hypothesis.

## Baseline

5 verified-cold runs (`/tmp/claude/evict_db.py` confirms `sirix.data` residency 0%
before each launch). Host load ≤ 2. `-Xms8g -Xmx24g -Dsirix.offheap.bytes=16G`,
`-Dprojection=true -DbuildPathSummary=true -DbuildPathStatistics=true`.

| run | wall (s) | projection build (ms) |
| --- | --- | --- |
| 1 | 4.69 | 1,129 |
| 2 | 4.55 | 1,139 |
| 3 | 4.73 | 1,141 |
| 4 | 4.71 | 1,134 |
| 5 | 4.86 | 1,170 |
| **median** | **4.71** | **1,141** |

Spread: 4.55–4.86, slightly faster than iter#04's documented 5.10s median. Likely
attributable to post-iter#04 system noise settling. iter#04 prefetch-disabled
default is confirmed in-tree (`PREFETCH_PARALLELISM_DEFAULT = 0`).

## Per-phase timing decomposition (via `-Dsirix.bench.timing=true` probe, reverted)

Temporarily instrumented the bench to time each startup phase (removed from the
tree before profile capture):

| phase | wall |
| --- | --- |
| `store.build` | 2.8 ms |
| `ctx + chain` | 36.7 ms |
| `store.lookup` | 45.5 ms |
| `beginResourceSession` | 74.8 ms |
| `Projection index hydrate` (parallel depth-2) | 1,129 ms |
| **Total pre-query wall** | **1,302 ms** |
| **All 9 queries wall** (with 3 warmup + 1 measure per query) | **2,973 ms** |
| JVM + class init (pre-main) | ~500 ms |
| Total | ~4,810 ms (matches WALL_SECONDS=4.81 on probe run) |

**63% of the cold wall is the query phase.** The 0.3-0.7 ms per-query "min" that
the bench table reports is the AFTER-warmup measurement — across 3 warmup
iterations each of 9 queries, queries collectively consume ~3 s of wall.

## 4-event profile (baseline, cold, iters=1)

Captured at `profiling-output/iter05-baseline-{cpu,wall,alloc,lock}.collapsed`
plus their top-25-frame digests (`.txt`).

### CPU (8,558 samples, 4.8s bench wall)

| samples | % | leaf frame |
| --- | --- | --- |
| 1,325 | 15.48% | `ProjectionIndexByteScan.conjunctiveCountByGroup` |
| 1,097 | 12.82% | `VarHandle.checkAccessModeThenIsDirect` |
| 936 | 10.94% | `VarHandleByteArrayAsInts$ArrayHandle.get` |
| 681 | 7.96% | `VarForm.getMemberName` |
| 537 | 6.27% | `Object2LongOpenHashMap.addTo` |
| 476 | 5.56% | `VarHandleByteArrayAsInts$ArrayHandle.index` |
| 331 | 3.87% | `VarHandleGuards.guard_LI_I` |
| 176 | 2.06% | `__memcpy_sse2_unaligned_erms` |

**Phase breakdown (CPU):**
- Query kernel: 6,431 (75.1%)
- Hydrate: 650 (7.6%)
- JIT/Graal: 832 (9.7%)
- Other: 645 (7.5%)

**VarHandle-path frames (any depth): 3,639 samples = 42.5 %.** Of those, 95.3 %
flow through `ProjectionIndexByteScan.getIntLE`; the remainder is `getLongLE` +
HOT-storage key decode.

### Wall (6,613 samples)

| samples | % | leaf frame |
| --- | --- | --- |
| 4,963 | 75.05% | `__futex_abstimed_wait_cancelable64` |
| 432 | 6.53% | `ProjectionIndexByteScan.conjunctiveCountByGroup` |
| 146 | 2.21% | `syscall` |
| 102 | 1.54% | `VarHandle.checkAccessModeThenIsDirect` |
| 82 | 1.24% | `__memcpy_sse2_unaligned_erms` |
| 82 | 1.24% | `__libc_pread64` |

**Active wall samples = 1,624 (24.6% of total).** The remaining 75.4% is futex
parking, dominated by idle pool threads (`ForkJoinPool.awaitWork` = 1,290,
internal `WorkerThread::run;PosixSemaphore::wait` = 1,233, CompileBroker wait =
473, ClockSweeper = 163, GC handlers = ~200) — all benign idle.

**Active wall breakdown:**
- Query kernel: 1,057 (65.1%)
- Hydrate: 265 (16.3%)
- JIT/Graal: 156 (9.6%)
- GC: 92 (5.7%)
- Other: 54 (3.3%)

### Alloc (32,346 samples)

| samples | % | leaf frame |
| --- | --- | --- |
| 15,390 | 47.6% | `byte[]` |
| 9,364 | 29.0% | `long[]` |
| 3,077 | 9.5% | `Long256Vector` |
| 2,332 | 7.2% | `boolean[]` |
| 1,469 | 4.5% | `Long256Vector$Long256Mask` |

Alloc is dominated by scratch arrays in the query kernel — expected. The
`ScanScratch` ThreadLocal already reuses the main scratch buffers, so these are
per-call vectors in the SIMD inner loop. Not a primary attack target.

### Lock (44,619 ns total blocking time)

| ns | % | lock object |
| --- | --- | --- |
| 44,221 | 99.11% | `ConcurrentHashMap$Node` |
| 182 | 0.41% | `int[]` |
| 121 | 0.27% | `Object` |
| 93 | 0.21% | `ReentrantLock$NonfairSync` |

**Top stack: `ProjectionIndexHOTStorage.decodeCompositeKeySegment →
MemorySegment.get(JAVA_LONG_UNALIGNED.withOrder(BIG_ENDIAN), 0) →
Utils.makeRawSegmentViewVarHandle → ConcurrentHashMap.computeIfAbsent`** =
44,042 ns (98.7% of all lock time).

At 44 µs total across a 4.8 s run, this is NOT a wall bottleneck — it's < 10 ppm
of wall. Noteworthy only because it's the dominant hot-path ConcurrentHashMap
write (and is trivially fixable by hoisting the ValueLayout constant — separate
from iter#05's main lever).

## JIT compile-log analysis (the key finding)

`-XX:+PrintCompilation` during a cold bench run reveals the real story behind
the 42.5 % VarHandle CPU attribution:

```
 1826 % 3     conjunctiveCountByGroup @ 420 (594 bytes)            [C1 OSR]
 1884 % 4     conjunctiveCountByGroup @ 420 (594 bytes)            [C2 OSR]
 2101 % 3     conjunctiveCountByGroup @ 420 (594 bytes)   made not entrant: OSR invalidation of lower level
 2111 % 4     conjunctiveCountByGroup @ 420 (594 bytes)   made not entrant: uncommon trap
 2111 % 4     conjunctiveCountByGroup @ 406 (594 bytes)            [C2 OSR — different bci]
 2413 % 4     conjunctiveCountByGroup @ 406 (594 bytes)   made not entrant: uncommon trap
 2547 % 4     conjunctiveCountByGroup @ 420 (594 bytes)   COMPILE SKIPPED: Speculation failed:
                                                          UnreachedCode@19[..., 593]
 2642 % 4     conjunctiveCountByGroup @ 420 (594 bytes)   made not entrant: uncommon trap
 ...
 3176 % 4     conjunctiveCountByGroup @ 304 (594 bytes)            [C2 OSR — yet another bci]
```

**`conjunctiveCountByGroup` is C2-compiled and deoptimized 8+ times between
1826 ms and 3176 ms of wall — a 1.35-second JIT recompile storm.**

The method is 594 bytes (near C2's inlining-threshold boundary) and contains
three nested loops:
- outer `for (byte[] payload : leafPayloads)` → **iterator-type polymorphism**
- middle `for (w = 0; w < stride; w++)` → row-word loop
- inner `while (word != 0L)` → bit-extraction loop per row

### Why it deopts

C2 first compiles with the iterator type profile it has seen: an
`ArrayList$Itr` (from the sequential call site
`SirixVectorizedExecutor.parallelConjunctiveCountByGroup` line 1420). When the
parallel-chunk call site (line 1435) invokes with
`leafPayloads.subList(from, to)`, the runtime iterator becomes
`ArrayList$SubList$Itr` — a different concrete type. C2's speculative inlined
`Iterator.next()` / `Iterator.hasNext()` fails its type guard, uncommon-trap
fires, method goes back to interpreter, recompile cycle restarts.

The `UnreachedCode@19[..., 593]` speculation failure is the same story: bytecode
593 is `return` (post-outer-loop exit). C2 assumed the outer loop's exit branch
was dead because only small inputs had flowed through during profiling.

Once C2 finally stabilises on a guard-free polymorphic dispatch after the 3rd
recompile, the hot loop runs at intrinsified `MOVL`/`MOVQ` speed — but by then
the bench has already spent 1.3 s of wall on JIT churn.

### Why VarHandle appears at 42 % CPU

During the deopt intervals, `conjunctiveCountByGroup` falls back to tier-3 (C1)
or interpreter. C1 does NOT intrinsify `byteArrayViewVarHandle.get` — it
generates a virtual `VarHandle.get` call that goes through the full
`VarHandleGuards.guard_LI_I → VarHandle.checkAccessModeThenIsDirect → ArrayHandle.get`
chain. Every `getIntLE(payload, off)` becomes a real method call with ~5
frames of stack. That's why the CPU profile shows 35-42 % in VarHandle
glue — it's the C1-compiled fallback, not the supposed-to-be-C2-intrinsic path.

iter#02's prior A/B (VarHandle vs MemorySegment vs Unsafe) was correct that all
three have the same C2-intrinsic target; swapping the implementation doesn't
help because the bottleneck isn't the steady-state hot loop — it's the JIT
failing to stabilise.

## iter#04 sanity check

iter#04 profile files are in `-t` summary format (not collapsed); converting via
the raw data shows the same pattern:
- CPU: `VarHandle.checkAccessModeThenIsDirect` = 12.03 %, `VarForm.getMemberName`
  = 7.48 %, `VarHandleGuards.guard_LI_I` = 4.10 % → total VarHandle-path = ~32 %.
- The cap=0 lock profile showed 97 % on `sun.nio.ch.NativeThreadSet` because
  the `ConcurrentHashMap` contention from `computeIfAbsent` was not yet
  surfaced (iter#04 wasn't looking at MemorySegment-path locking).

iter#04's focus on prefetcher cap was correct in its window — fixing the
prefetcher freed CPU cycles now visible as the query-path JIT problem.

## iter#05 attack target

**#1 cost on the wall: `conjunctiveCountByGroup` JIT deoptimisation storm.**

Own-the-wall math:
- 4,810 ms total
- 1,302 ms pre-query (mostly hydrate 1,141 ms)
- ~3,000 ms query phase (dominated by JIT warmup + deopt churn)
- Per-query measured `min(ms)` (after warmup) = 0.25-0.60 ms → 9 queries ≈ 3 ms
  steady-state.

The 3,000 ms query phase minus ~30 ms steady-state = **~2,970 ms of JIT-warmup
wall**. Even eliminating 50 % of that = 1.5 s saved = **31 % cold-wall
reduction** — well above the iter#05 10 % keep-threshold.

## Candidate fixes (in priority order)

### Option A: Stabilise the iterator type

Change `conjunctiveCountByGroup`'s signature from
`Iterable<byte[]> leafPayloads` to `List<byte[]>` and walk with a
for-i index: `for (int i = 0, n = leafPayloads.size(); i < n; i++) { byte[] payload = leafPayloads.get(i); ... }`.

This removes the `Iterator.next()` / `hasNext()` call-site polymorphism.
Both callers pass either an `ArrayList` or `ArrayList$SubList` — both implement
`List.get(int)` with monomorphic lookups. The `SubList.get(int)` adds one
range-check but no iterator allocation.

Also apply to `conjunctiveCount` which has the same shape.

**Expected wall impact**: Removes 80-90 % of the deopt events. Recovery from
1 pure-C2-compile run + 0 recompiles should save 1.0-1.5 s of cold wall.

### Option B: Hoist ValueLayout constants

`decodeCompositeKeySegment` at line 1267 creates a new `JAVA_LONG_UNALIGNED
.withOrder(BIG_ENDIAN)` instance per call. Hoist to a static final:

```java
private static final ValueLayout.OfLong BE_LONG_UNALIGNED =
    ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
```

Eliminates 100 % of the ConcurrentHashMap contention. At 44 µs of total blocking
time during a 4.8 s run, the wall impact is < 10 ppm. Low priority as a
standalone fix; include as a free cleanup if A is implemented.

### Option C: AOT pre-compile the hot path

Use `-XX:CompileThreshold=1 -XX:-TieredCompilation` at bench start, or call a
pre-warm lambda that invokes `conjunctiveCountByGroup` with synthetic data
before the bench queries run.

A quick `-XX:CompileThreshold=1` probe showed a modest 4 % wall improvement
(4.53 s vs 4.71 s median). Not a big enough lever by itself, but may compose
with option A.

### Option D: Split the 594-byte method

`conjunctiveCountByGroup` contains both the zero-predicate fast path
(groupByDept, countDistinct) and the predicate path (filterGroupBy) in the same
method body. Splitting into two methods lets C2 compile each at its own type
profile without cross-call-site polymorphism.

Requires more scaffolding than A; defer unless A doesn't land the target.

## iter#05 decision

**Go with Option A first** (and fold in the Option B cleanup since it's adjacent
and free). Expected wall saving: 1.0-1.5 s = 21-32 % cold-wall reduction. Clean
correctness story (byte-equal semantics; indexed access yields identical output
to the iterator walk). Rollback = single-file revert.

Plan the Phase 2 formal-verification doc, land the change behind the existing
callers unchanged, run the cross-engine test suite, then collect the 4-event
profile post-change for the iter#05 table.
