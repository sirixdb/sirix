# Graal JIT: repeated `COMPILE SKIPPED: Speculation failed` on a bitmap-scan hot method — 27% cold-wall regression vs C2 on real workload

## Environment

```
$ java -version
java version "25.0.2" 2026-01-20 LTS
Java(TM) SE Runtime Environment Oracle GraalVM 25.0.2+10.1 (build 25.0.2+10-LTS-jvmci-b01)
Java HotSpot(TM) 64-Bit Server VM Oracle GraalVM 25.0.2+10.1 (build 25.0.2+10-LTS-jvmci-b01, mixed mode, sharing)
```

```
$ uname -a
Linux luna 6.8.0-107-generic #107-Ubuntu SMP PREEMPT_DYNAMIC Fri Mar 13 19:51:50 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux
```

- Arch: `x86_64` (Intel i7-12700H, AVX2 + BMI2)
- JIT: JVMCI + libgraal (default on Oracle GraalVM 25), i.e. `UseJVMCICompiler=true`, `UseJVMCINativeLibrary=true`
- No custom compiler flags — stock defaults for the Graal config; `-XX:-UseJVMCICompiler` for the C2 comparison

## Summary

On one method in an analytical-query hot path, Graal repeatedly fails its own speculations at Tier-4 and emits `COMPILE SKIPPED: Speculation failed` three times across a 9-query-with-3-iter-warmup run. Each skip leaves the method running from a lower-tier compile until the next Tier-4 attempt, which also eventually invalidates. C2 on the same hardware, same binary, same workload, hits **zero** speculation-failed skips and settles after one bounded deopt cycle. Net effect on our cold wall: Graal 4.78 s median vs C2 3.47 s median across 5 verified-cold runs each (**−1.31 s, −27 %**). The first-call tax on the worst-hit query goes from 1013 ms (Graal) to 235 ms (C2).

## Reproducer

- Repo: https://github.com/sirixdb/sirix (public)
- Branch: `perf/umbra-ballpark-iter`, commit `aaa8d445357c360f7133f9af0ae1c411abbc4340`
- Method: `io.sirix.index.projection.ProjectionIndexByteScan.conjunctiveCountByGroup`
  (source: `bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionIndexByteScan.java`, ~line 191)
- Workload: analytical bench over a 100 M-row shredded JSON dataset (`chicago.json` subset), 9 query shapes, 3-iter warmup per query, cold DB + OS page cache evicted per run
- Run harnesses (internal to our tree, but the flag is the whole reproducer):
  - Graal: default, no extra flag
  - C2: `-XX:-UseJVMCICompiler`

Happy to provide a slimmed-down reproducer if you can't trigger on the in-tree bench. The shape we believe is responsible is:

```java
public static void conjunctiveCountByGroup(
        Iterable<byte[]> leafPayloads,
        ColumnPredicate[] predicates,            // often length 0, sometimes length 1 across callers
        int groupColumn,
        Object2LongOpenHashMap<String> out) {
    for (byte[] payload : leafPayloads) {        // outer OSR'd loop (bytecode index 420 in our build)
        int rowCount = evaluateLeafMask(payload, predicates, /*...*/);  // builds the 64-bit bitmap
        if (rowCount <= 0) continue;             // fully-empty-leaf branch — first-leaf frequency is call-site dependent
        // ... per-row bitmap scan (for-w over 64-bit words, while(word!=0) popcount) ...
        // ... dict-decode per matching row, out.addTo(gv, 1L) ...
    }
}
```

Inside `evaluateLeafMask`, an inner `for (var p : predicates)` runs zone-map pruning and then a second `for (var p : predicates)` builds the conjunctive mask. `predicates.length == 0` and `predicates.length == 1` are both reachable from different callers in the same run.

## Observation (verbatim log evidence)

All grep results below are from `/tmp/claude/iter06/{graal,c2}-printcompile.log`, captured with `-XX:+PrintCompilation -XX:+PrintInlining` on the same JVM binary.

### Graal — three `COMPILE SKIPPED: Speculation failed` events

```
4320:   2908  3416 %     4        io.sirix.index.projection.ProjectionIndexByteScan::conjunctiveCountByGroup @ 304 (594 bytes)0.191   COMPILE SKIPPED: Speculation failed: UnreachedCode@19[HotSpotMethod<ProjectionIndexByteScan.conjunctiveCountByGroup(Iterable, ProjectionIndexScan$ColumnPredicate[], int, Object2LongOpenHashMap)>, 593]
4449:3350 3660       4       io.sirix.index.projection.ProjectionIndexByteScan::conjunctiveCountByGroup (594 bytes)   COMPILE SKIPPED: Speculation failed: GuardMovement@11[HotSpotMethod<ProjectionIndexByteScan.conjunctiveCountByGroup(Iterable, ProjectionIndexScan$ColumnPredicate[], int, Object2LongOpenHashMap)>, 103, NullCheckException]
4455:3990 3663       4       io.sirix.index.projection.ProjectionIndexByteScan::conjunctiveCountByGroup (594 bytes)   COMPILE SKIPPED: Speculation failed: GuardMovement@11[HotSpotMethod<ProjectionIndexByteScan.conjunctiveCountByGroup(Iterable, ProjectionIndexScan$ColumnPredicate[], int, Object2LongOpenHashMap)>, 406, ClassCastException]
```

Three distinct `SpeculationReason` kinds, on three different bci:

| # | SpeculationReason | bci | kind |
|---|---|---:|---|
| 1 | `UnreachedCode@19` | 593 | UnreachedCode (speculated branch was unreached; then reached) |
| 2 | `GuardMovement@11` | 103 | NullCheckException |
| 3 | `GuardMovement@11` | 406 | ClassCastException |

The bci-406 event is particularly load-bearing — bci 406 is the head of the outer `for (byte[] payload : leafPayloads)` loop where the iterator is pulled and cast from `Iterable<byte[]>`. The call site is only ever invoked with a concrete `ArrayList<byte[]>` or streaming iterable over `byte[]`, so we didn't expect a `ClassCastException` guard motion at all.

### Graal — supporting invalidations on the same method

```
4160:2213 3409 %     3       conjunctiveCountByGroup @ 420   made not entrant: OSR invalidation of lower level
4161:2227 3411 %     4       conjunctiveCountByGroup @ 420   made not entrant: uncommon trap
4165:2750 3413 %     3       conjunctiveCountByGroup @ 420   made not entrant: OSR invalidation of lower level
4167:2818 3415 %     4       conjunctiveCountByGroup @ 420   made not entrant: uncommon trap
4448:3222 3657 %     4       conjunctiveCountByGroup @ 420   made not entrant: uncommon trap
4452:3690 3661 %     3       conjunctiveCountByGroup @ 420   made not entrant: OSR invalidation of lower level
4454:3704 3662 %     4       conjunctiveCountByGroup @ 420   made not entrant: uncommon trap
```

Counts on `conjunctiveCountByGroup` across the Graal run:
- `COMPILE SKIPPED: Speculation failed`: **3**
- `made not entrant: uncommon trap`: **4**
- `made not entrant: OSR invalidation of lower level`: **3**

### C2 — same method, same workload, same log

```
4139:1864 3391 %     3       conjunctiveCountByGroup @ 420   made not entrant: OSR invalidation of lower level
4140:1939 3394 %     4       conjunctiveCountByGroup @ 420   made not entrant: uncommon trap
4434:2315 3392       3       conjunctiveCountByGroup (594 bytes)   made not entrant: not used
```

Counts on `conjunctiveCountByGroup` across the C2 run:
- `COMPILE SKIPPED: Speculation failed`: **0**
- `made not entrant: uncommon trap`: **1**
- `made not entrant: OSR invalidation of lower level`: **1**
- `made not entrant: not used`: **1**

### Wall-clock impact

Cold 100 M-row analytical bench, 5 verified-cold runs (DB reopen + OS cache evict per run), each run is 9 queries × 3 warmup iters:

| config | run times (s) | median |
|---|---|---:|
| Graal (default) | 4.47, 4.67, 4.78, 5.22, 5.45 | **4.78** |
| C2 (`-XX:-UseJVMCICompiler`) | 3.21, 3.40, 3.47, 3.48, 3.56 | **3.47** |

**Δ = −1.31 s median, −27 %.**

First-call (no-warmup) per-query, same binary — the two queries whose hot path is `conjunctiveCountByGroup`:

| query | Graal first-call | C2 first-call |
|---|---:|---:|
| `groupByDept` | 1013 ms | **235 ms** (4.3× faster under C2) |
| `filterGroupBy` | 1363 ms | **216 ms** (6.3× faster under C2) |

The combined `groupByDept + filterGroupBy` first-call tax drops from **2376 ms (Graal)** to **451 ms (C2)**.

CPU profile (3-iter bench, async-profiler, same method):

| frame | Graal % | C2 % |
|---|---:|---:|
| `ProjectionIndexByteScan.conjunctiveCountByGroup` | 32.1 % | **8.4 %** |
| `VarHandle.checkAccessModeThenIsDirect` | 7.9 % | 4.5 % |
| `VarHandleByteArrayAsInts.get` | 6.4 % | 1.9 % |

## Expected behaviour

After a bounded number of recompilations (C2 converges in 1 OSR-invalidation + 1 uncommon-trap here), Graal should either (a) settle on a generally-correct compile for `conjunctiveCountByGroup` and stop burning time on speculation misses, or (b) at minimum match C2's wall time on this workload shape.

## Actual behaviour

Graal hits three successive `COMPILE SKIPPED: Speculation failed` events spanning the entire 9-query-with-3-iter-warmup window. The method never reaches a steady Tier-4 compile within the bench's warm-up budget. Each speculation miss invalidates the Tier-4 code and re-enters Tier-3 (level-3 re-profile), and the next Tier-4 attempt picks up a **different** `SpeculationReason` (`UnreachedCode@19` → `GuardMovement@11/NullCheckException` → `GuardMovement@11/ClassCastException`), i.e. a new class of guard each time. On a long-running workload this would eventually converge, but in practice end-users hit the cold tail every time the JVM starts up.

## Likely cause (speculative — just a theory)

`conjunctiveCountByGroup` is called from several sites in the same run. The dominant caller-shape is `predicates.length == 0` (pure count). A minority caller-shape is `predicates.length == 1` (numeric range filter). We suspect Graal's type profile records the all-empty-predicates shape first, speculates on it (UnreachedCode at the `for (var p : predicates)` loop body), then the non-empty shape trips the guard. C2's uncommon-trap model appears to tolerate the branch-flip without a compile-skip.

The `GuardMovement@11, ClassCastException` at bci 406 is the most surprising — that bci is the `Iterable<byte[]>` iterator cast at the outer loop head. C2 does not insert a movable cast guard there; Graal does, and that guard later fails.

## Workaround

`-XX:-UseJVMCICompiler` (fall back to stock HotSpot C2) fully eliminates the regression in our workload:

```
$JAVA_HOME/bin/java -XX:-UseJVMCICompiler -jar bench.jar   # 3.47 s cold
$JAVA_HOME/bin/java                       -jar bench.jar   # 4.78 s cold (default Graal)
```

We've landed this as the bench JVM default. Full project test suite (9506 tests) passes with the flag set — we don't see any functional regression from disabling Graal JIT on our code.

## Attachments available on request

- `graal-printcompile.log` (~400 kB, `-XX:+PrintCompilation -XX:+PrintInlining` full trace)
- `c2-printcompile.log` (~400 kB, same flags, same binary, different `UseJVMCICompiler` value)
- `graal-cpu.collapsed` / `c2-cpu.collapsed` (async-profiler collapsed stacks, 3-iter bench)
- Bench run logs for all 5+5 cold runs

Happy to open these if the speculation-reason strings + bci are not enough to localise.

---

## Prior filed issue for context

oracle/graal#13377 — separate, unrelated: MemorySegment intrinsic gap in native-image. Filed 2026-03.

---

## Paste-ready submission

```
gh issue create --repo oracle/graal \
  --title "Graal JIT: repeated COMPILE SKIPPED: Speculation failed on bitmap-scan hot method — 27% cold-wall regression vs C2 on real workload" \
  --body-file profiling-output/graal-speculation-failed-bug.md \
  --label performance
```
