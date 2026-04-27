# iter#05 — plan: de-polymorphise the conjunctive hot loops

2026-04-22. Branch: `perf/umbra-ballpark-iter`. Depends on the analysis in
`iter05-baseline-analysis.md`.

## Identified #1 cost

`ProjectionIndexByteScan.conjunctiveCountByGroup` (594 bytes) and
`ProjectionIndexByteScan.conjunctiveCount` (128 bytes) take `Iterable<byte[]>`
as their payload-list type. C2 first stabilises on `ArrayList$Itr` (from the
sequential caller `parallelConjunctiveCountByGroup` line 1420), then deopts when
the parallel call site (line 1435) passes `ArrayList$SubList$1` iterators.

The `-XX:+PrintCompilation` log shows ≥ 8 C2 (re)compiles of
`conjunctiveCountByGroup` between 1,826 ms and 3,176 ms of bench wall time —
a **1.35-second JIT-churn window** where the method cycles through
interpreter → C1 → C2 → uncommon-trap → back-to-interpreter → C1 → C2 → etc.

`java.util.ArrayList$SubList$1.next/hasNext` themselves deopt at 1,798 ms with
an "uncommon trap" flag (visible in the compile log). That is the same deopt
cascade spreading to the iterator's own compile units.

## Own-the-wall math

- 4,810 ms total cold wall (iter#05 baseline median).
- 1,302 ms pre-query (hydrate + DB open + class init).
- **~3,000 ms query phase**: dominated by warmup + deopt churn (per-query
  `min(ms)` after warmup sums to ~3 ms; warmup adds another ~2,970 ms).

Steady-state per-query cost (after JIT stabilises) ≈ 330 µs × 9 queries ×
4 invocations (3 warmup + 1 measure per iter=1) = **≈ 12 ms intrinsic query
work**. The remaining **~2,960 ms is JIT warmup + deopt**.

A 50 % reduction in query-phase warmup wall = **1.5 s saved = 31 % cold-wall
reduction** — well above iter#05's 10 % keep-threshold.

## Proposed attack: Option A (indexed walk)

Convert the outer `for-each Iterable<byte[]>` loop in `conjunctiveCount` and
`conjunctiveCountByGroup` to an indexed `List.get(i)` walk. Accept `List<byte[]>`
instead of `Iterable<byte[]>`. Both callers pass a `List<byte[]>`; tests pass
`ArrayList`/`Arrays.asList` lists.

Reasoning:

1. `List.get(int)` on `ArrayList` and `ArrayList$SubList` both compile to
   short monomorphic field-load-+-bounds-check sequences. `SubList.get`
   adds one check `if (i >= size) throw` over a simple offset-add; no
   iterator-object allocation, no iterator-state mutation, no iterator
   polymorphism.
2. C2 sees two call sites that both resolve to `List.get(int)`, which is
   a virtual call dispatched by the concrete List type. Hot-site
   bimorphic inlining on `ArrayList::get` and `ArrayList$SubList::get` is
   a well-understood, stable optimisation target (no speculation on
   `hasNext` truth, no iterator-state guard).
3. The loop exit condition `i < leafPayloads.size()` is trivially
   reachable, removing the `UnreachedCode@593` speculation failure.
4. No iterator object allocation — a minor allocation win on top of the
   wall improvement.

## Additional cleanup: Option B (ValueLayout hoist)

In `ProjectionIndexHOTStorage.decodeCompositeKeySegment` (line 1267), the call
`ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN)` is evaluated
per-invocation. Each invocation triggers a JDK-internal
`ConcurrentHashMap.computeIfAbsent` via `Utils.makeRawSegmentViewVarHandle`,
explaining the 99% of lock-time attribution in the iter#05 baseline lock
profile.

Hoist the layout to a static final:

```java
private static final ValueLayout.OfLong BE_LONG_UNALIGNED =
    ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
```

Wall impact is ~44 µs across a 4.8 s run (< 10 ppm), but:
- It's a correctness-safe drive-by fix.
- It closes a cross-resource hot-path contention issue for multi-workload
  deployments (many concurrent queries would hit the same ConcurrentHashMap).
- Fixes one of the top lock samples which otherwise pollutes future profiles.

## Expected result

| metric | before | after | expected delta |
| --- | --- | --- | --- |
| cold wall (median) | 4.71 s | ≤ 3.5 s | **≥ 20 %** |
| `conjunctiveCountByGroup` C2 deopts | 8+ | 0-1 | ≥ 80 % |
| VarHandle CPU samples (% of active wall) | 42.5 % | < 10 % | ≥ 70 % |
| lock blocking time | 44 µs | ~5 µs | ~90 % |

The 8 C2 deopts per cold run are the direct mechanism. Eliminating them
should close at least **half of the 2.97 s query-phase wall** because C2
would stabilise after 1-2 compiles rather than cycling for 1.35 s.

## Correctness/rollback plan

Semantic equivalence: byte-for-byte identical output.

Proof sketch:
- `List.get(i)` returns the i-th element of a `List<byte[]>`; for both
  `ArrayList` and `ArrayList$SubList` this is the same element that
  `list.iterator().next()` would return on the i-th call.
- `List.size()` is stable during the loop (callers pass an immutable
  snapshot).
- No ordering differences: the inner loop processes each payload exactly
  once, in the same order, with the same side-effects (`out.addTo(...)`).

Test plan: `ProjectionIndexByteScanTest` already covers semantic equivalence
between `ProjectionIndexScan.conjunctiveCount` (materialising path) and
`ProjectionIndexByteScan.conjunctiveCount` (zero-copy path) over 9 scenarios
— this will continue to pass if `ProjectionIndexByteScan` still produces the
right sum for every test input. Similarly, `SirixVectorizedExecutor` tests
that touch group-by queries will exercise the byGroup path.

Rollback: single-file revert of the method signature change. Callers adapt
trivially (pass `List<byte[]>` instead of `Iterable<byte[]>` — callers already
hold the former).

## HFT-grade checklist

- Explicit imports (no star imports). ✓
- `final` on locals, primitives over boxed types. ✓ (already the style)
- No synchronization in the hot loop. ✓ (none added)
- Pre-size `int` loop bounds once (not on every iteration). ✓
- `List.get(i)` monomorphic once C2 sees both call sites profile both types —
  no guard/fall-through in the hot inner loop.
- Zero extra allocation (in fact removes the iterator object allocation). ✓

## Measurement plan

After implementation:

1. Rebuild sirix-core + sirix-query. Verify class timestamps newer than source.
2. 5 verified-cold runs (`evict_db.py` before each launch).
   - Record median wall and projection build time.
3. 4-event profile capture (cpu/wall/alloc/lock) during one cold run.
   - Files: `iter05-postfix-{cpu,wall,alloc,lock}.collapsed` + `.txt` digests.
4. `-XX:+PrintCompilation` run to verify deopt-count in `conjunctiveCountByGroup`
   drops to 0-1 C2 compiles.
5. Gate checks:
   - **Keep** iff cold wall ≥ 10 % improvement AND deopt-count drops ≥ 80 %.
   - **Revert** otherwise.
6. `./gradlew :sirix-core:test :sirix-query:test --parallel` PASS.
7. Update `iteration-log.md` with the iter#05 entry.
