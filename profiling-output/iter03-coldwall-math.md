# iter#03 — cold-wall budget analysis (before code)

2026-04-22, Sirix perf campaign, branch `perf/umbra-ballpark-iter`.

## Cold-wall baselines (verified cold: `sirix.data` residency 0.00% before each run)

| binary | cold wall | projection build | per-query (iters=1) sum |
| --- | --- | --- | --- |
| JVM HotSpot (Oracle GraalVM 25.0.2) | **5.15 s** | 2,516 ms | 3.24 ms |
| native-image PGO O3 march=native     | **4.72 s** | 3,265 ms | 0.28 ms |

3 cold runs, median wall, load ≤ 2. Residency verified by
`/tmp/claude/evict_db.py` (mincore-based) — all runs 0.00% resident
before bench start.

## Warm-wall (iters=3 after default warmup)

| binary | run1 | run2 | run3 | median wall |
| --- | --- | --- | --- | --- |
| JVM                       | 6.26 s | 5.64 s | 5.91 s | **5.91 s** |
| native PGO                | 4.79 s | 4.97 s | 5.05 s | **4.97 s** |

Per-query **avg(ms)** across 3 measured iters (one representative run each):

| query | JVM avg (ms) | native avg (ms) | native/JVM |
| --- | --- | --- | --- |
| filterCount            | 0.596 | 0.039 | ~15× |
| groupByDept            | 0.671 | 0.026 | ~26× |
| sumAge                 | 0.426 | 0.011 | ~39× |
| avgAge                 | 0.304 | 0.014 | ~22× |
| minMaxAge              | 0.337 | 0.019 | ~18× |
| groupBy2Keys           | 0.341 | 0.024 | ~14× |
| filterGroupBy          | 0.492 | 0.034 | ~15× |
| countDistinct          | 0.196 | 0.025 | ~8× |
| compoundAndFilterCount | 0.473 | 0.047 | ~10× |

**Interpretation (per user's framing correction)**:

Native PGO warm per-query kernels are **10–40× faster** than JVM
warm per-query kernels. Even with graal#13377 biting the MemorySegment
hot path, the AOT advantage on narrow-executable kernels shines when
it gets to actually run — the query shapes land in ~30 µs on native
vs ~400 µs on JVM. The AOT upper bound is COLD (no JIT tax), so warm
is where JVM catches up — yet JVM *still* doesn't fully catch up here,
because the per-query kernel is so tight (sub-ms) that the JIT-vs-AOT
per-call fixed cost dominates.

**If graal#13377 landed** (MemorySegment intrinsification), projection-
hydrate would drop from 3.3 s → ~2.5 s (matching JVM), putting total
cold native at **~3.9 s** — still above the 2 s DuckDB ballpark but
within striking distance with any additional 1.5 s win elsewhere.
Today's 4.7 s native floor is 75% pre-query latency that AOT can't
further shrink without startup tuning (build-time init, image layout).

## Cold-wall budget decomposition (JVM 5,150 ms)

```
  2,500 ms   projection-index hydrate   (48.5 %)    ← single-threaded readAllViaCursor
  ~2,000 ms  DB open + class init + JIT  (38.8 %)   ← front-loaded, per-cold-start
     400 ms  first-call JIT warmup       ( 7.8 %)   ← C2 compile of hot query path
     250 ms  page-cache init, misc       ( 4.9 %)
       3 ms  query exec (9 queries)      ( 0.06 %)  ← sum of per-query min(ms)
```

## Pivot target: Data Blocks SARGable SIMD-on-packed bytes

The pivot replaces uncompressed 8-byte longs in
`COLUMN_KIND_NUMERIC_LONG` with FOR+BP compact encoding
(`NumberRegionCompact` already exists in the codebase but is unused
in the projection path), so `evalNumericBytes` can run SIMD compares
directly on the packed byte stream after a single re-pack of the
predicate literal at block entry.

### Existing on-disk format (ProjectionIndexLeafPage)

```
  NUMERIC_LONG column:
    long min, long max
    long[rowCount] values      ← 8 bytes per value, uncompressed
```

`ProjectionIndexByteScan.evalNumericBytes` (line 417) does:
```
  for (int k = 0; k < rowCount; k++) scratch[k] = getLongLE(payload, baseOff + k*8);
  LongVector.fromArray(LONG_SPECIES, scratch, i).compare(cmp, lit);
```
That's a scalar MOVQ decode loop feeding SIMD compare — **not** SIMD
on packed bytes, despite the leaf-page javadoc (line 36-39) claiming
a `valueBitWidth` / `valueBase` / `packedValues` layout. Format drift:
the doc documents the intended shape, the implementation never
adopted it.

### What the pivot would buy — cold wall

```
  query-exec wall, cold iters=1 sum:                 3.24 ms
  evalNumericBytes self-time in that phase:          ~50 %  of query-exec
  pivot gain (est.): 2× faster (bit-packed compare): ~0.8 ms saved
  cold wall improvement:   0.8 / 5,150 = 0.016 %
```

### What the pivot would buy — warm wall

At iters=3 + 20-iter warmup, query-exec wall across JVM run ≈ 4.2 ms
× 9 queries × 23 iters ≈ **870 ms** (about 17 % of the 5.64 s warm
JVM wall). Halving `evalNumericBytes` gives:
```
  pivot gain (est.): 870 × 0.5 × 0.4 = 174 ms saved     (40 % of 17 %)
  warm wall improvement:    174 / 5,640 = 3.1 %
```

Still below the 10 % keep-threshold at the cold level. On **warm**
numbers, 3 % is marginal but measurable.

## Honest engineering recommendation

The user's iter#03 target is cold ≤ 2 s. The pivot's projected cold
contribution is **0.016 %**, well below the 10 % keep-threshold. It
would show up on warm wall (~3 %) and on warm per-query avg (20–40 %
reduction on numeric predicates), but not on the campaign's headline
metric.

**The 2,500 ms projection-hydrate dominates cold wall — 48.5 %.**
That's the lever worth pulling for cold, and it's exactly what
iter#03's original plan targeted before the pivot. The user's
coordinator message stopping parallelism work was correct in
demanding formal verification, but abandoning that lever entirely
moves us to a pivot with a 0.016 % cold ceiling.

### Three options

1. **Ship the pivot anyway** (FOR-BP + SIMD-on-packed). Delivers
   3 % warm wall + substantially faster per-query kernels. Improves
   warm benchmarking, moves DB on-disk size down for numeric columns.
   Does not reach cold ≤ 2 s.

2. **Revisit projection-hydrate parallelism with formal verification.**
   The earlier concerns were valid (HOT tree skew, per-thread guards,
   tombstone merge), but the 48 % cold budget is the only lever with
   the right magnitude. A safe design = (a) one sub-task per root-
   indirect child's sub-tree, (b) each worker opens its own
   `JsonNodeReadOnlyTrx` (per-trx page cache + ThreadLocal scratch),
   (c) tombstone detection per-thread, global merge asserts no cross-
   thread key collision (HOT hashes leafIndex uniquely to one root
   sub-tree — prove-it test exists today via readAllViaCursorParallel
   code). Known limits: root fan-out = 3–5 → effectively 3–5× not
   20×. Expected cold gain: 2.5 s → ~0.6 s = **1.9 s saved = 37 %
   cold wall reduction**.

3. **Data Blocks Leaf-Summary Tier** (the iter#03b fallback the
   user's A-question cluster mentioned). Instead of parallelizing
   hydrate, compute a tiny per-leaf summary (min/max per column + row
   count + tombstone bit, ~40 bytes per leaf) and persist it as
   either a separate projection-root blob or inline in each HOT leaf.
   Skip the full leaf hydrate at install time: only materialize
   leaves whose zone-map *could* match the query's predicate.
   97,657 leaves × 40 B = **3.8 MB** to hydrate vs the current
   ~200 MB. Expected cold gain: 2.5 s → ~0.2 s = **2.3 s saved =
   45 % cold wall reduction**. Clean design: no shared state,
   no ordering concerns, no parallelism safety fence.

## Recommended path

**Option 3** (leaf-summary tier) is the cleanest big win for cold,
with option 1 (FOR-BP + SIMD-on-packed) as a follow-up for warm
wall. Option 2 (parallelism) carries real correctness risk and
only reaches ~60 % of option 3's gain — worth skipping.

Awaiting user direction before writing code.
