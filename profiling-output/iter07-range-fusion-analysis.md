# iter#07 — Range-fusion formal-verification gate

2026-04-22. Branch: `perf/umbra-ballpark-iter`. Pre-code formal-verification
gate, per the STOP-BEFORE-CODE + formal-verification discipline enforced
since iter#03c.

## Target

`compoundAndFilterCount` first-call under C2 (iter#06 default) is **568 ms**
vs. `filterCount` first-call at **218 ms** — a **350 ms delta** for an
almost-identical query shape. The only structural difference:

```
filterCount             : count(... where $u.age > 40 and $u.active)             — 2 preds
compoundAndFilterCount  : count(... where $u.age > 30 and $u.age < 50 and $u.active)  — 3 preds
```

One extra same-column numeric predicate doubles cold-wall cost. Profile
evidence (see iter#06 notes) attributes the extra wall to redundant
evaluation in `ProjectionIndexByteScan.evaluateLeafMask` (line ~309):

```java
for (final var p : predicates) {
  Arrays.fill(colMask, 0, stride, 0L);
  final byte kind = payload[kindsOff + p.column];
  switch (kind) {
    case COLUMN_KIND_NUMERIC_LONG -> evalNumericBytes(payload, columnDataOff[p.column], ...);
    ...
  }
  for (int i = 0; i < stride; i++) mask[i] &= colMask[i];
}
```

For `age > 30 AND age < 50`, this loop:

1. Calls `evalNumericBytes` **twice** on column `age`.
2. `evalNumericBytes` (line 417) does a **scalar-to-scratch load**
   (`for k < rowCount: scratch[k] = getLongLE(payload, baseOff + k*8)`)
   **twice** on the same 1024-row column — the payload is byte-addressed
   so the JIT can't CSE the loads across the two invocations.
3. Zone-map prune runs twice (lines 344-350) for two independent bounds.
4. Two `colMask ← mask & colMask` folds (cheap).

Per 100 K-leaf bench, per cold call: `rowCount × 8 B × 2 predicates ×
97,657 leaves ≈ 1.6 GB of redundant MOVQ loads` that should be one pass.

## Attack: range fusion

Detect at predicate-compile time that `age > 30 AND age < 50` is a single
**BETWEEN** on column `age`. Evaluate it with:

- One zone-map prune (`max < low OR min > high`).
- One scalar-to-scratch load.
- One SIMD compare for the low bound + one SIMD compare for the high
  bound + one AND into `colMask`.
- One AND into `mask`.

Versus today's two independent `evalNumericBytes` calls, this halves the
scalar-to-scratch loads (the dominant MOVQ stream) and halves the zone-
map work. SIMD compare + AND stays at ~0.5 ns/lane so the saving is
dominated by the load halving.

Expected win:
- Per-leaf numeric load: 2× → 1× → saves ~8 µs × 97,657 leaves ÷ 20
  threads ≈ **39 ms per call**.
- First-call JIT amortization: the fused BETWEEN branch is compiled
  once alongside the existing op arms, not as a new method; should
  not re-trigger C2 tier-up.
- `compoundAndFilterCount` first-call target: **568 ms → ~250-300 ms**
  (drop of ~45-50 %).
- Total cold-wall delta: ~0.05-0.1 s reduction on `compoundAndFilterCount`
  first-call (which is inside the 3-iter warmup window that contributes
  to the 3.47 s median). Conservative cold-wall estimate: **~1-3 %
  total** before considering JIT speculation stability.

## Correctness

### Op-combo matrix

Four BETWEEN variants cover all inclusive/exclusive combinations:

| low op | high op | compound semantics  | fused op        |
| ------ | ------- | ------------------- | --------------- |
| GT     | LT      | `low <  v <  high`  | `BETWEEN_GT_LT` |
| GT     | LE      | `low <  v <= high`  | `BETWEEN_GT_LE` |
| GE     | LT      | `low <= v <  high`  | `BETWEEN_GE_LT` |
| GE     | LE      | `low <= v <= high`  | `BETWEEN_GE_LE` |

These are the only legal pairings for `AND`-fused opposing-direction
comparisons. Fusion must NOT activate when the two predicates share a
direction (`age > 30 AND age > 40` — not a range) — those degenerate to
`age > max(30, 40)` but that's a different rewrite (monotonicity
absorption) outside the iter#07 scope.

### Edge cases

- **Empty range** (`low > high`, e.g. `age > 50 AND age < 30`):
  evaluator must return zero matches. In the inclusive-exclusive
  combinations (GT+LT, GT+LE, GE+LT) the range is empty if `low >=
  high`. For GE+LE the range is empty if `low > high`; if `low ==
  high` it degenerates to equality (one row matches). Our compare is
  `v.compare(lowOp, lowLit).and(v.compare(highOp, highLit))` which
  produces an empty mask naturally — no special-casing needed.
- **Single-value range** (`low == high`, both inclusive: GE+LE):
  compiles to `v >= L AND v <= L` → `v == L`. SIMD produces the
  correct bitmap without any special branch.
- **All-match** (e.g. low < min, high > max): zone-map prune never
  trips on either bound, evaluator fills true-bits for every row.
  Same as a single predicate would do — no behavior change.
- **No-match on zone** (`max <= low OR min >= high`): zone-map prune
  returns true, whole leaf skipped. Must behave identically to the
  union of two individual zone-map skips (which would each skip the
  leaf already).
- **More than two NumCmp on one column**: e.g. `a > 10 AND a > 20 AND a
  < 50`. Fusion should pair the tightest (max of lower bounds) + the
  other bound. Or more conservatively, fuse at most one pair per
  column and leave extras alone — correctness preserved because the
  un-fused ones still AND into `mask`. For iter#07 we take the
  conservative choice: **fuse at most one pair per column**. Extra
  predicates fall through to the existing per-op path. Zero
  correctness risk.
- **NumCmp mixed with EQ on same column** (`a == 7 AND a < 50`):
  don't fuse. EQ + range doesn't simplify under our op set; leave EQ
  + LT as two independent predicates. Same-column detection must
  check op kind is in {GT, GE} or {LT, LE} before pairing.
- **Out-of-range literals** (e.g. low == Long.MIN_VALUE): identical
  behavior to current code — one-sided compare degenerates to
  "always true" on one side, still produces the correct result.

### Zone-map BETWEEN semantics

A leaf's `(min, max)` for column `age`. The BETWEEN condition is
satisfied iff there exists a row `v` in `[min, max]` that lies in the
BETWEEN range. Zone-skip is the negation: no row can satisfy the
BETWEEN.

For BETWEEN_GT_LT (`low < v < high`):
```
zoneSkip iff   max <= low   OR   min >= high
```

For BETWEEN_GT_LE (`low < v <= high`):
```
zoneSkip iff   max <= low   OR   min >  high
```

For BETWEEN_GE_LT (`low <= v < high`):
```
zoneSkip iff   max <  low   OR   min >= high
```

For BETWEEN_GE_LE (`low <= v <= high`):
```
zoneSkip iff   max <  low   OR   min >  high
```

These are exactly the composition of the two independent zone-skip
conditions via OR, so the fused zone-skip is **strictly no more
pessimistic** than the un-fused pair — no rows gained, no rows lost.

### Equivalence proof sketch

For every leaf `L` with columns `age` and `active`:

- Un-fused: `mask_L = age_GT_30(L) AND age_LT_50(L) AND active(L)`
- Fused:    `mask_L = age_BETWEEN(30,50)(L) AND active(L)`

where `age_BETWEEN(low,high)(L)[i] = (age(L,i) > low) AND (age(L,i) <
high)`. Per row, this is exactly `age_GT_30(L)[i] AND age_LT_50(L)[i]`
by associativity of AND over compound bit predicates. QED.

The zone-map step in the fused path skips at least as many leaves as
the un-fused path (by the OR argument above). For the leaves that
pass, row-level bit masks are bit-identical.

## Algorithm

### Phase 1 — API shape (landed in ProjectionIndexScan.java)

- Four new `Op` enum values: `BETWEEN_GT_LT`, `BETWEEN_GT_LE`,
  `BETWEEN_GE_LT`, `BETWEEN_GE_LE`.
- `ColumnPredicate` gains `long highLit` field (default 0L for non-
  BETWEEN ops — same padding policy as `boolLit`/`stringLitBytes`).
- Static factory `ColumnPredicate.numericBetween(column, lowOp, lowLit,
  highOp, highLit)` validates `(lowOp, highOp)` is one of the four
  combinations and returns the matching fused `Op`.
- `evalNumeric` and `pruneByZoneMap` gain BETWEEN arms per the
  semantics above. The materialising `ProjectionIndexScan` is kept in
  parity with `ProjectionIndexByteScan` — both update in lockstep.

### Phase 2 — SIMD fast path (ProjectionIndexByteScan.evalNumericBytes)

```java
case BETWEEN_GT_LT, BETWEEN_GT_LE, BETWEEN_GE_LT, BETWEEN_GE_LE -> {
  // 1) One scalar-to-scratch load (unchanged; shared with single-op path).
  for (int k = 0; k < rowCount; k++) {
    scratch[k] = getLongLE(payload, baseOff + k * 8);
  }
  // 2) Two SIMD compares + AND.
  final VectorOperators.Comparison loCmp, hiCmp;
  switch (op) {
    case BETWEEN_GT_LT: loCmp = GT; hiCmp = LT; break;
    case BETWEEN_GT_LE: loCmp = GT; hiCmp = LE; break;
    case BETWEEN_GE_LT: loCmp = GE; hiCmp = LT; break;
    case BETWEEN_GE_LE: loCmp = GE; hiCmp = LE; break;
  }
  int i = 0;
  final int simdEnd = rowCount - (rowCount % LANES);
  for (; i < simdEnd; i += LANES) {
    final LongVector v = LongVector.fromArray(LONG_SPECIES, scratch, i);
    final VectorMask<Long> mLo = v.compare(loCmp, lit);       // lit == lowLit
    final VectorMask<Long> mHi = v.compare(hiCmp, highLit);
    final long bits = mLo.and(mHi).toLong();
    if (bits != 0L) {
      final int wordIdx = i >>> 6;
      final int bitOffset = i & 63;
      out[wordIdx] |= bits << bitOffset;
    }
  }
  // Scalar tail — same as single-op path but with both bounds.
  for (; i < rowCount; i++) {
    final long v = scratch[i];
    final boolean match = /* switch over op with v vs lit/highLit */;
    if (match) out[i >>> 6] |= 1L << (i & 63);
  }
}
```

Cost per leaf (1024 rows, AVX-512 with LANES=8):

- Load: 1024 × 8 B = 8192 bytes MOVQ stream (~1 µs at 8 GB/s L2).
- SIMD compare: 128 iterations × 2 compares × 0.5 ns = 128 ns.
- AND + store: 128 × 0.3 ns = 38 ns.
- Scalar tail: up to 7 rows, negligible.

Un-fused cost for two independent predicates: 2× load + 2× SIMD compare
+ 2× AND-into-mask.

Fused saving per leaf: one load (~1 µs) + one mask AND (~40 ns).
Dominant saving is the load, ~8 KB/leaf × 97,657 leaves ÷ 20 threads
≈ 40 MB/thread → about 40 µs per thread in L2-bound throughput. That
scales to 3 predicates on 1.5 GB/s per-thread to be ~40 ms total
per query — matching the first-call profile delta.

### Phase 3 — Fusion pass (SirixVectorizedExecutor)

After `extractConjunctivePredicates` produces a `ColumnPredicate[]`:

1. Build a `column → List<NumCmpIdx>` map (IntObjectHashMap from
   fastutil, pre-sized to `preds.length`). Each entry records
   `(predicate index, Op)`.
2. For every column with ≥ 2 numeric entries, find one GT/GE + one
   LT/LE pair. If found, emit a fused `BETWEEN_*` predicate,
   deleting the pair from the output.
3. Preserve order of remaining predicates (stable). Preserve non-
   numeric and same-direction numerics unchanged.
4. If no fusion possible, return the input array unchanged — zero
   overhead.

Controlled by `-Dsirix.projection.rangeFusion=true` (default). Setting
`=false` skips the pass entirely.

### Phase 4 — tests

- Unit: `ProjectionIndexByteScanTest.betweenParity` with op-combo
  matrix — fused vs un-fused byte-for-byte equality on synthetic
  1024-row leaves.
- Unit: `ProjectionIndexByteScanTest.betweenEdgeCases` covering
  empty range, single-value range, all-match, no-match, BETWEEN
  mixed with boolean + string-eq predicates.
- Unit: `SirixVectorizedExecutorFusionTest` exercising the fusion
  pass on conjunctive predicate arrays of shapes `[GT, LT]`, `[GT,
  LT, BOOL]`, `[GT, LT, EQ]`, `[GT, GT]` (no fusion), `[GT, GE, LT]`
  (partial fusion with GE left alone).
- Integration: the existing 100M-scale bench `compoundAndFilterCount`
  is the real-world correctness test — its result count must match
  across fused and un-fused runs (compare with `-Dsirix.projection.
  rangeFusion=false`).

## Rollback

- `-Dsirix.projection.rangeFusion=false` disables the fusion pass —
  array passes through unchanged. No code-path divergence in the
  evaluator beyond the dead BETWEEN branches (which C2 removes).
- Feature-flag gate `FUSE_RANGES_ENABLED` boolean static-final
  initialized once from the system property. JIT folds to
  `if (false) fuse(...);` → dead branch eliminated.
- BETWEEN op arms in the evaluator remain valid even with the
  fusion pass off — they're just never reached.

## HFT invariants

- No allocation in the fusion pass hot path: the fusion pass runs
  once per query compile, not per leaf. Uses a pre-sized fastutil
  Int2ObjectOpenHashMap that's allocated on the per-query stack
  (tens of entries max).
- BETWEEN evaluator path reuses the same `scratch[]` that the
  single-op path uses — no new buffer.
- All op-arm branches are `final` constants on `Op` enum; C2's
  switch-lowering produces a single tableswitch.
- No virtual dispatch: `ColumnPredicate` is final with public fields
  (unchanged from current). `ProjectionIndexScan.Op` is an enum.

## Decision (pre-bench)

Land the BETWEEN ops + evaluator arms + fusion pass behind the
rollback flag, defaulting **ON**. Keep if:

1. `compoundAndFilterCount` first-call drops ≥ 30 % (568 → 400 ms).
2. Total cold-wall drops ≥ 3 % (3.47 → 3.37 s).
3. All parity tests pass byte-for-byte.
4. `:sirix-core:test :sirix-query:test --parallel` green.
5. No regression on other cold-wall runs (verified-cold ×3 median
   before/after differs within ±5 %).

Revert if any of the above fail.
