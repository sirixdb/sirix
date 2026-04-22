# iter#05 result — hypothesis rejected, JVM path declared plateau

2026-04-22. Branch: `perf/umbra-ballpark-iter`. Final iter#05 status: **no code
landed**; baseline attack abandoned after native A/B + compile-log data
disproved the primary hypothesis. JVM cold wall holding at **4.71 s** median —
recommending declaring the JVM path done at its current plateau.

## Decision

**No iter#05 code lands.** All experimental refactors were reverted after
measurement data contradicted the hypothesis.

## Baseline reproduced

5 verified-cold runs of the iter#04 default build:

| run | wall | hydrate |
| --- | --- | --- |
| 1 | 4.69 s | 1,129 ms |
| 2 | 4.55 s | 1,139 ms |
| 3 | 4.73 s | 1,141 ms |
| 4 | 4.71 s | 1,134 ms |
| 5 | 4.86 s | 1,170 ms |
| median | **4.71 s** | **1,141 ms** |

Roughly 8 % faster than iter#04's documented 5.10 s median — within normal
time-of-day / residency drift. Treating **4.71 s** as the iter#05 baseline.

## Attack attempted — two refactors (both reverted)

### Refactor 1 — `Iterable<byte[]>` → `List<byte[]>` indexed walk

**Hypothesis**: C2 bimorphic dispatch between `ArrayList$Itr` (sequential caller)
and `ArrayList$SubList$1` (parallel-chunk caller) on the outer `for-each` in
`conjunctiveCountByGroup` was causing 8+ OSR recompiles across 1.35 s of wall.

**Result**: 5 cold runs — 4.17, 5.80, 4.67, 5.20, 4.87 s → **median 4.87 s vs
baseline 4.71 s** (slightly regressed within noise). Deopt count went from 7 to
9. Reverted.

### Refactor 2 — split zero-predicate / non-zero-predicate paths

**Hypothesis**: C2 compiled `conjunctiveCountByGroup` first with
`predicates.length == 0` (groupByDept, countDistinct), then hit the
`UnreachedCode@593` speculation failure when filterGroupBy passed a non-empty
predicate array.

**Result**: **Wall 4.91 s vs baseline 4.71 s → ~4% regression.** Deopt total
stayed similar (13 distributed across two methods instead of 9 in one).
Reverted.

## Why both refactors failed — the native-PGO A/B data

Per the coordinator's suggestion: if JIT deopt is the real 1 s+ tax, then
native-image PGO (no JIT) should show sub-100 ms first-call across all queries.
Captured native PGO first-call with `-Dsirix.noWarmup=true -iters=1`:

| query | JVM first-call (ms) | Native PGO first-call (ms) | verdict |
| --- | --- | --- | --- |
| filterCount | 218 | **331** | **JVM FASTER** — eliminates cold-cache / streaming theory |
| groupByDept | 1,149 | 94 | 12× faster on native — JIT deopt confirmed |
| sumAge | 37 | 2.15 | 17× faster on native |
| avgAge | 1.6 | 0.32 | JVM already fast |
| minMaxAge | 2.7 | 0.55 | JVM already fast |
| groupBy2Keys | 0.9 | 0.05 | JVM already fast |
| filterGroupBy | 1,057 | **43** | 25× faster on native — JIT deopt confirmed |
| countDistinct | 97 | **96** | tied — NOT a JIT issue |
| compoundAndFilter | 490 | **347** | 1.4× faster on native — mostly NOT JIT |

**Interpretation**:
- JIT deopt is real **only for the group-by-count path** (groupByDept,
  filterGroupBy). Those two queries account for ~1.1 s of JVM cold wall alone.
- The other 7 queries match or beat native's first-call — they're NOT
  JIT-deopt-bound.
- `filterCount` is actually SLOWER on native (331 ms vs JVM 218 ms) — native
  has a cold CPU/code-cache cost that JVM doesn't.
- **The specific JIT deopt target is `conjunctiveCountByGroup`'s inner loops
  (bytecode positions 305, 407, 421), not the outer iterator.**

### The compile-log reveals the real culprit

After reverting refactor 2, the original method's deopts sit at:
- `uncommon trap @ 421` — inner `while (word != 0)` bit-extraction loop
- `uncommon trap @ 407` — middle `for (w = 0; w < stride; w++)` word loop
- `UnreachedCode@19[..., 593]` — outer iterator exit at the method's tail
- `GuardMovement@11[..., 106, NullCheckException]` — C2 mis-hoisted null check

The inner loops deopt because C2's speculative DCE eliminates the "word == 0"
branch assuming uniform non-zero words, then hits all-zero words (leaves where
no rows match the predicate). Refactoring the outer iterator doesn't address
this. Splitting the method moves the deopts to a different bytecode position
but doesn't eliminate them — the inner-loop speculation failure is structural
to the masked-bitmap iteration pattern.

## What this means for the JVM plateau

The 4.71 s cold wall decomposes (measured via instrumented bench timing probe,
reverted after capture):

| phase | wall |
| --- | --- |
| JVM startup + class init | ~400 ms |
| `store.build + ctx/chain` | ~40 ms |
| `store.lookup + beginResourceSession` | ~120 ms |
| Projection hydrate (parallel depth-2) | 1,141 ms |
| **Pre-query wall** | **~1,700 ms** |
| Query phase (9 queries × 4 invocations each with 3 warmup + 1 measure) | ~2,970 ms |
| **Total** | ~4,670 ms |

Inside the query phase:
- `conjunctiveCountByGroup` first-call cost (groupByDept + filterGroupBy
  combined): ~2,200 ms of cold wall
- Other queries combined: ~770 ms

**The 2.2 s `conjunctiveCountByGroup` first-call cost IS JIT-deopt-driven**
(proven by native PGO showing 137 ms combined for the same two queries).
Removing it would bring JVM cold wall to ~2.5 s — within striking distance of
DuckDB's 2 s ballpark.

But **every JIT-stabilisation technique tried so far has failed to eliminate
the inner-loop speculation failures**:
- `-XX:CompileThreshold=1`: 4.53 s (saved ~200 ms but didn't close the gap)
- Iterator refactor: no effect on inner-loop deopts
- Method split: deopts migrate, don't disappear

### Remaining attacks (not attempted this iter)

1. **Pre-warm with synthetic predicates before bench queries run.** Instead of
   letting the first real query pay the JIT cost, fire a synthetic
   `conjunctiveCountByGroup` call at bench startup with ALL predicate shapes
   (zero-pred, numeric-pred, boolean-pred, compound-pred) to fully train C2's
   type profile before measurement. Plausible ~1 s saving if it works.

2. **`-XX:-UseCountedLoopSafepoints`** + `-XX:LoopUnrollLimit=0`: disables some
   of C2's speculative loop transformations that are triggering the
   `UnreachedCode` failures. Unclear whether this would help or hurt
   steady-state performance — needs a careful A/B.

3. **JVMCI/Graal JIT** (`-XX:+UseJVMCICompiler`): Graal JIT's speculation model
   is different from C2's; might compile the inner loops without the
   data-dependent dead-code elimination that trips HotSpot C2. Oracle GraalVM
   Community is already in use here; enabling JVMCI on top may change
   behaviour.

4. **Replace masked-bitmap iteration with dense row scan for short leaves.**
   At 1024 rows/leaf the bitmap savings on partially-matching leaves might not
   outweigh the JIT speculation cost. A dense loop that evaluates the
   predicates per row, unrolled via SIMD, may compile more stably.

5. **Native PGO as the production path.** Already measured at 4.70 s cold (tied
   with JVM), 4.44 s warm (faster than JVM's 5.91 s), and dominant per-query
   (10-40× speedups on hot queries). With graal#13377 closed, native PGO
   projection hydrate would drop from 3.2 s to ~1.1 s, giving **~2.5 s native
   cold** — DuckDB ballpark territory. Open issue: <https://github.com/oracle/graal/issues/13377>.

## Recommended iter#06 path

**Option A (low-risk): pre-warm attack.** Add a one-shot synthetic warm-up at
bench startup after hydrate completes, before the real queries run. Run each
query shape once against the installed projection handle so C2 sees all
predicate-array shapes before the bench timer starts. Expected gain: 0.8-1.2 s
off cold wall if it works. Rollback: drop the warm-up call.

**Option B (higher risk, higher ceiling): Graal JIT.** Try
`-XX:+UseJVMCICompiler -XX:+EnableJVMCI` instead of C2. Graal is bundled with
the Oracle GraalVM 25 build already in use and can be flipped on via a flag.
Expected gain: unknown — could close the gap or open a new one. Rollback:
drop the flag.

**Option C (structural, multi-week): Unblock graal#13377 + land native PGO as
the primary target.** This has been the long-tail winner track since iter#02 —
native PGO is already competitive on cold (4.70 s) and dominant on warm (4.44 s
vs JVM 5.91 s), and only held back by the MemorySegment-intrinsic gap on
projection hydrate. When that lands, native cold drops to ~2.5 s and becomes
the production recommendation for cold-start latency.

## Declaration

Absent one of the above three interventions, **the JVM path is at plateau**:

- iter#00 → 108 s
- iter#01 → 5.29 s (20× win)
- iter#03c → 5.08 s (parallel hydrate)
- iter#04 → 5.10 s (prefetch off)
- **iter#05 → 4.71 s (no code landed; ±8 % residency drift)**
- target: 2.0 s
- remaining gap: 2.5 s = ~1 × factor, not 5 ×

Attack surface for the remaining gap lives either inside HotSpot's JIT
internals (Options A + B above) or in native-image ecosystem fixes (Option C).
Further iteration inside the sirix-core Java code is unlikely to move the
needle further on the cold 100M brackit-scale bench without addressing one of
those systemic levers.

## Files / artifacts

- `profiling-output/iter05-baseline-{cpu,wall,alloc,lock}.collapsed` — raw
  async-profiler dumps (baseline profile).
- `profiling-output/iter05-baseline-{cpu,wall,alloc,lock}.txt` — top-25-frame
  digests.
- `profiling-output/iter05-baseline-analysis.md` — baseline profile
  interpretation including the JIT deopt diagnosis.
- `profiling-output/iter05-plan.md` — pre-code formal-verification doc (the
  plan that did NOT land).
- `profiling-output/iter05-result.md` — this document.

No code changes landed. In-tree uncommitted diff reduced to the iter#02
prior-campaign state (VarHandle javadoc + `getIntLE/getLongLE` helpers — both
pre-dating iter#05 work).

## Test suite

`./gradlew :sirix-core:test --tests "io.sirix.index.projection.*" --parallel` —
PASS (all projection tests green on in-tree state).
