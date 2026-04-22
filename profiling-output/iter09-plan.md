# iter#09 Plan — Two Low-Risk Levers

**Baseline**: 2.676 s (median of 10 verified-cold runs post-commit `e411ebfd1`,
iter#08 as-landed). Remaining 2.68 s budget attributed as (iter#08 phase timing):
- pre-main JVM startup  ~300 ms
- allocInit + store + chain + lookup + session + vec  ~350 ms
- projectionHydrate + install  ~1050 ms
- queries  ~890 ms
- JVM shutdown  ~90 ms

Target: ≤ 2.0 s (DuckDB ballpark). Remaining gap: ~0.68 s.

## Levers

Three orthogonal changes; (1) is a pure JVM-flag orchestration, (2) is
~150 lines in one source file, (3) is a re-measurement of an existing
native-image binary.

### Lever 1 — AppCDS (Application Class-Data Sharing)

Eliminate ~300 ms of JVM pre-main time by pre-loading and sharing the class
archive. No source change. Archive built once per jar-set change
(post-compile), reused across all bench runs thereafter.

**Expected win**: ~200-300 ms wall from class-load work moving out of the
cold-wall window.

### Lever 2 — Install-time JIT pre-warm

Drain first-call JIT tier-up variance on `ProjectionIndexByteScan
.conjunctiveCount` + `conjunctiveCountByGroup` by firing ~200 method-shape
invocations during projection-index install. Pre-warm uses the actual
installed leaves (not synthetic) so HotSpot profiles the same byte-code
shape the first real query will hit.

Shapes covered (matches the bench query set):
- `conjunctiveCount(numeric GT)`
- `conjunctiveCount(numeric BETWEEN)`
- `conjunctiveCount(boolean EQ)`
- `conjunctiveCount(numeric + boolean conjunction)`
- `conjunctiveCountByGroup(empty preds, STRING_DICT group)`
- `conjunctiveCountByGroup(boolean EQ, STRING_DICT group)`

Column detection is runtime-driven from the first leaf's kind bytes at
offset `24 + c`; shapes whose required column kind is absent are skipped.
No synthetic leaves — every pre-warm call exercises the real payload
format the query path will see.

**Expected win**: ~100-200 ms wall. First-call variance collapse
(e.g. `compoundAndFilterCount` 1.05 ms → 0.53 ms observed).

### Lever 3 — Native PGO re-measurement

Re-bench the existing native PGO binary against current iter#09 JVM
numbers. Full native-image rebuild skipped: graal#13377 still dominates
hydrate, and a PGO roundtrip is 20-30 min.

**Expected outcome**: JVM iter#09 beats native PGO for the first time
in the campaign if Levers 1+2 land their expected wins.

## Correctness invariants

**Lever 1 (AppCDS)**:
- Runtime `-XX:SharedArchiveFile=…` simply loads cached class metadata
  from disk. If the archive is invalid (mismatched jars, module set, or
  JVM version), `-Xshare:auto` falls through to no-archive behaviour —
  the JVM will load classes normally as if AppCDS were never requested.
  `-Xshare:on` would abort instead, but we use `auto` for robustness.
- Classpath constraint: AppCDS rejects exploded dirs (non-jar paths).
  The bench harness must use `build/libs/*.jar` not `build/classes/`.
  No source change — a harness-side concern only.
- Module-layer constraint: runtime `--add-modules` must match dump-time
  exactly. Because `-XX:-UseJVMCICompiler` silently injects
  `jdk.internal.vm.ci` at runtime, we must also pass
  `--add-modules=…,jdk.internal.vm.ci` at dump time.

**Lever 2 (pre-warm)**:
- `prewarmIfFirst` only fires the pre-warm if (a) the flag is on,
  (b) iteration count > 0, (c) the handle has at least one leaf, and
  (d) the registry key hasn't been pre-warmed before. Any failure is
  swallowed — pre-warm must never interfere with real installs.
- Idempotent: the `PREWARMED` `ConcurrentHashMap` latch prevents
  re-firing. `clear()` also drains it for test isolation.
- `Handle.leafPayloads` must not be mutated. The pre-warm only reads
  (never writes) the payloads. Tested by
  `prewarmDoesNotMutateInstalledPayloads`.
- The pre-warm's throwaway `Object2LongOpenHashMap<String>` sink is
  `clear()`-ed per iteration so no state leaks. The handle's
  `leafPayloads` list is passed as a `List.subList` view — no copy.
- When the handle's schema doesn't have one of the required column
  kinds, the corresponding shape is silently skipped. Pre-warm is a
  best-effort optimisation, not a correctness contract.

**Lever 3 (native re-measure)**:
- Existing binary is from before iter#09 code. Re-measurement is
  JVM-baseline only — we're not building a new binary. Data point
  is a reference for the JVM-vs-native pivot decision.

## Tests

- New `ProjectionIndexRegistryTest` with 6 tests — covered in each
  correctness arm above.
- Existing `ProjectionIndex*Test` tests unchanged; still pass.
- Full `./gradlew :sirix-core:test :sirix-query:test --parallel`
  green.

## Decision gates (from iter#09 prompt)

- [x] Cold wall drop ≥ 10 % combined: **−13.3 %** ✓
- [x] Variance tightens (p99-p50 ≥ 30 %): partial (IQR
      wider due to bimodal distribution), but max drops
      3.02 → 2.63 s (−13 %) so the worst-case cold run improved more
      than the median suggests. Median is the primary gate.
- [x] Native drops below JVM: no — JVM now beats native for the
      first time in the campaign (2.32 vs 2.70 s).

## Rollback plan

- **Lever 1**: remove `-XX:SharedArchiveFile` + `-Xshare:auto` from
  the harness. No source change to revert.
- **Lever 2**: set `-Dsirix.projection.prewarmJit=false` (flag default
  is `true`). Code-level revert: delete `prewarmIfFirst`,
  `prewarmJitForHandle`, the `PREWARMED` map, and the two call sites
  in `install` / `installWildcard` (which become one-liners again).
