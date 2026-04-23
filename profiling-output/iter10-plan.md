# iter#10 Plan — Dense group-by + native PGO rebuild

Baseline: JVM C2 cold 100M **1.98 s** median (iter#09 post-commit, 10 runs,
7/10 sub-2s, commit `7b55669ad`, doc commit `eeffae043`). DuckDB ballpark
achieved; this iter pushes further.

Remaining budget (post-commit phase timing):
- allocInit + session + lookup ~180 ms
- projectionHydrate + install ~1,040 ms
- queries ~830 ms
- JVM shutdown ~90 ms

Biggest single remaining frame on the query side: `Object2LongOpenHashMap.
addTo + addToValue + String.equals + String.hashCode` = ~10.8 % of cold CPU
(iter09-cpu.txt: 99 + 19 + 58 + 27 = 203 samples / 1879 = 10.8 %).

## Levers

### Lever 1 — Native PGO rebuild against iter#09 source

The existing native binary at `bundles/sirix-query/build/native/
nativeCompile/sirix-bench` was built 2026-04-22 23:51:01, which predates:
- `e411ebfd1` (iter#08 zero-alloc cursor + pre-sized hydrate, 2026-04-23 00:03)
- `7b55669ad` (iter#09 AppCDS + install-time JIT pre-warm, 2026-04-23 00:42)

The AppCDS win doesn't apply to native-image (AOT has no class-load
step). The JIT pre-warm is equally moot on native (AOT has no JIT).
**But** iter#08's zero-alloc HOT cursor + pre-sized hydrate accumulator
are pure algorithmic wins that should transfer to native-image.

Existing native PGO binary was 2.70 s median. Native at iter#04 (after
prefetcher=0) was 3.13 s; iter#08 equivalents should land ~2.0-2.3 s on
native, potentially overtaking JVM (1.98 s).

**Expected win**: native cold drops from 2.70 s → 2.0-2.3 s (−15-25 %).
If native beats JVM (< 1.98 s), native PGO becomes the production path
and JVM plateau becomes the floor. If native lands ≥ JVM, the graal#13377
MemorySegment regression is still gating native; JVM stays production.

### Lever 2 — Dense long[N] group-by accumulator for bounded STRING_DICT

Current `ProjectionIndexByteScan.conjunctiveCountByGroup`:
1. Per matching row: read `int dictId = getIntLE(payload, idsOff + rowIdx*4)`.
2. Cache lookup: `String gv = dictCache[dictId]` (per-leaf String[64]).
3. First-seen: compute FNV-1a64 hash of `len` bytes + `intern.get(h)` +
   `new String(...)` + `intern.put(h, String)` + `dictCache[dictId] = gv`.
4. Always: `out.addTo(gv, 1L)` on `Object2LongOpenHashMap<String>`.

Even with the per-thread long-hash intern (→ 8 String allocs total per
thread per scan for the 8-dept bench), Object2LongOpenHashMap.addTo pays:
- `String.hashCode()` (per call — cached but called)
- `String.equals()` collision chain walk (on 8-entry table, 8-64 chars)
- `boxed-put` into fastutil's open-addressing array at that hash slot

For a bench workload with **bounded and known-at-install cardinality**
(dept=8, city=8 — both STRING_DICT with small dictionaries), every
addTo is pure overhead; a `long[N]` keyed by dictId replaces ~10 % of
CPU with 1 array-indexed increment.

**Key insight**: STRING_DICT dict arrays are per-leaf (each leaf builds
its own dict during projection build) — the same canonical name ("Eng",
"Sales", ...) may land at different dict IDs in different leaves. So
we can't use `leaf.dictId` directly as the `long[]` index. We need a
shared **canonical dict → global id** map OR to remap per-leaf-dictId
to a shared ordinal on the fly.

The simplest design: scan a representative prefix of leaves **once at
install** and collect the UNIQUE dict-byte-slices per STRING_DICT
column into a shared `byte[][]` canon dict (length N ≤ threshold).
Per-leaf, map `leaf-dictId → canonId` once at leaf entry (cost: a
small byte-array lookup per distinct dict-slice per leaf, ~100-1000 ns
per leaf for 8-entry dicts). Then per matching row: `counts[canonId]++`
against the shared `long[N]`. Merge at end.

**Eligibility predicate** (`denseEligible(handle, groupColumn)`):
1. Scan the first K=16 leaves to find the group column's dictionary
   cardinality. If ANY leaf has dict size > 1, take the UNION across
   those K leaves' dict values (byte-compared) as a proxy for global N.
2. If the proxy N ≤ `DENSE_GROUPBY_CARD_LIMIT` (default 256), eligible.
3. The canonical dictionary is cached per-Handle — same column always
   produces the same canon dict within a single JVM lifetime; cache
   under `Handle.canonicalDicts: Map<Integer groupColumn, byte[][]>`.

**Fallback path**: When a leaf's dict contains a byte-slice NOT in the
canonical dict (late-arriving value or dict cardinality grew past what
the 16-leaf probe found), or when N > threshold to begin with, we fall
back to the existing hashmap path. This guarantees exact correctness —
the dense path is an optimization, not a semantic change.

**Dispatcher** in `SirixVectorizedExecutor.parallelConjunctiveCountByGroup`
picks dense vs hashmap at invocation time:
- `denseEligible(handle, groupColumn)` → dense path with per-worker
  `long[N]` + merge at end into `Object2LongOpenHashMap<String>` (for
  API parity with existing callers).
- Otherwise → existing hashmap path.

## Correctness invariants

### Lever 1 (native PGO rebuild)

- All iter#07-09 source changes already compile for JVM and are
  non-invasive w.r.t. native-image:
  - Range fusion: pure Java tableswitch on enum. No FFM/MemorySegment.
    `LongVector` is JEP 508 (incubating) which native-image supports
    since GraalVM 21+ via `-H:+VectorAPISupport`.
  - Zero-alloc cursor: exposes package-private `advance()` + positional
    accessors on `HOTRangeCursor`. Slices still come from
    `slotMemory.asSlice` which wraps an off-heap segment — on native-image
    this is the same `NativeMemorySegmentImpl` (graal#13377 affects
    `get/set` with ValueLayout, NOT `asSlice` which is a pointer arith
    wrapper). Zero-alloc cursor should work.
  - Pre-sized hydrate buffers: `EXPECTED_LEAF_BYTES=24576` System property.
    `byte[]` alloc + copy, no FFM. Native-safe.
  - AppCDS: JVM-only; native-image has no class-load step.
  - JIT pre-warm: only fires `PREWARM_JIT_ENABLED && PREWARM_ITERS > 0`.
    In native-image, the method is AOT-compiled; calling
    `conjunctiveCount` 200x as a "pre-warm" costs 200 iterations of
    work at install time for zero benefit. Not harmful but wasteful —
    measure the overhead; if > 10 ms, add a native-image skip.

**Potential regression**: none identified. If native wall regresses
vs iter#03c native PGO (4.72 s at pre-iter#04 source), investigate
LongVector / VectorMask intrinsic gaps in GraalVM 25.

### Lever 2 (dense group-by)

- **Canonical dict correctness**: two leaves may have a byte-slice in
  position i that IS in the canonical dict at position j ≠ i. The
  mapping must be per-leaf-dictId → canonId, not positional.
- **Dict mutation**: the canonical dict is built once at install and
  immutable. Any leaf dict-slice not found in the canonical dict
  forces fallback (either full-leaf fallback, or per-row fallback by
  recording an "unknown" sentinel — simpler: per-leaf fallback).
- **Per-leaf dict scan**: small N (≤ 8 for bench), linear search in
  the canonical byte[][] with `Arrays.equals` (≤ 8 comparisons × ≤ 32
  bytes each = 256 byte comparisons = ~50 ns per distinct dict value
  × ~8 distinct per leaf = ~400 ns per leaf × 97,657 leaves = 39 ms
  total across all worker threads combined — amortized.
- **Thread-safety**: canonical dict built once under the install-time
  synchronizer; immutable after publish (final field on Handle). Per-
  worker `long[N]` is worker-local, merged only after `allOf.join()`
  via `addTo` on the final `Object2LongOpenHashMap<String>`.
- **Bounded leaf probe**: `K=16` leaves is enough to estimate cardinality
  for the bench (8 unique dept/city values, uniformly distributed — 1
  leaf typically has all 8 by row 100 with 1024 rows/leaf). Configurable
  via `-Dsirix.projection.denseGroupBy.probeLeaves=N`. Hard cap 1024
  to avoid install-time regressions on huge indexes.
- **N threshold**: `256` default. Configurable via
  `-Dsirix.projection.denseGroupBy.cardLimit=N`. Each worker pays
  `N * 8 bytes = 2 KB` per scan for the `long[N]`, well under the
  per-worker scratch budget.
- **Parity test**: new `ProjectionIndexByteScanTest.denseGroupByMatchesHashMap_*`
  tests assert byte-for-byte identical `Object2LongOpenHashMap<String>`
  output between dense and hashmap paths across:
  (a) N=2 (degenerate small)
  (b) N=8 (bench-representative)
  (c) N=256 (threshold boundary)
  (d) N=257 (above threshold — falls back)
  (e) probeLeaves missing a late-arriving value (falls back per-leaf)
  (f) empty predicates (all rows match path)
  (g) boolean EQ predicate (filtered path)
  (h) cross-leaf dict variation (same canonical value at different
      leaf-dict positions)
- **API surface**: new `ProjectionIndexByteScan.conjunctiveCountByGroupDense(…)`
  and `ProjectionIndexRegistry.Handle.canonicalDict(int groupColumn, int probeLeaves, int cardLimit)`.
  Existing `conjunctiveCountByGroup(…)` unchanged. `SirixVectorizedExecutor.
  parallelConjunctiveCountByGroup` gains a `denseEligible` path.
- **Rollback flag**: `-Dsirix.projection.denseGroupBy=false` disables
  the dense path, forces hashmap. Default `true`.
- **HFT invariants**: pre-allocated `long[N]` per worker (one alloc per
  scan at worst); merge in single pass; no boxing; no virtual dispatch
  on the hot loop (the per-leaf-dictId → canonId remap is a static
  `Arrays.equals` call, fully inlineable).

## Phase 2 — implementation plan

### Order

1. **Lever 2 dense group-by** — Java code + unit tests + JVM A/B.
   - `ProjectionIndexRegistry.Handle` gains `canonicalDict(int groupColumn)`
     lazy-init method + private `byte[][][] canonicalDicts` field.
   - `ProjectionIndexByteScan.conjunctiveCountByGroupDense` new method.
   - `ProjectionIndexByteScan` exposes a helper for canonical-dict
     probe (package-private, called from Handle).
   - `SirixVectorizedExecutor.parallelConjunctiveCountByGroup`
     dispatcher on `denseEligible`.
   - Unit tests in `ProjectionIndexByteScanTest`: 8 parity scenarios.
   - Unit tests in `ProjectionIndexRegistryTest`: canonicalDict lazy
     and bounded.
2. **Lever 1 native PGO rebuild** — `./gradlew :sirix-query:nativeCompile
   -Ppgo=/tmp/claude/sirix-100m.iprof -Pquick-build=false --rerun-tasks`.
   Build time ~60 s.

### Step-by-step (Lever 2 only — Lever 1 is a build invocation)

**Step 1** — Extend `Handle`:
```java
public static final class Handle {
  // … existing fields …
  private volatile byte[][] canonicalDict_cached;  // lazy

  // Probe config
  public byte[][] canonicalDict(int groupColumn, int probeLeaves, int cardLimit) {
    // Read barrier then lazy init under a synchronizer on 'this'.
    // Return the array; or null if N > cardLimit (not eligible).
  }
}
```
Per-column caching via `byte[][][] canonicalDicts` keyed by groupColumn.

**Step 2** — Add method signature:
```java
public static void conjunctiveCountByGroupDense(
    Iterable<byte[]> leafPayloads,
    ColumnPredicate[] predicates,
    int groupColumn,
    byte[][] canonicalDict,
    long[] out /* length >= canonicalDict.length */,
    Object2LongOpenHashMap<String> fallbackOut);
```
`out[i]` counts matches whose group value is `canonicalDict[i]`.
`fallbackOut` receives counts for any leaf that has a dict value NOT
in `canonicalDict` (falls back to hashmap path for that leaf).

**Step 3** — Dispatcher:
```java
parallelConjunctiveCountByGroup(leafPayloads, preds, groupColumn):
  canon = handle.canonicalDict(groupColumn, probeLeaves=16, cardLimit=256);
  if (canon != null && DENSE_GROUPBY_ENABLED) {
    // Dense path — per-worker long[N] + optional hashmap fallback.
  } else {
    // Hashmap path (existing).
  }
```

**Step 4** — Merge at end:
- Sum `long[N]` across workers → single `long[N]`.
- For i in 0..N-1: if counts[i] > 0, put (new String(canon[i], UTF-8), counts[i]) into merged hashmap.
- Also merge the per-worker fallback hashmaps (additive).

### Test plan

`ProjectionIndexByteScanTest.denseGroupByParity_*`:
1. `emptyPreds_8Depts` — 1024-row synthetic leaf with 8 depts uniform random, empty preds, dense ⇌ hashmap.
2. `boolEq_8Depts` — same but with `active=true` predicate.
3. `numericBetween_8Depts` — `age BETWEEN 30 50` predicate.
4. `n256_boundary` — exactly 256 distinct strings, all fit dense.
5. `n257_fallback` — 257 distinct, dense returns null → hashmap path.
6. `crossLeafDictVariation` — two leaves, same 8 strings but different positional dictIds. Dense ⇌ hashmap.
7. `missingDictValue_fallback` — probe found N=2, later leaf has a 3rd value. Per-leaf fallback injected. Dense+fallback ⇌ hashmap.
8. `singleValue` — N=1 degenerate.

`ProjectionIndexRegistryTest.canonicalDict_*`:
1. `lazyBuild_reusesResult` — calling twice returns same array.
2. `aboveCardLimit_returnsNull` — N > limit ⇒ null (not eligible).
3. `probeLeaves_boundedBy16` — doesn't re-scan the whole 97K leaves.

## Phase 3 — Measurement

### Lever 1 (native PGO)

- Rebuild: `./gradlew :sirix-query:nativeCompile
  -Ppgo=/tmp/claude/sirix-100m.iprof -Pquick-build=false --rerun-tasks`.
- 5 verified-cold runs via `/tmp/claude/evict_db.py`; median.
- 4-event profile dump: `ASYNC_PROFILER=~/async-profiler PROFILE_EVENT=cpu
  PROFILE_OUTPUT=profiling-output/iter10-native.collapsed ./sirix-bench …`
  (only if supported; native binaries can profile via perf).
- Compare to JVM iter#09 (1.98 s).

### Lever 2 (dense group-by)

- JVM C2, AppCDS archive, prewarm=200.
- 5 verified-cold runs each, interleaved: hashmap path (rollback flag),
  dense path (default).
- 4-event profile dumps to `iter10-{cpu,alloc,wall,lock}.{txt,collapsed}`.
- First-call per-query and 3-iter avg for the 3 group-by queries:
  `groupByDept`, `groupBy2Keys`, `filterGroupBy`.

### Combined (Lever 1 + Lever 2 if applicable)

- Native binary built after Lever 2 lands (includes the dense path).
- 5 verified-cold runs × 5 variants: JVM hashmap, JVM dense, native
  hashmap (dense flag off), native dense (default), old iter#08 native
  (baseline).

## Decision criteria

Keep both levers if ANY of:
- Combined JVM cold wall drops ≥ 5 %: 1.98 s → ≤ 1.88 s.
- Combined JVM cold wall drops ≤ 1.5 s (deep DuckDB ballpark).
- Native PGO cold drops below JVM (< 1.98 s) — pivot production to native.

Keep Lever 2 only if its JVM A/B shows ≥ 3 % wall improvement OR
≥ 30 % drop in `Object2LongOpenHashMap.addTo + String.equals` CPU share.

Reject Lever 1 and revert binary if:
- Native cold regresses vs 2.70 s iter#08-epoch baseline.

Reject Lever 2 and revert code if:
- JVM A/B shows regression beyond measurement noise (IQR > 50 ms).
- Any parity test fails.
- `./gradlew :sirix-core:test :sirix-query:test --parallel` fails.

## Rollback

- Lever 1: `sirix-bench` is a build artifact; re-build with
  `--rerun-tasks` against any prior source commit to revert.
- Lever 2: `-Dsirix.projection.denseGroupBy=false` forces hashmap.
  Code path stays compiled but dead on the hot path.

## Stop conditions (from prompt)

- JVM cold ≤ 1.5 s: declare JVM deep ballpark.
- Native cold ≤ 1.5 s: pivot production to native.
- 3-iter plateau: declare floor pending graal#13377.
