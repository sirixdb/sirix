# iter#08 Plan — DB Open + Hydrate Attack

**Baseline**: 2.80 / 2.94 / 3.54 / 2.79 / 2.91 / 3.00 s → median **2.94 s** over 6 cold runs post-commit `09522bba2`.

Target: ≤ 2.0 s (DuckDB ballpark). Remaining gap: ~0.94 s.

## Phase attribution (3-run median via `-Dsirix.bench.phaseTiming=true`)

| phase | ms | share | cumulative t |
|---|---:|---:|---:|
| pre-main JVM startup | ~300 | 10% | ~300 |
| allocInit (FrameSlotAllocator) | 190 | 6.5% | 490 |
| storeBuild | 3 | 0.1% | 493 |
| ctxChain | 38 | 1.3% | 531 |
| lookup | 50 | 1.7% | 581 |
| beginSession | 76 | 2.6% | 657 |
| vecExecutor | 3 | 0.1% | 660 |
| **projectionHydrate** | **1250** | **42.5%** | **1910** |
| docBind | 9 | 0.3% | 1920 |
| queries (3 iters × 9 queries) | 900 | 30.6% | 2820 |
| JVM shutdown | ~120 | 4.1% | 2940 |

Pre-query work ≈ 1.92 s, queries ≈ 0.90 s, JVM overhead ≈ 0.42 s. **Hydrate is the dominant attack surface.**

## Top CPU frames (iter08-baseline-cpu.txt, 2667 samples)

Post-commit top-10 leaf frames unchanged from iter#07:
- 177 (6.64%) `ProjectionIndexByteScan.conjunctiveCountByGroup` — query path
- 158 (5.92%) `Object2LongOpenHashMap.addTo` — groupBy accumulation
- 127 (4.76%) `VarHandle.checkAccessModeThenIsDirect` — spread across queries
- 126 (4.72%) `__memcpy_sse2_unaligned_erms` — spread across hydrate + query
- 96 (3.60%) `clear_page_erms_[k]` — mmap-pagefault zero-fill
- ...

## Hydrate CPU drill-down (643 samples / 24% of CPU)

From `collectSubtreeChunks`:
- **360 samples (57%)** — `HOTRangeCursor.next()` and descendants
- **139 samples (22%)** — `Arrays.copyOf` (buffer growth in `collectSubtreeChunks`)
- **51 samples (8%)** — `MemorySegment.copy` (actual bulk copy)
- **19 samples (3%)** — `decodeCompositeKeySegment`
- remaining — cursor.range construction, etc.

## Hydrate alloc drill-down (under HOTRangeCursor stacks, sampled)

MemorySegment-related allocs — cursor is **the** allocator in this path:
- 85 `HeapSession`
- 80 `HeapMemorySegmentImpl$OfByte`
- 64 `ValueLayouts$OfLongImpl`
- 60 `NativeMemorySegmentImpl`
- 32 `HOTRangeCursor$Entry`

Plus 11,182 `byte[]` samples (buffer copyOf + range key allocation).

## Attack — One change, measured

**Target**: `ProjectionIndexHOTStorage.collectSubtreeChunks` + `HOTRangeCursor` hot loop.

Three sub-attacks, smallest-surface-area first:

### Sub-attack 1 — Pre-size the per-leaf accumulator buffer (pre-copyOf)

In `collectSubtreeChunks` line 936:
```java
buf = new byte[Math.max(valueSize, CHUNK_SIZE)];
```

With `CHUNK_SIZE=4096` and typical leaf serialized size ~20 KB, most leaves do 4-5 `Arrays.copyOf` as the buffer doubles. Pre-size to a typical-leaf estimate (say `CHUNK_SIZE * 6 = 24 KB`) and most leaves will never copyOf at all. Use a new system property `sirix.projection.expectedLeafBytes` so we can tune without recompile.

**Expected win**: 139 copyOf samples → ~20 samples (only oversize leaves copy). Drop ~5% of hydrate CPU = ~50-70 ms wall. Modest but zero-risk.

### Sub-attack 2 — Zero-alloc cursor (eliminate per-entry `new Entry`, `MemorySegment.ofArray`)

1. **Reuse the `Entry` record** — or better, eliminate it entirely. Add package-private accessors `currentKeySlice()` / `currentValueSlice()` and expose `advance()` returning boolean. Callers that don't need the `Entry` allocation walk the cursor via these primitives.
2. **Pre-alloc `toKey` as MemorySegment once** — `compareKeys` currently does `MemorySegment.ofArray(b)` every invocation. Pre-allocate the `toKey` MemorySegment as an instance field at cursor construction time.

The `Entry` record + two slice allocations × ~500K calls ≈ 1.5M allocs eliminated. Alloc profiler estimates 321 samples → near zero.

**Expected win**: 360 `next()` samples → ~120 samples. ~9% of hydrate CPU = ~100-150 ms wall. Bigger but requires API surface change.

### Sub-attack 3 — Async allocator init

`FrameSlotAllocator.init()` does 7 serial mmaps + 7 AtomicLongArray zero-fills. Each class is independent. Parallelize across 7 ForkJoinPool tasks. Or even better: kick off init from static init in background thread, have `init()` just join.

**Expected win**: 190 ms → ~50 ms (limited by longest class init + syscall latency). ~5% wall saving.

### Integration & ordering

Do **sub-attack 1** first (lowest risk, measurable in isolation). Then sub-attack 2 (bigger win). Sub-attack 3 independently if 1+2 don't get us to the threshold.

### Decision gates

- **Keep if**: cold wall (3-run median) improves by ≥ 10% (−0.29 s → ≤ 2.65 s) AND target frame(s) drop ≥ 30%.
- **Revert if**: any regression on `:sirix-core:test` / `:sirix-query:test` parallel suite OR wall doesn't improve ≥ 3%.
- **Rollback flag**: `-Dsirix.projection.hydrate.expectedLeafBytes=4096` (defaults to 24576; setting to CHUNK_SIZE reproduces old behavior).

## Correctness invariants (formal)

For **Sub-attack 1** (pre-size buffer):
- Initial buffer size `INITIAL_CAPACITY` > valueSize for first chunk in all cases: `max(valueSize, INITIAL_CAPACITY)`.
- Growth logic unchanged: `len + valueSize > buf.length` still triggers `copyOf` with doubling. So pre-sizing NEVER causes an early truncation.
- Terminal trim unchanged: `len == buf.length ? buf : Arrays.copyOf(buf, len)` — still right-sizes.
- Behavior for payloads > INITIAL_CAPACITY: growth loop doubles as before. Correct at all leaf sizes.

For **Sub-attack 2** (zero-alloc cursor):
- `Entry` elimination: package-private accessors return MemorySegment slices with exactly the same lifetime semantics as the returned Entry's fields (bounded by leaf guard). No behavior change.
- `toKey` pre-allocation: `MemorySegment.ofArray(byte[])` is pure — creating the same heap-backed view at construction vs. per-call is observationally identical. The `b[]` reference must not change after construction, which is guaranteed (cursor stores `toKey` as final field).
- Existing `Iterator<Entry>` API remains functional — `next()` can still allocate an Entry when callers use it via the interface. Only the dedicated fast-path users (`collectSubtreeChunks`) call the zero-alloc accessors.

For **Sub-attack 3** (async allocator init):
- Every caller of `Allocators.getInstance()` currently calls `.init(budget)` before first use of `allocate()`. We must preserve this happens-before.
- `FrameSlotAllocator.init()` blocks on the parallel init tasks via `Future.get()` or `CompletableFuture.allOf().join()` before returning. Same observable semantics, just faster.
- Existing tests exercise `init()` from single-thread paths; the parallelism is internal.

## Tests to add / extend

1. **`ProjectionIndexHOTStorageTest.testHydratePreservesDataUnderDifferentInitialBuffers`** — parametrize `expectedLeafBytes = 1, 4096, 24576, 65536, 1_000_000`; assert byte-for-byte equivalence of `readAll` output.
2. **`HOTRangeCursorTest.testZeroAllocCursorPath`** — run cursor over 1000 entries, verify `Entry`-free accessors return the same data as `next().key()/value()`.
3. **`HOTRangeCursorTest.testToKeyPrecomputedBoundary`** — range with `toKey` at several boundaries (1 entry, exactly at bound, past bound); verify termination condition identical to current impl.

## Rollback plan

- **Sub-attack 1**: Revert via `-Dsirix.projection.hydrate.expectedLeafBytes=4096` or delete the 1-line default change.
- **Sub-attack 2**: Keep the Iterator path as baseline; make the zero-alloc fast path opt-in via a new static method `collectSubtreeChunksFast`. Rollback = switch the call site back.
- **Sub-attack 3**: Revert to serial `for` loop in `initInternal`. Same API surface.
